/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0 
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License. 
 */

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "avro.h"
#include "allocation.h"
#include "datum.h"
#include "jansson.h"

/*
 * Converts a binary buffer into a NUL-terminated JSON UTF-8 string.
 * Avro bytes and fixed values are encoded in JSON as a string, and JSON
 * strings must be in UTF-8.  For these Avro types, the JSON string is
 * restricted to the characters U+0000..U+00FF, which corresponds to the
 * ISO-8859-1 character set.  This function performs this conversion.
 * The resulting string must be freed using avro_free when you're done
 * with it.
 */

static int
encode_utf8_bytes(const void *src, size_t src_len,
		  void **dest, size_t *dest_len)
{
	if (!src || !dest || !dest_len) {
		return EINVAL;
	}

	// First, determine the size of the resulting UTF-8 buffer.
	// Bytes in the range 0x00..0x7f will take up one byte; bytes in
	// the range 0x80..0xff will take up two.
	const uint8_t  *src8 = src;

	size_t  utf8_len = src_len + 1;  // +1 for NUL terminator
	size_t  i;
	for (i = 0; i < src_len; i++) {
		if (src8[i] & 0x80) {
			utf8_len++;
		}
	}

	// Allocate a new buffer for the UTF-8 string and fill it in.
	uint8_t  *dest8 = avro_malloc(utf8_len);
	if (dest8 == NULL) {
		return ENOMEM;
	}

	uint8_t  *curr = dest8;
	for (i = 0; i < src_len; i++) {
		if (src8[i] & 0x80) {
			*curr++ = (0xc0 | (src8[i] >> 6));
			*curr++ = (0x80 | (src8[i] & 0x3f));
		} else {
			*curr++ = src8[i];
		}
	}

	*curr = '\0';

	// And we're good.
	*dest = dest8;
	*dest_len = utf8_len;
	return 0;
}

static json_t *
avro_datum_to_json_t(const avro_datum_t datum, const avro_schema_t schema)
{
	switch (avro_typeof(datum)) {
		case AVRO_BOOLEAN:
			return avro_datum_to_boolean(datum)->i?
			    json_true():
			    json_false();

		case AVRO_BYTES:
			{
				struct avro_bytes_datum_t  *bytes =
				    avro_datum_to_bytes(datum);

				void  *encoded = NULL;
				size_t  encoded_size = 0;

				if (encode_utf8_bytes(bytes->bytes, bytes->size,
						      &encoded, &encoded_size)) {
					return NULL;
				}

				json_t  *result = json_string_nocheck(encoded);
				avro_free(encoded, encoded_size);
				return result;
			}

		case AVRO_DOUBLE:
			return json_real(avro_datum_to_double(datum)->d);

		case AVRO_FLOAT:
			return json_real(avro_datum_to_float(datum)->f);

		case AVRO_INT32:
			return json_integer(avro_datum_to_int32(datum)->i32);

		case AVRO_INT64:
			return json_integer(avro_datum_to_int64(datum)->i64);

		case AVRO_NULL:
			return json_null();

		case AVRO_STRING:
			return json_string(avro_datum_to_string(datum)->s);

		case AVRO_ARRAY:
			{
				json_t  *result = json_array();
				if (!result) {
					return NULL;
				}

				avro_schema_t  element_schema = avro_schema_array_items(schema);
				int  num_elements = avro_array_size(datum);
				int  i;
				for (i = 0; i < num_elements; i++) {
					avro_datum_t  element = NULL;
					if (avro_array_get(datum, i, &element)) {
						json_decref(result);
						return NULL;
					}

					json_t  *element_json =
					    avro_datum_to_json_t(element, element_schema);
					if (!element_json) {
						json_decref(result);
						return NULL;
					}

					if (json_array_append_new(result, element_json)) {
						json_decref(result);
						return NULL;
					}
				}

				return result;
			}

		case AVRO_ENUM:
			return json_string(avro_enum_get_name(datum, schema));

		case AVRO_FIXED:
			{
				struct avro_fixed_datum_t  *fixed =
				    avro_datum_to_fixed(datum);

				void  *encoded = NULL;
				size_t  encoded_size = 0;

				if (encode_utf8_bytes(fixed->bytes, fixed->size,
						      &encoded, &encoded_size)) {
					return NULL;
				}

				json_t  *result = json_string_nocheck(encoded);
				avro_free(encoded, encoded_size);
				return result;
			}

		case AVRO_MAP:
			{
				json_t  *result = json_object();
				if (!result) {
					return NULL;
				}

				avro_schema_t  element_schema = avro_schema_map_values(schema);
				int  num_elements = avro_map_size(datum);
				int  i;
				for (i = 0; i < num_elements; i++) {
					const char  *key = NULL;
					if (avro_map_get_key(datum, i, &key)) {
						json_decref(result);
						return NULL;
					}

					avro_datum_t  element = NULL;
					if (avro_map_get(datum, key, &element)) {
						json_decref(result);
						return NULL;
					}

					json_t  *element_json =
					    avro_datum_to_json_t(element, element_schema);
					if (!element_json) {
						json_decref(result);
						return NULL;
					}

					if (json_object_set_new(result, key, element_json)) {
						json_decref(result);
						return NULL;
					}
				}

				return result;
			}

		case AVRO_RECORD:
			{
				json_t  *result = json_object();
				if (!result) {
					return NULL;
				}

				int  num_fields = avro_schema_record_size(schema);
				int  i;
				for (i = 0; i < num_fields; i++) {
					const char  *field_name =
					    avro_schema_record_field_name(schema, i);

					avro_schema_t  field_schema =
					    avro_schema_record_field_get(schema, field_name);

					avro_datum_t  field = NULL;
					if (avro_record_get(datum, field_name, &field)) {
						json_decref(result);
						return NULL;
					}

					json_t  *field_json =
					    avro_datum_to_json_t(field, field_schema);
					if (!field_json) {
						json_decref(result);
						return NULL;
					}

					if (json_object_set_new(result, field_name, field_json)) {
						json_decref(result);
						return NULL;
					}
				}

				return result;
			}

		case AVRO_UNION:
			{
				int64_t  discriminant = avro_union_discriminant(datum);
				avro_datum_t  branch = avro_union_current_branch(datum);
				avro_schema_t  branch_schema =
				    avro_schema_union_branch(schema, discriminant);

				if (is_avro_null(branch_schema)) {
					return json_null();
				}

				json_t  *result = json_object();
				if (!result) {
					return NULL;
				}

				json_t  *branch_json = avro_datum_to_json_t(branch, branch_schema);
				if (!branch_json) {
					json_decref(result);
					return NULL;
				}

				const char  *branch_name = avro_schema_type_name(branch_schema);
				if (json_object_set_new(result, branch_name, branch_json)) {
					json_decref(result);
					return NULL;
				}

				return result;
			}

		default:
			return NULL;
	}
}

int avro_datum_to_json(const avro_datum_t datum, const avro_schema_t schema,
		       int one_line, char **json_str)
{
	if (!is_avro_datum(datum) || !is_avro_schema(schema) || !json_str) {
		return EINVAL;
	}

	json_t  *json = avro_datum_to_json_t(datum, schema);
	if (!json) {
		return ENOMEM;
	}

	// Jansson will only encode an object or array as the root
	// element.

	if (json_is_array(json) || json_is_object(json)) {
		*json_str = json_dumps
		    (json,
		     JSON_INDENT(one_line? 0: 2) |
		     JSON_ENSURE_ASCII |
		     JSON_PRESERVE_ORDER);
		json_decref(json);
		return 0;
	}

	// Otherwise we have to play some games.  We'll wrap the JSON
	// value in an array, and then strip off the leading and
	// trailing square brackets.

	json_t  *array = json_array();
	json_array_append_new(array, json);
	char  *array_str = json_dumps
	    (array,
	     JSON_INDENT(one_line? 0: 2) |
	     JSON_ENSURE_ASCII |
	     JSON_PRESERVE_ORDER);
	json_decref(array);

	// If the caller requested a one-line string, then we strip off
	// "[" from the front and "]" from the back.  Otherwise, we
	// strip off "[\n  " from the front and "\n]" from the back.

	size_t  length = strlen(array_str);
	size_t  front_chop = one_line? 1: 4;
	size_t  back_chop = one_line? 1: 2;
	length -= (front_chop + back_chop);

	// We don't use the custom allocator, because we need to mimic
	// the string that Jansson would have returned.

	char  *result = malloc(length + 1);
	memcpy(result, array_str + front_chop, length);
	result[length] = '\0';
	free(array_str);

	*json_str = result;
	return 0;
}
