/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

/*
 * A bytes/string value is encoded as a length prefix followed by that many
 * bytes of data. A malicious or truncated input can declare a huge length with
 * little or no actual data, which previously caused a correspondingly huge
 * allocation before the shortfall was noticed. When the reader knows how many
 * bytes remain (a memory-backed reader), the declared length must be rejected
 * before allocating for it.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <avro.h>

/* Decodes buf against the given schema and returns the avro_value_read rc.
 * A non-zero rc means the read was rejected. */
static int try_decode(avro_value_iface_t *iface, const char *buf, size_t buf_size)
{
	int rc;
	avro_value_t decoded;
	avro_reader_t reader;

	rc = avro_generic_value_new(iface, &decoded);
	if (rc != 0) {
		fprintf(stderr, "avro_generic_value_new failed: %s\n", avro_strerror());
		return -1;
	}

	reader = avro_reader_memory(buf, buf_size);
	if (reader == NULL) {
		fprintf(stderr, "avro_reader_memory failed\n");
		avro_value_decref(&decoded);
		return -1;
	}

	rc = avro_value_read(reader, &decoded);

	avro_reader_free(reader);
	avro_value_decref(&decoded);
	return rc;
}

static int check_rejects_oversized(const char *schema_literal, const char *label)
{
	avro_schema_t schema = NULL;
	avro_value_iface_t *iface = NULL;
	int rc;
	int ret = EXIT_FAILURE;

	/*
	 * A length prefix declaring 127 bytes (zig-zag long 254 -> varint
	 * 0xFE 0x01), followed by no data at all. The reader has 0 bytes
	 * remaining after the prefix, so a declared length of 127 must be
	 * rejected before allocating.
	 */
	const char oversized[] = { (char) 0xFE, (char) 0x01 };

	if (avro_schema_from_json_length(schema_literal, strlen(schema_literal),
					 &schema) != 0) {
		fprintf(stderr, "%s: failed to parse schema: %s\n", label, avro_strerror());
		return EXIT_FAILURE;
	}

	iface = avro_generic_class_from_schema(schema);
	if (iface == NULL) {
		fprintf(stderr, "%s: failed to create iface\n", label);
		avro_schema_decref(schema);
		return EXIT_FAILURE;
	}

	rc = try_decode(iface, oversized, sizeof(oversized));
	if (rc == 0) {
		fprintf(stderr, "%s: FAIL - oversized length was accepted\n", label);
	} else if (rc != EINVAL) {
		/* The availability checks reject with EINVAL before allocating.
		 * A different error (e.g. ENOSPC from failing to read after a
		 * large allocation) would indicate the pre-fix behavior. */
		fprintf(stderr, "%s: FAIL - rejected with rc=%d (expected EINVAL): %s\n",
			label, rc, avro_strerror());
	} else {
		fprintf(stderr, "%s: oversized length rejected as expected: %s\n",
			label, avro_strerror());
		ret = EXIT_SUCCESS;
	}

	avro_value_iface_decref(iface);
	avro_schema_decref(schema);
	return ret;
}

static int check_accepts_valid(void)
{
	avro_schema_t schema = NULL;
	avro_value_iface_t *iface = NULL;
	int rc;
	int ret = EXIT_FAILURE;
	const char *text = NULL;
	size_t text_size = 0;
	avro_value_t decoded;
	avro_reader_t reader;

	/* A well-formed 3-byte string "abc": length 3 (zig-zag 6 -> 0x06),
	 * then the bytes 'a','b','c'. This must still decode. */
	const char valid[] = { 0x06, 'a', 'b', 'c' };

	if (avro_schema_from_json_literal("\"string\"", &schema) != 0) {
		fprintf(stderr, "valid: failed to parse schema: %s\n", avro_strerror());
		return EXIT_FAILURE;
	}
	iface = avro_generic_class_from_schema(schema);
	if (iface == NULL) {
		fprintf(stderr, "valid: failed to create iface\n");
		avro_schema_decref(schema);
		return EXIT_FAILURE;
	}

	if (avro_generic_value_new(iface, &decoded) != 0) {
		avro_value_iface_decref(iface);
		avro_schema_decref(schema);
		return EXIT_FAILURE;
	}
	reader = avro_reader_memory(valid, sizeof(valid));
	rc = avro_value_read(reader, &decoded);
	if (rc == 0 && avro_value_get_string(&decoded, &text, &text_size) == 0 &&
	    text_size == 4 && strcmp(text, "abc") == 0) {
		fprintf(stderr, "valid: well-formed string decoded as expected\n");
		ret = EXIT_SUCCESS;
	} else {
		fprintf(stderr, "valid: FAIL - well-formed string did not decode (rc=%d)\n", rc);
	}

	avro_reader_free(reader);
	avro_value_decref(&decoded);
	avro_value_iface_decref(iface);
	avro_schema_decref(schema);
	return ret;
}

/* An array of nulls: null elements occupy zero bytes, so a large declared
 * count is legitimate and must not be rejected. */
static int check_accepts_null_array(void)
{
	avro_schema_t schema = NULL;
	avro_value_iface_t *iface = NULL;
	avro_value_t decoded;
	avro_reader_t reader;
	int rc;
	int ret = EXIT_FAILURE;
	size_t count = 0;

	/* count 127 (zig-zag 254 -> 0xFE 0x01), then the end-of-array marker 0. */
	const char null_array[] = { (char) 0xFE, (char) 0x01, 0x00 };

	if (avro_schema_from_json_length("{\"type\":\"array\",\"items\":\"null\"}",
					 strlen("{\"type\":\"array\",\"items\":\"null\"}"),
					 &schema) != 0) {
		fprintf(stderr, "null-array: failed to parse schema: %s\n", avro_strerror());
		return EXIT_FAILURE;
	}
	iface = avro_generic_class_from_schema(schema);
	if (iface == NULL) {
		avro_schema_decref(schema);
		return EXIT_FAILURE;
	}
	if (avro_generic_value_new(iface, &decoded) != 0) {
		avro_value_iface_decref(iface);
		avro_schema_decref(schema);
		return EXIT_FAILURE;
	}
	reader = avro_reader_memory(null_array, sizeof(null_array));
	rc = avro_value_read(reader, &decoded);
	if (rc == 0 && avro_value_get_size(&decoded, &count) == 0 && count == 127) {
		fprintf(stderr, "null-array: 127 nulls decoded, not falsely rejected\n");
		ret = EXIT_SUCCESS;
	} else {
		fprintf(stderr, "null-array: FAIL (rc=%d count=%zu): %s\n",
			rc, count, avro_strerror());
	}

	avro_reader_free(reader);
	avro_value_decref(&decoded);
	avro_value_iface_decref(iface);
	avro_schema_decref(schema);
	return ret;
}

int main(void)
{
	if (check_rejects_oversized("\"string\"", "string") != EXIT_SUCCESS) {
		return EXIT_FAILURE;
	}
	if (check_rejects_oversized("\"bytes\"", "bytes") != EXIT_SUCCESS) {
		return EXIT_FAILURE;
	}
	/* An array/map declaring 127 elements with no data must be rejected: the
	 * count exceeds the bytes that could back it. */
	if (check_rejects_oversized("{\"type\":\"array\",\"items\":\"long\"}", "array") != EXIT_SUCCESS) {
		return EXIT_FAILURE;
	}
	if (check_rejects_oversized("{\"type\":\"map\",\"values\":\"long\"}", "map") != EXIT_SUCCESS) {
		return EXIT_FAILURE;
	}
	if (check_accepts_valid() != EXIT_SUCCESS) {
		return EXIT_FAILURE;
	}
	if (check_accepts_null_array() != EXIT_SUCCESS) {
		return EXIT_FAILURE;
	}
	return EXIT_SUCCESS;
}
