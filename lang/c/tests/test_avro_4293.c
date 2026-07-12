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

/* Portable set/unset of the collection-limit environment variable: MSVC has no
 * setenv/unsetenv, so use _putenv_s (an empty value unsets the variable). */
#ifdef _WIN32
static void set_collection_limit(const char *value)
{
	_putenv_s("AVRO_MAX_COLLECTION_ITEMS", value);
}
static void unset_collection_limit(void)
{
	_putenv_s("AVRO_MAX_COLLECTION_ITEMS", "");
}
#else
static void set_collection_limit(const char *value)
{
	setenv("AVRO_MAX_COLLECTION_ITEMS", value, 1);
}
static void unset_collection_limit(void)
{
	unsetenv("AVRO_MAX_COLLECTION_ITEMS");
}
#endif

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

/* Decodes an enum whose declared symbol index is out of range for the schema.
 * The index must be rejected before it is stored, rather than accepted as an
 * out-of-bounds ordinal. */
static int check_enum_index_rejected(const char *encoded, size_t size, const char *label)
{
	const char *schema_literal =
	    "{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"A\",\"B\"]}";
	avro_schema_t schema = NULL;
	avro_value_iface_t *iface = NULL;
	int rc;
	int ret = EXIT_FAILURE;

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

	rc = try_decode(iface, encoded, size);
	if (rc == 0) {
		fprintf(stderr, "%s: FAIL - out-of-range enum index was accepted\n", label);
	} else {
		fprintf(stderr, "%s: out-of-range enum index rejected as expected: %s\n",
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
	if (reader == NULL) {
		fprintf(stderr, "valid: avro_reader_memory failed\n");
		avro_value_decref(&decoded);
		avro_value_iface_decref(iface);
		avro_schema_decref(schema);
		return EXIT_FAILURE;
	}
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

/* An array of nulls within the configured limit: null elements occupy zero
 * bytes, so a moderate declared count is legitimate and must not be rejected by
 * the available-bytes check (it is instead bounded by the zero-byte item cap). */
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
	if (reader == NULL) {
		fprintf(stderr, "null-array: avro_reader_memory failed\n");
		avro_value_decref(&decoded);
		avro_value_iface_decref(iface);
		avro_schema_decref(schema);
		return EXIT_FAILURE;
	}
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

/*
 * Zero-byte-element collection allocation limit
 */

/* Encode a long as an Avro zig-zag varint into buf; returns the byte count. */
static size_t encode_long(int64_t n, char *buf)
{
	uint64_t z = ((uint64_t) n << 1) ^ (uint64_t) (n >> 63);
	size_t i = 0;
	do {
		uint8_t b = z & 0x7F;
		z >>= 7;
		if (z) {
			b |= 0x80;
		}
		buf[i++] = (char) b;
	} while (z);
	return i;
}

/* Build one array block of `count` elements (a negative count is followed by a
 * block byte-size), plus the end-of-array marker. Returns the total length. */
static size_t build_null_array(int64_t count, int negative, char *buf)
{
	size_t n = 0;
	if (negative) {
		n += encode_long(-count, buf + n);
		n += encode_long(0, buf + n); /* block byte-size */
	} else {
		n += encode_long(count, buf + n);
	}
	n += encode_long(0, buf + n); /* end-of-array marker */
	return n;
}

/* Decode `buf` against `schema_literal` and return the avro_value_read rc. */
static int decode_schema(const char *schema_literal, const char *buf, size_t buf_size)
{
	avro_schema_t schema = NULL;
	avro_value_iface_t *iface = NULL;
	int rc;

	if (avro_schema_from_json_length(schema_literal, strlen(schema_literal),
					 &schema) != 0) {
		fprintf(stderr, "failed to parse schema: %s\n", avro_strerror());
		return -1;
	}
	iface = avro_generic_class_from_schema(schema);
	if (iface == NULL) {
		avro_schema_decref(schema);
		return -1;
	}
	rc = try_decode(iface, buf, buf_size);
	avro_value_iface_decref(iface);
	avro_schema_decref(schema);
	return rc;
}

static int check_null_collection_rejected(const char *schema_literal, int64_t count,
					  int negative, const char *label)
{
	char buf[32];
	size_t len = build_null_array(count, negative, buf);
	int rc = decode_schema(schema_literal, buf, len);
	if (rc == EINVAL) {
		fprintf(stderr, "%s: rejected as expected: %s\n", label, avro_strerror());
		return EXIT_SUCCESS;
	}
	fprintf(stderr, "%s: FAIL - rc=%d (expected EINVAL)\n", label, rc);
	return EXIT_FAILURE;
}

static int check_null_array_accepted(int64_t count, const char *label)
{
	char buf[32];
	size_t len = build_null_array(count, 0, buf);
	int rc = decode_schema("{\"type\":\"array\",\"items\":\"null\"}", buf, len);
	if (rc == 0) {
		fprintf(stderr, "%s: accepted as expected\n", label);
		return EXIT_SUCCESS;
	}
	fprintf(stderr, "%s: FAIL - rc=%d (expected 0)\n", label, rc);
	return EXIT_FAILURE;
}

/* Two blocks of `each` null elements followed by the end marker. */
static int check_null_array_cumulative_rejected(int64_t each, const char *label)
{
	char buf[48];
	size_t n = 0;
	int rc;
	n += encode_long(each, buf + n);
	n += encode_long(each, buf + n);
	n += encode_long(0, buf + n);
	rc = decode_schema("{\"type\":\"array\",\"items\":\"null\"}", buf, n);
	if (rc == EINVAL) {
		fprintf(stderr, "%s: rejected as expected: %s\n", label, avro_strerror());
		return EXIT_SUCCESS;
	}
	fprintf(stderr, "%s: FAIL - rc=%d (expected EINVAL)\n", label, rc);
	return EXIT_FAILURE;
}

/* Skipping a zero-byte array/map is bounded the same way as reading it. */
static int check_skip_null_collection_rejected(const char *schema_literal, int64_t count,
					       const char *label)
{
	avro_schema_t schema = NULL;
	avro_reader_t reader;
	char buf[32];
	size_t len;
	int rc;

	if (avro_schema_from_json_length(schema_literal, strlen(schema_literal),
					 &schema) != 0) {
		return EXIT_FAILURE;
	}
	len = build_null_array(count, 0, buf);
	reader = avro_reader_memory(buf, len);
	if (reader == NULL) {
		avro_schema_decref(schema);
		return EXIT_FAILURE;
	}
	rc = avro_skip_data(reader, schema);
	avro_reader_free(reader);
	avro_schema_decref(schema);
	if (rc == EINVAL) {
		fprintf(stderr, "%s: skip rejected as expected\n", label);
		return EXIT_SUCCESS;
	}
	fprintf(stderr, "%s: FAIL - skip rc=%d (expected EINVAL)\n", label, rc);
	return EXIT_FAILURE;
}

/* A backed non-zero-byte array that passes the bytes check is still bounded by
 * the structural cap (exercised with a lowered limit). */
static int check_structural_cap_rejected(const char *label)
{
	char buf[64];
	size_t n = 0;
	int i;
	int rc;
	n += encode_long(10, buf + n);          /* block count 10 */
	for (i = 0; i < 10; i++) {
		n += encode_long(i, buf + n);   /* 10 real longs (1 byte each) */
	}
	n += encode_long(0, buf + n);           /* end marker */
	rc = decode_schema("{\"type\":\"array\",\"items\":\"long\"}", buf, n);
	if (rc == EINVAL) {
		fprintf(stderr, "%s: rejected as expected: %s\n", label, avro_strerror());
		return EXIT_SUCCESS;
	}
	fprintf(stderr, "%s: FAIL - rc=%d (expected EINVAL)\n", label, rc);
	return EXIT_FAILURE;
}

int main(void)
{
	const char *array_null = "{\"type\":\"array\",\"items\":\"null\"}";
	const char *record_null =
	    "{\"type\":\"array\",\"items\":"
	    "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"n\",\"type\":\"null\"}]}}";
	const char *map_null = "{\"type\":\"map\",\"values\":\"null\"}";

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

	/* The reported exploit: 200,000,000 nulls must be rejected by the
	 * default limit before allocating. */
	if (check_null_collection_rejected(array_null, 200000000, 0,
					   "array<null> 200M (default limit)") != EXIT_SUCCESS) {
		return EXIT_FAILURE;
	}
	/* A record whose only field is null also encodes to zero bytes. */
	if (check_null_collection_rejected(record_null, 200000000, 0,
					   "array<record-of-null> 200M") != EXIT_SUCCESS) {
		return EXIT_FAILURE;
	}
	/* A negative block count is normalized and must still be bounded. */
	if (check_null_collection_rejected(array_null, 200000000, 1,
					   "array<null> negative count") != EXIT_SUCCESS) {
		return EXIT_FAILURE;
	}

	/* With a lowered limit, boundary behavior: 1000 accepted, 1001 rejected. */
	set_collection_limit("1000");
	if (check_null_array_accepted(1000, "array<null> 1000 within limit") != EXIT_SUCCESS) {
		unset_collection_limit();
		return EXIT_FAILURE;
	}
	if (check_null_collection_rejected(array_null, 1001, 0,
					   "array<null> 1001 over limit") != EXIT_SUCCESS) {
		unset_collection_limit();
		return EXIT_FAILURE;
	}
	if (check_null_array_cumulative_rejected(600, "array<null> cumulative 600+600") != EXIT_SUCCESS) {
		unset_collection_limit();
		return EXIT_FAILURE;
	}
	/* Skipping a huge zero-byte array is bounded too. */
	if (check_skip_null_collection_rejected(array_null, 5000,
						"skip array<null> over limit") != EXIT_SUCCESS) {
		unset_collection_limit();
		return EXIT_FAILURE;
	}
	unset_collection_limit();

	/* A backed non-zero-byte array is bounded by the structural cap. */
	set_collection_limit("5");
	if (check_structural_cap_rejected("array<long> over structural cap") != EXIT_SUCCESS) {
		unset_collection_limit();
		return EXIT_FAILURE;
	}
	unset_collection_limit();

	/* A huge map<null> is bounded by the available-bytes check (each entry
	 * carries a >= 1 byte key), rejected with EINVAL as well. */
	if (check_null_collection_rejected(map_null, 200000000, 0,
					   "map<null> 200M (available bytes)") != EXIT_SUCCESS) {
		return EXIT_FAILURE;
	}

	/* An enum symbol index out of range (>= symbol count, or negative) must be
	 * rejected rather than stored as an out-of-bounds ordinal. The schema has
	 * two symbols; index 9 (zig-zag long -> 0x12) and index -1 (-> 0x01) are
	 * both invalid. */
	{
		const char enum_too_large[] = { (char) 0x12 };
		const char enum_negative[] = { (char) 0x01 };
		if (check_enum_index_rejected(enum_too_large, sizeof(enum_too_large),
					      "enum index 9 of 2") != EXIT_SUCCESS) {
			return EXIT_FAILURE;
		}
		if (check_enum_index_rejected(enum_negative, sizeof(enum_negative),
					      "enum index -1") != EXIT_SUCCESS) {
			return EXIT_FAILURE;
		}
	}

	return EXIT_SUCCESS;
}

