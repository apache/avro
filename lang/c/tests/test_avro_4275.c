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

/**
 * Regression test for INT64_MIN negation overflow in read_array_value()
 * and read_map_value() (CWE-190).
 *
 * The Avro binary format encodes block counts as zigzag-encoded varints.
 * A negative block count means the absolute value is the actual count,
 * preceded by a byte-size field. When block_count == INT64_MIN, the
 * negation overflows (undefined behavior in C). This test verifies that
 * the decoder rejects such malformed input gracefully.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <avro.h>

/*
 * Zigzag encoding of INT64_MIN is 0xFFFFFFFFFFFFFFFF, which encodes as
 * the 10-byte varint: FF FF FF FF FF FF FF FF FF 01
 */
static const char int64min_block_count[] = {
    (char)0xFF, (char)0xFF, (char)0xFF, (char)0xFF, (char)0xFF,
    (char)0xFF, (char)0xFF, (char)0xFF, (char)0xFF, 0x01
};

/*
 * A valid array with negative block count: [-3] means 3 items follow,
 * preceded by the block byte-size.
 *
 * Layout:
 *   05        = varint 5 = zigzag(-3) => block_count = -3
 *   06        = varint 6 = zigzag(3)  => block_size = 3 bytes
 *   14 28 3C  = zigzag ints: 10, 20, 30
 *   00        = terminator (block_count = 0)
 */
static const char valid_neg_block_array[] = {
    '\x05',                     /* block_count = -3 */
    '\x06',                     /* block_size = 3 */
    '\x14', '\x28', '\x3C',    /* ints: 10, 20, 30 */
    '\x00'                      /* terminator */
};

/*
 * A valid map with negative block count: [-2] means 2 entries follow.
 *
 * Layout:
 *   03        = varint 3 = zigzag(-2) => block_count = -2
 *   0C        = varint 12 = zigzag(6) => block_size = 6 bytes
 *   02 61 14  = key "a" (len=1, 'a'), value int 10
 *   02 62 28  = key "b" (len=1, 'b'), value int 20
 *   00        = terminator
 */
static const char valid_neg_block_map[] = {
    '\x03',                     /* block_count = -2 */
    '\x0C',                     /* block_size = 6 */
    '\x02', '\x61', '\x14',    /* key "a", value 10 */
    '\x02', '\x62', '\x28',    /* key "b", value 20 */
    '\x00'                      /* terminator */
};

static int test_array_int64min(void)
{
    int rc;
    avro_schema_t schema = NULL;
    avro_value_iface_t *iface = NULL;
    avro_value_t value;
    avro_reader_t reader = NULL;

    rc = avro_schema_from_json_literal(
        "{\"type\":\"array\",\"items\":\"int\"}", &schema);
    if (rc != 0) {
        fprintf(stderr, "Failed to parse array schema: %s\n",
                avro_strerror());
        return 1;
    }

    iface = avro_generic_class_from_schema(schema);
    if (iface == NULL) {
        fprintf(stderr, "Failed to create array iface\n");
        avro_schema_decref(schema);
        return 1;
    }

    rc = avro_generic_value_new(iface, &value);
    if (rc != 0) {
        fprintf(stderr, "Failed to create array value: %s\n",
                avro_strerror());
        avro_value_iface_decref(iface);
        avro_schema_decref(schema);
        return 1;
    }

    reader = avro_reader_memory(int64min_block_count,
                                sizeof(int64min_block_count));
    if (reader == NULL) {
        fprintf(stderr, "Failed to create reader\n");
        avro_value_decref(&value);
        avro_value_iface_decref(iface);
        avro_schema_decref(schema);
        return 1;
    }

    /* This MUST fail with EINVAL rather than looping unboundedly or
     * triggering undefined behavior from negating INT64_MIN. */
    rc = avro_value_read(reader, &value);
    if (rc != EINVAL) {
        fprintf(stderr,
                "FAIL: INT64_MIN array block count: expected EINVAL, "
                "got rc=%d (%s)\n", rc, avro_strerror());
        avro_reader_free(reader);
        avro_value_decref(&value);
        avro_value_iface_decref(iface);
        avro_schema_decref(schema);
        return 1;
    }

    printf("PASS: INT64_MIN array block count rejected with EINVAL: %s\n",
           avro_strerror());

    avro_reader_free(reader);
    avro_value_decref(&value);
    avro_value_iface_decref(iface);
    avro_schema_decref(schema);
    return 0;
}

static int test_map_int64min(void)
{
    int rc;
    avro_schema_t schema = NULL;
    avro_value_iface_t *iface = NULL;
    avro_value_t value;
    avro_reader_t reader = NULL;

    rc = avro_schema_from_json_literal(
        "{\"type\":\"map\",\"values\":\"int\"}", &schema);
    if (rc != 0) {
        fprintf(stderr, "Failed to parse map schema: %s\n",
                avro_strerror());
        return 1;
    }

    iface = avro_generic_class_from_schema(schema);
    if (iface == NULL) {
        fprintf(stderr, "Failed to create map iface\n");
        avro_schema_decref(schema);
        return 1;
    }

    rc = avro_generic_value_new(iface, &value);
    if (rc != 0) {
        fprintf(stderr, "Failed to create map value: %s\n",
                avro_strerror());
        avro_value_iface_decref(iface);
        avro_schema_decref(schema);
        return 1;
    }

    reader = avro_reader_memory(int64min_block_count,
                                sizeof(int64min_block_count));
    if (reader == NULL) {
        fprintf(stderr, "Failed to create reader\n");
        avro_value_decref(&value);
        avro_value_iface_decref(iface);
        avro_schema_decref(schema);
        return 1;
    }

    /* This MUST fail with EINVAL rather than looping unboundedly or
     * triggering undefined behavior from negating INT64_MIN. */
    rc = avro_value_read(reader, &value);
    if (rc != EINVAL) {
        fprintf(stderr,
                "FAIL: INT64_MIN map block count: expected EINVAL, "
                "got rc=%d (%s)\n", rc, avro_strerror());
        avro_reader_free(reader);
        avro_value_decref(&value);
        avro_value_iface_decref(iface);
        avro_schema_decref(schema);
        return 1;
    }

    printf("PASS: INT64_MIN map block count rejected with EINVAL: %s\n",
           avro_strerror());

    avro_reader_free(reader);
    avro_value_decref(&value);
    avro_value_iface_decref(iface);
    avro_schema_decref(schema);
    return 0;
}

static int test_valid_neg_block_array(void)
{
    int rc;
    size_t count;
    avro_schema_t schema = NULL;
    avro_value_iface_t *iface = NULL;
    avro_value_t value;
    avro_reader_t reader = NULL;

    rc = avro_schema_from_json_literal(
        "{\"type\":\"array\",\"items\":\"int\"}", &schema);
    if (rc != 0) {
        fprintf(stderr, "Failed to parse array schema: %s\n",
                avro_strerror());
        return 1;
    }

    iface = avro_generic_class_from_schema(schema);
    if (iface == NULL) {
        fprintf(stderr, "Failed to create array iface\n");
        avro_schema_decref(schema);
        return 1;
    }

    rc = avro_generic_value_new(iface, &value);
    if (rc != 0) {
        fprintf(stderr, "Failed to create array value: %s\n",
                avro_strerror());
        avro_value_iface_decref(iface);
        avro_schema_decref(schema);
        return 1;
    }

    reader = avro_reader_memory(valid_neg_block_array,
                                sizeof(valid_neg_block_array));
    if (reader == NULL) {
        fprintf(stderr, "Failed to create reader\n");
        avro_value_decref(&value);
        avro_value_iface_decref(iface);
        avro_schema_decref(schema);
        return 1;
    }

    rc = avro_value_read(reader, &value);
    if (rc != 0) {
        fprintf(stderr,
                "FAIL: valid negative block count array rejected: %s\n",
                avro_strerror());
        avro_reader_free(reader);
        avro_value_decref(&value);
        avro_value_iface_decref(iface);
        avro_schema_decref(schema);
        return 1;
    }

    rc = avro_value_get_size(&value, &count);
    if (rc != 0 || count != 3) {
        fprintf(stderr,
                "FAIL: expected 3 elements, got %zu (rc=%d)\n",
                count, rc);
        avro_reader_free(reader);
        avro_value_decref(&value);
        avro_value_iface_decref(iface);
        avro_schema_decref(schema);
        return 1;
    }

    printf("PASS: valid negative block count array decoded (%zu items)\n",
           count);

    avro_reader_free(reader);
    avro_value_decref(&value);
    avro_value_iface_decref(iface);
    avro_schema_decref(schema);
    return 0;
}

static int test_valid_neg_block_map(void)
{
    int rc;
    size_t count;
    avro_schema_t schema = NULL;
    avro_value_iface_t *iface = NULL;
    avro_value_t value;
    avro_reader_t reader = NULL;

    rc = avro_schema_from_json_literal(
        "{\"type\":\"map\",\"values\":\"int\"}", &schema);
    if (rc != 0) {
        fprintf(stderr, "Failed to parse map schema: %s\n",
                avro_strerror());
        return 1;
    }

    iface = avro_generic_class_from_schema(schema);
    if (iface == NULL) {
        fprintf(stderr, "Failed to create map iface\n");
        avro_schema_decref(schema);
        return 1;
    }

    rc = avro_generic_value_new(iface, &value);
    if (rc != 0) {
        fprintf(stderr, "Failed to create map value: %s\n",
                avro_strerror());
        avro_value_iface_decref(iface);
        avro_schema_decref(schema);
        return 1;
    }

    reader = avro_reader_memory(valid_neg_block_map,
                                sizeof(valid_neg_block_map));
    if (reader == NULL) {
        fprintf(stderr, "Failed to create reader\n");
        avro_value_decref(&value);
        avro_value_iface_decref(iface);
        avro_schema_decref(schema);
        return 1;
    }

    rc = avro_value_read(reader, &value);
    if (rc != 0) {
        fprintf(stderr,
                "FAIL: valid negative block count map rejected: %s\n",
                avro_strerror());
        avro_reader_free(reader);
        avro_value_decref(&value);
        avro_value_iface_decref(iface);
        avro_schema_decref(schema);
        return 1;
    }

    rc = avro_value_get_size(&value, &count);
    if (rc != 0 || count != 2) {
        fprintf(stderr,
                "FAIL: expected 2 map entries, got %zu (rc=%d)\n",
                count, rc);
        avro_reader_free(reader);
        avro_value_decref(&value);
        avro_value_iface_decref(iface);
        avro_schema_decref(schema);
        return 1;
    }

    printf("PASS: valid negative block count map decoded (%zu entries)\n",
           count);

    avro_reader_free(reader);
    avro_value_decref(&value);
    avro_value_iface_decref(iface);
    avro_schema_decref(schema);
    return 0;
}

int main(void)
{
    int failures = 0;

    printf("Testing INT64_MIN block count rejection (CWE-190)...\n\n");

    failures += test_array_int64min();
    failures += test_map_int64min();
    failures += test_valid_neg_block_array();
    failures += test_valid_neg_block_map();

    printf("\n%s: %d test(s) failed\n",
           failures ? "FAIL" : "OK", failures);

    return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
