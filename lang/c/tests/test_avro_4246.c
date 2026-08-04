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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <avro.h>

#define STRING_SCHEMA_LITERAL "\"string\""
#define INT_SCHEMA_LITERAL "\"int\""

typedef struct
{
    char data[256];
    size_t size;
} encoded_buf_t;

static int encode_int(
    avro_value_iface_t *iface,
    int32_t number,
    encoded_buf_t *out_buf)
{
    int rc;
    avro_value_t value;
    avro_writer_t writer = NULL;

    memset(out_buf, 0, sizeof(*out_buf));

    rc = avro_generic_value_new(iface, &value);
    if (rc != 0)
    {
        fprintf(stderr, "encode_int: avro_generic_value_new failed: %s\n",
                avro_strerror());
        return rc;
    }

    rc = avro_value_set_int(&value, number);
    if (rc != 0)
    {
        fprintf(stderr, "encode_int: avro_value_set_int failed: %s\n",
                avro_strerror());
        avro_value_decref(&value);
        return rc;
    }

    writer = avro_writer_memory(out_buf->data, sizeof(out_buf->data));
    if (writer == NULL)
    {
        fprintf(stderr, "encode_int: avro_writer_memory failed\n");
        avro_value_decref(&value);
        return -1;
    }

    rc = avro_value_write(writer, &value);
    if (rc != 0)
    {
        fprintf(stderr, "encode_int: avro_value_write failed: %s\n",
                avro_strerror());
        avro_writer_free(writer);
        avro_value_decref(&value);
        return rc;
    }

    out_buf->size = avro_writer_tell(writer);

    avro_writer_free(writer);
    avro_value_decref(&value);
    return 0;
}

static int decode_as_string(
    avro_value_iface_t *string_iface,
    const void *buf,
    size_t buf_size,
    const char *label)
{
    int rc;
    avro_reader_t reader = NULL;
    avro_value_t decoded;
    const char *text = NULL;
    size_t text_size = 0;

    rc = avro_generic_value_new(string_iface, &decoded);
    if (rc != 0)
    {
        fprintf(stderr, "%s: avro_generic_value_new failed: %s\n",
                label, avro_strerror());
        return rc;
    }

    reader = avro_reader_memory((const char *)buf, buf_size);
    if (reader == NULL)
    {
        fprintf(stderr, "%s: avro_reader_memory failed\n", label);
        avro_value_decref(&decoded);
        return -1;
    }

    rc = avro_value_read(reader, &decoded);
    if (rc != 0)
    {
        fprintf(stderr, "%s: avro_value_read failed as expected? rc=%d err=%s\n",
                label, rc, avro_strerror());
        avro_reader_free(reader);
        avro_value_decref(&decoded);
        return rc;
    }

    rc = avro_value_get_string(&decoded, &text, &text_size);
    if (rc != 0)
    {
        fprintf(stderr, "%s: avro_value_get_string failed: %s\n",
                label, avro_strerror());
        avro_reader_free(reader);
        avro_value_decref(&decoded);
        return rc;
    }

    printf("%s: decoded successfully: \"%s\" (%zu bytes)\n",
           label, text, text_size);

    avro_reader_free(reader);
    avro_value_decref(&decoded);
    return 0;
}

int main(void)
{
    int rc;
    int ret = EXIT_SUCCESS;

    avro_schema_t string_schema = NULL;
    avro_schema_t int_schema = NULL;

    avro_value_iface_t *string_iface = NULL;
    avro_value_iface_t *int_iface = NULL;

    encoded_buf_t encoded_string;
    encoded_buf_t encoded_int;

    rc = avro_schema_from_json_literal(STRING_SCHEMA_LITERAL, &string_schema);
    if (rc != 0)
    {
        fprintf(stderr, "Failed to parse string schema: %s\n", avro_strerror());
        return EXIT_FAILURE;
    }

    rc = avro_schema_from_json_literal(INT_SCHEMA_LITERAL, &int_schema);
    if (rc != 0)
    {
        fprintf(stderr, "Failed to parse int schema: %s\n", avro_strerror());
        avro_schema_decref(string_schema);
        return EXIT_FAILURE;
    }

    string_iface = avro_generic_class_from_schema(string_schema);
    if (string_iface == NULL)
    {
        fprintf(stderr, "Failed to create string iface\n");
        avro_schema_decref(int_schema);
        avro_schema_decref(string_schema);
        return EXIT_FAILURE;
    }

    int_iface = avro_generic_class_from_schema(int_schema);
    if (int_iface == NULL)
    {
        fprintf(stderr, "Failed to create int iface\n");
        avro_value_iface_decref(string_iface);
        avro_schema_decref(int_schema);
        avro_schema_decref(string_schema);
        return EXIT_FAILURE;
    }

    rc = encode_int(int_iface, 12345, &encoded_int);
    if (rc != 0)
    {
        fprintf(stderr, "Encoding int failed\n");
        avro_value_iface_decref(int_iface);
        avro_value_iface_decref(string_iface);
        avro_schema_decref(int_schema);
        avro_schema_decref(string_schema);
        return EXIT_FAILURE;
    }

    printf("Encoded int size:    %zu bytes\n", encoded_int.size);

    rc = decode_as_string(string_iface,
                          encoded_int.data,
                          encoded_int.size,
                          "int payload as string");
    if (rc == 0)
    {
        fprintf(stderr, "Unexpected success decoding int payload as string\n");
        ret = EXIT_FAILURE;
    }
    else
    {
        printf("int payload as string: failed cleanly, as expected\n");
    }

    avro_value_iface_decref(int_iface);
    avro_value_iface_decref(string_iface);
    avro_schema_decref(int_schema);
    avro_schema_decref(string_schema);

    return ret;
}
