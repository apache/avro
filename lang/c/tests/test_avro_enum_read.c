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
#include <avro.h>

#define ENUM_SCHEMA_LITERAL \
	"{\"type\":\"enum\",\"name\":\"Color\"," \
	"\"symbols\":[\"RED\",\"GREEN\",\"BLUE\"]}"

/* Read the given bytes as an enum value and report the result code. */
static int read_enum(avro_value_iface_t *iface,
		     const unsigned char *buf, size_t size)
{
	int rc;
	avro_value_t value;
	avro_reader_t reader;

	if (avro_generic_value_new(iface, &value) != 0) {
		fprintf(stderr, "avro_generic_value_new failed: %s\n",
			avro_strerror());
		exit(EXIT_FAILURE);
	}

	reader = avro_reader_memory((const char *) buf, size);
	if (reader == NULL) {
		fprintf(stderr, "avro_reader_memory failed\n");
		exit(EXIT_FAILURE);
	}

	rc = avro_value_read(reader, &value);

	avro_reader_free(reader);
	avro_value_decref(&value);
	return rc;
}

int main(void)
{
	avro_schema_t schema;
	avro_value_iface_t *iface;

	/* A valid in-range symbol index (1 -> "GREEN"), zig-zag encoded. */
	unsigned char valid[] = { 0x02 };
	/* An out-of-range symbol index (1000) for a 3-symbol enum. Left
	 * unchecked this index is stored and later dereferenced by
	 * avro_schema_enum_get, which returns an invalid pointer. */
	unsigned char out_of_range[] = { 0xd0, 0x0f };
	/* A negative symbol index (-1), zig-zag encoded. */
	unsigned char negative[] = { 0x01 };

	if (avro_schema_from_json_literal(ENUM_SCHEMA_LITERAL, &schema) != 0) {
		fprintf(stderr, "Failed to parse enum schema: %s\n",
			avro_strerror());
		return EXIT_FAILURE;
	}

	iface = avro_generic_class_from_schema(schema);
	if (iface == NULL) {
		fprintf(stderr, "Failed to create enum iface\n");
		avro_schema_decref(schema);
		return EXIT_FAILURE;
	}

	if (read_enum(iface, valid, sizeof(valid)) != 0) {
		fprintf(stderr, "Rejected a valid enum index: %s\n",
			avro_strerror());
		avro_value_iface_decref(iface);
		avro_schema_decref(schema);
		return EXIT_FAILURE;
	}

	if (read_enum(iface, out_of_range, sizeof(out_of_range)) == 0) {
		fprintf(stderr, "Accepted an out-of-range enum index\n");
		avro_value_iface_decref(iface);
		avro_schema_decref(schema);
		return EXIT_FAILURE;
	}

	if (read_enum(iface, negative, sizeof(negative)) == 0) {
		fprintf(stderr, "Accepted a negative enum index\n");
		avro_value_iface_decref(iface);
		avro_schema_decref(schema);
		return EXIT_FAILURE;
	}

	avro_value_iface_decref(iface);
	avro_schema_decref(schema);
	return EXIT_SUCCESS;
}
