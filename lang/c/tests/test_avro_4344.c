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
 * AVRO-4344: reading an out-of-range enum ordinal must return an error
 * rather than continuing with an out-of-range index (which previously led
 * avro_schema_enum_get() to return an uninitialized pointer).
 */

#include <avro.h>
#include <stdio.h>
#include <stdlib.h>

static avro_schema_t
make_enum_schema(void)
{
	/* Two symbols -> valid ordinals are 0 and 1. */
	avro_schema_t  schema = avro_schema_enum("suit");
	avro_schema_enum_symbol_append(schema, "SPADES");
	avro_schema_enum_symbol_append(schema, "HEARTS");
	return schema;
}

static int
read_enum(avro_value_iface_t *iface, const char *buf, size_t len)
{
	avro_reader_t  reader = avro_reader_memory(buf, len);
	avro_value_t   value;
	int            rval;

	avro_generic_value_new(iface, &value);
	rval = avro_value_read(reader, &value);
	avro_value_decref(&value);
	avro_reader_free(reader);
	return rval;
}

int main(void)
{
	avro_schema_t       schema = make_enum_schema();
	avro_value_iface_t *iface = avro_generic_class_from_schema(schema);

	/* A valid ordinal (1, zig-zag 0x02) must read successfully. */
	{
		const char  buf[] = { 0x02 };
		if (read_enum(iface, buf, sizeof(buf)) != 0) {
			fprintf(stderr, "Valid enum ordinal failed to read: %s\n",
				avro_strerror());
			exit(EXIT_FAILURE);
		}
	}

	/* An out-of-range positive ordinal (5, zig-zag 0x0A) must error. */
	{
		const char  buf[] = { 0x0a };
		if (read_enum(iface, buf, sizeof(buf)) == 0) {
			fprintf(stderr, "Out-of-range enum ordinal was accepted\n");
			exit(EXIT_FAILURE);
		}
	}

	/* A negative ordinal (-1, zig-zag 0x01) must error. */
	{
		const char  buf[] = { 0x01 };
		if (read_enum(iface, buf, sizeof(buf)) == 0) {
			fprintf(stderr, "Negative enum ordinal was accepted\n");
			exit(EXIT_FAILURE);
		}
	}

	/* avro_schema_enum_get() returns NULL (not garbage) for a bad index. */
	if (avro_schema_enum_get(schema, 99) != NULL) {
		fprintf(stderr, "avro_schema_enum_get did not return NULL for "
			"an out-of-range index\n");
		exit(EXIT_FAILURE);
	}

	avro_value_iface_decref(iface);
	avro_schema_decref(schema);
	exit(EXIT_SUCCESS);
}
