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
 * AVRO-4279: the block count of an array or map is read from the input and
 * drives allocation of the resulting collection. A very large or malformed
 * block count must be rejected instead of attempting an unbounded allocation.
 * The limit is configurable via the AVRO_MAX_COLLECTION_ITEMS environment
 * variable, which these tests set to a small value.
 */

#include <avro.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static avro_value_iface_t *
iface_for(const char *json)
{
	avro_schema_t schema = NULL;
	if (avro_schema_from_json_length(json, strlen(json), &schema)) {
		fprintf(stderr, "Cannot parse schema %s: %s\n", json, avro_strerror());
		exit(EXIT_FAILURE);
	}
	avro_value_iface_t *iface = avro_generic_class_from_schema(schema);
	avro_schema_decref(schema);
	if (iface == NULL) {
		fprintf(stderr, "Cannot create value interface: %s\n", avro_strerror());
		exit(EXIT_FAILURE);
	}
	return iface;
}

/* Attempt to decode `bytes` with the given interface. Returns the rc of the
 * read (0 on success, non-zero on error). */
static int
try_read(avro_value_iface_t *iface, const char *bytes, size_t len)
{
	avro_value_t value;
	avro_generic_value_new(iface, &value);
	avro_reader_t reader = avro_reader_memory(bytes, (int64_t) len);
	int rc = avro_value_read(reader, &value);
	avro_reader_free(reader);
	avro_value_decref(&value);
	return rc;
}

static void
expect_rejected(avro_value_iface_t *iface, const char *bytes, size_t len, const char *label)
{
	if (try_read(iface, bytes, len) == 0) {
		fprintf(stderr, "%s: expected read to be rejected but it succeeded\n", label);
		exit(EXIT_FAILURE);
	}
}

static void
expect_accepted(avro_value_iface_t *iface, const char *bytes, size_t len, const char *label)
{
	if (try_read(iface, bytes, len) != 0) {
		fprintf(stderr, "%s: expected read to succeed but it failed: %s\n",
			label, avro_strerror());
		exit(EXIT_FAILURE);
	}
}

int main(void)
{
	/* Cap collections at 10 items for the duration of the test. */
#ifdef _WIN32
	_putenv_s("AVRO_MAX_COLLECTION_ITEMS", "10");
#else
	setenv("AVRO_MAX_COLLECTION_ITEMS", "10", 1);
#endif

	avro_value_iface_t *array_iface =
	    iface_for("{\"type\": \"array\", \"items\": \"null\"}");
	avro_value_iface_t *map_iface =
	    iface_for("{\"type\": \"map\", \"values\": \"null\"}");

	/* A single block declaring 11 items (zigzag(11) = 0x16) exceeds the
	 * configured limit of 10 and must be rejected. The null items occupy no
	 * bytes, so a tiny input could otherwise drive a large allocation. */
	{
		const char bytes[] = { 0x16, 0x00 };
		expect_rejected(array_iface, bytes, sizeof(bytes), "array over limit");
		expect_rejected(map_iface, bytes, sizeof(bytes), "map over limit");
	}

	/* A negative count (unsigned varint 0x15 -> -11) uses its absolute value
	 * (11) as the item count and is followed by a block size (0x00). It must
	 * still be bounded. */
	{
		const char bytes[] = { 0x15, 0x00 };
		expect_rejected(array_iface, bytes, sizeof(bytes), "array negative count");
	}

	/* A block count of INT64_MIN (zigzag of 2^64-1: nine 0xFF bytes then
	 * 0x01) cannot be negated without signed-overflow UB and must be rejected
	 * outright. */
	{
		const char bytes[] = {
			(char) 0xFF, (char) 0xFF, (char) 0xFF, (char) 0xFF,
			(char) 0xFF, (char) 0xFF, (char) 0xFF, (char) 0xFF,
			(char) 0xFF, 0x01
		};
		expect_rejected(array_iface, bytes, sizeof(bytes), "array INT64_MIN count");
		expect_rejected(map_iface, bytes, sizeof(bytes), "map INT64_MIN count");
	}

	/* Two blocks of 6 items (zigzag(6) = 0x0c) exceed the limit cumulatively
	 * even though neither block does on its own. */
	{
		const char bytes[] = { 0x0c, 0x0c };
		expect_rejected(array_iface, bytes, sizeof(bytes), "array cumulative");
	}

	/* Three items (zigzag(3) = 0x06) then the end-of-array marker (0x00) is
	 * within the limit and must decode successfully. */
	{
		const char bytes[] = { 0x06, 0x00 };
		expect_accepted(array_iface, bytes, sizeof(bytes), "array within limit");
	}

	avro_value_iface_decref(array_iface);
	avro_value_iface_decref(map_iface);

	exit(EXIT_SUCCESS);
}
