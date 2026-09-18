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

#include <avro.h>
#include <avro/consumer.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * A crafted binary datum can carry a union discriminant larger than the
 * number of writer branches. avro_resolver_union_branch used that value to
 * index child_resolvers directly, so an out-of-range discriminant read past
 * the array and handed avro_consume_binary a wild avro_consumer_t pointer.
 * Reading such a datum must fail cleanly instead.
 */

int main(void)
{
	const char *json = "[\"null\",\"string\"]";

	avro_schema_t writer = NULL;
	avro_schema_t reader = NULL;

	if (avro_schema_from_json_length(json, strlen(json), &writer)) {
		fprintf(stderr, "Cannot parse writer schema: %s\n",
			avro_strerror());
		exit(EXIT_FAILURE);
	}
	if (avro_schema_from_json_length(json, strlen(json), &reader)) {
		fprintf(stderr, "Cannot parse reader schema: %s\n",
			avro_strerror());
		exit(EXIT_FAILURE);
	}

	avro_consumer_t *resolver = avro_resolver_new(writer, reader);
	if (resolver == NULL) {
		fprintf(stderr, "Cannot create resolver: %s\n",
			avro_strerror());
		exit(EXIT_FAILURE);
	}

	/* Union discriminant 100, encoded as the long 0xC8 0x01. The writer
	 * union only has two branches. */
	char buf[] = { (char) 0xC8, 0x01 };
	avro_reader_t rdr = avro_reader_memory(buf, sizeof(buf));

	int rc = avro_consume_binary(rdr, resolver, NULL);
	if (rc == 0) {
		fprintf(stderr, "Expected an error for out-of-range "
			"union discriminant\n");
		exit(EXIT_FAILURE);
	}

	avro_reader_free(rdr);
	avro_consumer_free(resolver);
	avro_schema_decref(writer);
	avro_schema_decref(reader);
	exit(EXIT_SUCCESS);
}
