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

#include "avro.h"

#include <stdio.h>
#include <stdlib.h>

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic error "-Wswitch"
#endif

#define ASSERT_NOT_AVRO_INVALID(type)                                   \
	if (type == AVRO_INVALID) {                                     \
		fprintf(stderr, #type " collides with AVRO_INVALID\n"); \
		exit(EXIT_FAILURE);                                     \
	} else {                                                        \
		break;                                                  \
	}

#define CASE_ASSERTION(type) case type: ASSERT_NOT_AVRO_INVALID(type)

int main(void)
{
	avro_schema_t null_schema = avro_schema_null();
	avro_type_t type = avro_typeof(null_schema);
	avro_schema_decref(null_schema);

	switch (type) {
	CASE_ASSERTION(AVRO_STRING)
	CASE_ASSERTION(AVRO_BYTES)
	CASE_ASSERTION(AVRO_INT32)
	CASE_ASSERTION(AVRO_INT64)
	CASE_ASSERTION(AVRO_FLOAT)
	CASE_ASSERTION(AVRO_DOUBLE)
	CASE_ASSERTION(AVRO_BOOLEAN)
	CASE_ASSERTION(AVRO_NULL)
	CASE_ASSERTION(AVRO_RECORD)
	CASE_ASSERTION(AVRO_ENUM)
	CASE_ASSERTION(AVRO_FIXED)
	CASE_ASSERTION(AVRO_MAP)
	CASE_ASSERTION(AVRO_ARRAY)
	CASE_ASSERTION(AVRO_UNION)
	CASE_ASSERTION(AVRO_LINK)
	case AVRO_INVALID:
		break;
	}

	return EXIT_SUCCESS;
}
