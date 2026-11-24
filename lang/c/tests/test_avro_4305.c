/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.  See the License for the specific language
 * governing permissions and limitations under the License.
 */

/*
 * Regression test for AVRO-4305: st_insert() must be OOM-safe. On allocation
 * failure it should return -1 and leave the table unchanged, rather than
 * dereferencing the NULL returned by avro_new() in ADD_DIRECT.
 */

#include <stdio.h>
#include <stdlib.h>

#include "avro.h"
#include "avro_private.h"
#include "st.h"

/* When armed, this allocator fails every allocation to simulate OOM. */
static int fail_allocations = 0;

static void *
oom_allocator(void *user_data, void *ptr, size_t osize, size_t nsize)
{
	(void) user_data;
	(void) osize;
	if (nsize == 0) {
		free(ptr);
		return NULL;
	}
	if (fail_allocations) {
		return NULL;
	}
	return realloc(ptr, nsize);
}

int main(void)
{
	avro_set_allocator(oom_allocator, NULL);

	st_table *table = st_init_strtable();
	if (table == NULL) {
		fprintf(stderr, "could not create table\n");
		return EXIT_FAILURE;
	}

	/* Arm OOM and try to insert a new key: st_insert must fail gracefully
	 * with -1 and must not modify the table (previously this dereferenced
	 * a NULL entry and crashed). */
	fail_allocations = 1;
	int rval = st_insert(table, (st_data_t) "key", (st_data_t) 42);
	fail_allocations = 0;

	if (rval != -1) {
		fprintf(stderr, "expected st_insert to return -1 on OOM, got %d\n", rval);
		return EXIT_FAILURE;
	}
	if (table->num_entries != 0) {
		fprintf(stderr, "table must be unchanged after OOM, num_entries=%d\n",
			table->num_entries);
		return EXIT_FAILURE;
	}

	/* After memory is available again the same insert must succeed. */
	rval = st_insert(table, (st_data_t) "key", (st_data_t) 42);
	if (rval != 0) {
		fprintf(stderr, "expected successful insert to return 0, got %d\n", rval);
		return EXIT_FAILURE;
	}

	st_data_t value = 0;
	if (!st_lookup(table, (st_data_t) "key", &value) || value != 42) {
		fprintf(stderr, "lookup after successful insert failed\n");
		return EXIT_FAILURE;
	}

	st_free_table(table);
	fprintf(stderr, "test_avro_4305 passed\n");
	return EXIT_SUCCESS;
}
