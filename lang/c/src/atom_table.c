/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0 
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License. 
 */

#include "avro_private.h"

#include <stdlib.h>
#include <string.h>

typedef struct avro_atom_entry_t
{
	char *str;
	int64_t hash_value;
	int32_t length;
	int32_t next;
	int32_t refcount;
} avro_atom_entry_t;

struct avro_atom_table_t_
{
	avro_atom_entry_t *entries;
	int32_t *hashtab;
	int32_t size;
	int32_t count;
	int32_t freelist;
};

avro_atom_table_t g_avro_atom_table = NULL;

// ELF hash
static int32_t _atom_string_hash(const char *string)
{
	register int c;
	register unsigned int h = 0, g;

	while ((c = *string++) != '\0') {
		h = (h << 4) + c;
		if ((g = (h & 0xF0000000))) {
			h ^= g >> 24;
		}
		h &= ~g;
	}
	return h;
}

avro_atom_table_t avro_atom_table_create(int32_t size)
{
	avro_atom_table_t table;
	int32_t i;

	table = (avro_atom_table_t)malloc(sizeof(struct avro_atom_table_t_));
	table->size = size;
	table->count = 0;
	table->entries = malloc(sizeof(avro_atom_entry_t) * table->size);
	table->hashtab = malloc(sizeof(int32_t) * table->size);
	table->freelist = 0;

	memset(table->entries, 0, sizeof(avro_atom_entry_t) * table->size);
	memset(table->hashtab, -1, sizeof(int32_t) * table->size);

	for (i = 0; i < table->size; i++) {
		table->entries[i].next = i + 1;
	}

	table->entries[table->size - 1].next = -1;

	return table;
}

void avro_atom_table_destroy(avro_atom_table_t table)
{
	int32_t i;
	for (i = 0; i < table->size; i++) {
		if (table->entries[i].str) {
			free(table->entries[i].str);
		}
	}
	free(table->entries);
	free(table->hashtab);
	free(table);
}

void avro_atom_table_dump(avro_atom_table_t table)
{
	int32_t atom;
	printf("Atom table dump:\n");
	for (atom = 0; atom < table->size; atom++) {
		avro_atom_entry_t *entry = &(table->entries[atom]);
		if (entry->str) {
			printf("	%d - %s - %d refs\n", atom, entry->str, entry->refcount);
		}
	}
	printf("--\n");
}

avro_atom_t avro_atom_table_add(avro_atom_table_t table, const char *str)
{
	return avro_atom_table_add_length(table, str, strlen(str));
}

avro_atom_t avro_atom_table_add_length(avro_atom_table_t table, const char *str, int32_t length)
{
	int32_t hash_value = _atom_string_hash(str);
	int32_t ind, new_size, old_size;
	avro_atom_t atom;
	avro_atom_entry_t *entry;

	/* Look for an existing identifier. */
	atom = table->hashtab[hash_value % table->size];
	while (atom != -1) {
		if (table->entries[atom].hash_value == hash_value &&
			table->entries[atom].length == length &&
			strcmp(table->entries[atom].str, str) == 0) {
			table->entries[atom].refcount++;
			return atom;
		}
		atom = table->entries[atom].next;
	}

	/* Check if we have to resize the table. */
	if (table->freelist == -1) {
		int32_t i;

		/* Allocate new space for table. */
		if (table->size > 4096) {
			new_size = table->size + 4096;
		} else {
			new_size = table->size * 2;
		}

		table->entries = realloc(table->entries, sizeof(avro_atom_entry_t) * new_size);
		table->hashtab = realloc(table->hashtab, sizeof(int32_t) * new_size);

		/* Make new string of freelist. */
		memset(&(table->entries[table->size]), 0, sizeof(avro_atom_entry_t) * (new_size-table->size));
		for (ind = table->size; ind < new_size - 1; ind++) {
			table->entries[ind].next = ind + 1;
		}
		table->entries[ind].next = -1;
		table->freelist = table->size;

		old_size = table->size;
		table->size = new_size;

		memset(table->hashtab, -1, sizeof(int64_t) * new_size);
		for (i = 0; i < old_size; i++) {
			if (table->entries[i].str) {
				int32_t bucket = table->entries[i].hash_value % table->size;
				table->entries[i].next = table->hashtab[bucket];
				table->hashtab[bucket] = i;
			}
		}
	}

	/* Use one off of the free list */
	atom = table->freelist;
	entry = &(table->entries[atom]);
	table->freelist = entry->next;
	entry->str = strdup(str);
	entry->length = length;
	entry->refcount = 1;
	entry->hash_value = hash_value;
	/* Link into the hash chain */
	entry->next = table->hashtab[hash_value % table->size];
	table->hashtab[hash_value % table->size] = atom;

	table->count++;

	return atom;
}

avro_atom_t avro_atom_table_lookup(avro_atom_table_t table, const char *str, int32_t length)
{
	int32_t hash_value = _atom_string_hash(str);
	avro_atom_t atom;

	/* Look for an existing identifier. */
	atom = table->hashtab[hash_value % table->size];
	while (atom != -1) {
		if (table->entries[atom].hash_value == hash_value &&
			table->entries[atom].length == length &&
			strcmp(table->entries[atom].str, str) == 0) {
			table->entries[atom].refcount++;
			return atom;
		}
		atom = table->entries[atom].next;
	}
	return -1;
}

int avro_atom_table_describe(avro_atom_table_t table, avro_atom_t atom, const char **s, int32_t *length)
{
	if (NULL != table->entries[atom].str) {
		*s = table->entries[atom].str;
		*length = table->entries[atom].length;
	}
	return -1;
}

const char *avro_atom_table_to_string(avro_atom_table_t table, avro_atom_t atom)
{
	if (NULL != table->entries[atom].str) {
		return table->entries[atom].str;
	}
	return "(invalid atom)";
}

avro_atom_t avro_atom_table_incref(avro_atom_table_t table, avro_atom_t atom)
{
	table->entries[atom].refcount++;
	return atom;
}

void avro_atom_table_decref(avro_atom_table_t table, avro_atom_t atom)
{
	int32_t bucket, *p;

	table->entries[atom].refcount--;

	if (0 == table->entries[atom].refcount) {
		/* Get the hash table thread for this entry. */
		bucket = _atom_string_hash(table->entries[atom].str) % table->size;

		/* Free the string. */
		free(table->entries[atom].str);
		table->entries[atom].str = NULL;
		table->entries[atom].length = 0;
		table->entries[atom].hash_value = 0;

		/* Find the pointer to this entry. */
		for (p = &table->hashtab[bucket]; p && *p != atom; p = &table->entries[*p].next);

		/* Remove this entry and add it to freelist. */
		*p = table->entries[atom].next;
		table->entries[atom].next = table->freelist;
		table->freelist = atom;
		table->count--;
	}
}

