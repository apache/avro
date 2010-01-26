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
#include <stdio.h>
#include <stdlib.h>
#include <avro.h>

char buf[4096];
avro_schema_t person_schema;
avro_reader_t reader;
avro_writer_t writer;
int64_t id = 0;

/* A simple schema for our tutorial */
#define PERSON_SCHEMA \
"{\"type\":\"record\",\
  \"name\":\"Person\",\
  \"fields\":[\
     {\"name\": \"ID\", \"type\": \"long\"},\
     {\"name\": \"First\", \"type\": \"string\"},\
     {\"name\": \"Last\", \"type\": \"string\"},\
     {\"name\": \"Phone\", \"type\": \"string\"},\
     {\"name\": \"Age\", \"type\": \"int\"}]}"

/* Parse schema into a schema data structure */
void init_schema(void)
{
	avro_schema_error_t error;
	if (avro_schema_from_json(PERSON_SCHEMA, sizeof(PERSON_SCHEMA),
				  &person_schema, &error)) {
		fprintf(stderr, "Unable to parse person schema\n");
		exit(EXIT_FAILURE);
	}
}

/* Create a datum to match the person schema and save it */
void
add_person(const char *first, const char *last, const char *phone, int32_t age)
{
	avro_datum_t person = avro_record("Person");

	avro_datum_t id_datum = avro_int64(++id);
	avro_datum_t first_datum = avro_string(first);
	avro_datum_t last_datum = avro_string(last);
	avro_datum_t age_datum = avro_int32(age);
	avro_datum_t phone_datum = avro_string(phone);

	if (avro_record_field_set(person, "ID", id_datum)
	    || avro_record_field_set(person, "First", first_datum)
	    || avro_record_field_set(person, "Last", last_datum)
	    || avro_record_field_set(person, "Age", age_datum)
	    || avro_record_field_set(person, "Phone", phone_datum)) {
		fprintf(stderr, "Unable to create Person datum structure");
		exit(EXIT_FAILURE);
	}

	if (avro_write_data(writer, person_schema, person)) {
		fprintf(stderr,
			"Unable to write Person datum to memory buffer");
		exit(EXIT_FAILURE);
	}

	/* Decrement all our references to prevent memory from leaking */
	avro_datum_decref(id_datum);
	avro_datum_decref(first_datum);
	avro_datum_decref(last_datum);
	avro_datum_decref(age_datum);
	avro_datum_decref(phone_datum);
	avro_datum_decref(person);

	fprintf(stdout, "Successfully added %s, %s id=%ld\n", last, first, id);
}

int print_person(void)
{
	int rval;
	avro_datum_t person;

	rval = avro_read_data(reader, person_schema, NULL, &person);
	if (rval == 0) {
		int64_t i64;
		int32_t i32;
		char *p;
		avro_datum_t id_datum, first_datum, last_datum, phone_datum,
		    age_datum;

		id_datum = avro_record_field_get(person, "ID");
		first_datum = avro_record_field_get(person, "First");
		last_datum = avro_record_field_get(person, "Last");
		phone_datum = avro_record_field_get(person, "Phone");
		age_datum = avro_record_field_get(person, "Age");

		avro_int64_get(id_datum, &i64);
		fprintf(stdout, "%ld | ", i64);
		avro_string_get(first_datum, &p);
		fprintf(stdout, "%15s | ", p);
		avro_string_get(last_datum, &p);
		fprintf(stdout, "%15s | ", p);
		avro_string_get(phone_datum, &p);
		fprintf(stdout, "%25s | ", p);
		avro_int32_get(age_datum, &i32);
		fprintf(stdout, "  %2d\n", i32);

		/* We no longer need this memory */
		avro_datum_decref(person);
	}
	return rval;
}

int main(void)
{
	int64_t i;

	/* Create readers and writers backed by memory */
	writer = avro_writer_memory(buf, sizeof(buf));
	reader = avro_reader_memory(buf, sizeof(buf));

	/* Initialize the schema structure from JSON */
	init_schema();

	/* Add people to the database */
	add_person("Dante", "Hicks", "(555) 123-4567", 32);
	add_person("Randal", "Graves", "(555) 123-5678", 30);
	add_person("Veronica", "Loughran", "(555) 123-0987", 28);
	add_person("Caitlin", "Bree", "(555) 123-2323", 27);
	add_person("Bob", "Silent", "(555) 123-6422", 29);
	add_person("Jay", "???", "(555) 123-9182", 26);

	fprintf(stdout,
		"\nAvro is compact. Here is the data for all %ld people.\n",
		id);
	avro_writer_dump(writer, stdout);

	fprintf(stdout, "\nNow let's read all the records back out\n");

	/* Read all the records and print them */
	for (i = 0; i < id; i++) {
		if (print_person()) {
			fprintf(stderr, "Error printing person\n");
			exit(EXIT_FAILURE);
		}
	}

	/* We don't need this schema anymore */
	avro_schema_decref(person_schema);

	/* We dont' need the reader/writer anymore */
	avro_reader_free(reader);
	avro_writer_free(writer);
	return 0;
}
