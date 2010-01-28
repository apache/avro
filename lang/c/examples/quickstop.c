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
add_person(avro_writer_t writer, const char *first, const char *last,
	   const char *phone, int32_t age)
{
	avro_datum_t person = avro_record("Person");

	avro_datum_t id_datum = avro_int64(++id);
	avro_datum_t first_datum = avro_string(first);
	avro_datum_t last_datum = avro_string(last);
	avro_datum_t age_datum = avro_int32(age);
	avro_datum_t phone_datum = avro_string(phone);

	if (avro_record_set(person, "ID", id_datum)
	    || avro_record_set(person, "First", first_datum)
	    || avro_record_set(person, "Last", last_datum)
	    || avro_record_set(person, "Age", age_datum)
	    || avro_record_set(person, "Phone", phone_datum)) {
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

int print_person(avro_reader_t reader, avro_schema_t reader_schema)
{
	int rval;
	avro_datum_t person;

	rval = avro_read_data(reader, person_schema, reader_schema, &person);
	if (rval == 0) {
		int64_t i64;
		int32_t i32;
		char *p;
		avro_datum_t id_datum, first_datum, last_datum, phone_datum,
		    age_datum;

		if (avro_record_get(person, "ID", &id_datum) == 0) {
			avro_int64_get(id_datum, &i64);
			fprintf(stdout, "%ld | ", i64);
		}
		if (avro_record_get(person, "First", &first_datum) == 0) {
			avro_string_get(first_datum, &p);
			fprintf(stdout, "%15s | ", p);
		}
		if (avro_record_get(person, "Last", &last_datum) == 0) {
			avro_string_get(last_datum, &p);
			fprintf(stdout, "%15s | ", p);
		}
		if (avro_record_get(person, "Phone", &phone_datum) == 0) {
			avro_string_get(phone_datum, &p);
			fprintf(stdout, "%15s | ", p);
		}
		if (avro_record_get(person, "Age", &age_datum) == 0) {
			avro_int32_get(age_datum, &i32);
			fprintf(stdout, "%ld", i32);
		}
		fprintf(stdout, "\n");

		/* We no longer need this memory */
		avro_datum_decref(person);
	}
	return rval;
}

int main(void)
{
	avro_reader_t reader;
	avro_writer_t writer;
	avro_schema_t projection_schema, first_name_schema, phone_schema;
	int64_t i;

	/* Initialize the schema structure from JSON */
	init_schema();

	/* Add people to the database */
	writer = avro_writer_memory(buf, sizeof(buf));
	add_person(writer, "Dante", "Hicks", "(555) 123-4567", 32);
	add_person(writer, "Randal", "Graves", "(555) 123-5678", 30);
	add_person(writer, "Veronica", "Loughran", "(555) 123-0987", 28);
	add_person(writer, "Caitlin", "Bree", "(555) 123-2323", 27);
	add_person(writer, "Bob", "Silent", "(555) 123-6422", 29);
	add_person(writer, "Jay", "???", "(555) 123-9182", 26);
	avro_writer_free(writer);

	fprintf(stdout,
		"\nAvro is compact. Here is the data for all %ld people.\n",
		id);
	avro_writer_dump(writer, stdout);

	fprintf(stdout, "\nNow let's read all the records back out\n");

	/* Read all the records and print them */
	reader = avro_reader_memory(buf, sizeof(buf));
	for (i = 0; i < id; i++) {
		if (print_person(reader, NULL)) {
			fprintf(stderr, "Error printing person\n");
			exit(EXIT_FAILURE);
		}
	}
	avro_reader_free(reader);

	/* You can also use projection, to only decode only the data you are
	   interested in.  This is particularly useful when you have 
	   huge data sets and you'll only interest in particular fields
	   e.g. your contacts First name and phone number */
	projection_schema = avro_schema_record("Person");
	first_name_schema = avro_schema_string();
	phone_schema = avro_schema_string();
	avro_schema_record_field_append(projection_schema, "First",
					first_name_schema);
	avro_schema_record_field_append(projection_schema, "Phone",
					phone_schema);

	/* Read only the record you're interested in */
	fprintf(stdout,
		"\n\nUse projection to print only the First name and phone numbers\n");
	reader = avro_reader_memory(buf, sizeof(buf));
	for (i = 0; i < id; i++) {
		if (print_person(reader, projection_schema)) {
			fprintf(stderr, "Error printing person\n");
			exit(EXIT_FAILURE);
		}
	}
	avro_reader_free(reader);
	avro_schema_decref(first_name_schema);
	avro_schema_decref(phone_schema);
	avro_schema_decref(projection_schema);

	/* We don't need this schema anymore */
	avro_schema_decref(person_schema);
	return 0;
}
