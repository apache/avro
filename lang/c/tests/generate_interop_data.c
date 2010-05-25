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
#include <stdio.h>
#include <stdlib.h>
#include "config.h"

struct atom_holder {
	avro_atom_t arrayField;
	avro_atom_t boolField;
	avro_atom_t bytesField;
	avro_atom_t doubleField;
	avro_atom_t enumField;
	avro_atom_t fixedField;
	avro_atom_t floatField;
	avro_atom_t intField;
	avro_atom_t longField;
	avro_atom_t mapField;
	avro_atom_t nullField;
	avro_atom_t recordField;
	avro_atom_t stringField;
	avro_atom_t unionField;

	avro_atom_t children;
	avro_atom_t label;
} atoms;

void init_atoms(void)
{
	atoms.arrayField = avro_atom_add("arrayField");
	atoms.boolField = avro_atom_add("boolField");
	atoms.bytesField = avro_atom_add("bytesField");
	atoms.doubleField = avro_atom_add("doubleField");
	atoms.enumField = avro_atom_add("enumField");
	atoms.fixedField = avro_atom_add("fixedField");
	atoms.floatField = avro_atom_add("floatField");
	atoms.intField = avro_atom_add("intField");
	atoms.longField = avro_atom_add("longField");
	atoms.mapField = avro_atom_add("mapField");
	atoms.nullField = avro_atom_add("nullField");
	atoms.recordField = avro_atom_add("recordField");
	atoms.stringField = avro_atom_add("stringField");
	atoms.unionField = avro_atom_add("unionField");
	atoms.children = avro_atom_add("children");
	atoms.label = avro_atom_add("label");
}

void cleanup_atoms(void)
{
	avro_atom_decref(atoms.arrayField);
	avro_atom_decref(atoms.boolField);
	avro_atom_decref(atoms.bytesField);
	avro_atom_decref(atoms.doubleField);
	avro_atom_decref(atoms.enumField);
	avro_atom_decref(atoms.fixedField);
	avro_atom_decref(atoms.floatField);
	avro_atom_decref(atoms.intField);
	avro_atom_decref(atoms.longField);
	avro_atom_decref(atoms.mapField);
	avro_atom_decref(atoms.nullField);
	avro_atom_decref(atoms.recordField);
	avro_atom_decref(atoms.stringField);
	avro_atom_decref(atoms.unionField);
	avro_atom_decref(atoms.children);
	avro_atom_decref(atoms.label);
}

int main(int argc, char *argv[])
{
	int rval;
	avro_file_writer_t file_writer;
	avro_file_reader_t file_reader;
	char outpath[128];
	FILE *fp;
	char jsontext[16 * 1024];
	avro_schema_t schema;
	avro_schema_error_t schema_error;
	avro_datum_t interop;
	avro_datum_t array_datum;
	avro_datum_t node_datum;
	avro_datum_t union_datum;
	avro_datum_t out_datum;
	enum Kind {
		KIND_A,
		KIND_B,
		KIND_C
	};

	avro_init();
	init_atoms();

	if (argc != 3) {
		exit(EXIT_FAILURE);
	}
	snprintf(outpath, sizeof(outpath), "%s/c.avro", argv[2]);
	fprintf(stderr, "Writing to %s\n", outpath);

	fp = fopen(argv[1], "r");
	rval = fread(jsontext, 1, sizeof(jsontext) - 1, fp);
	jsontext[rval] = '\0';

	check(rval,
	      avro_schema_from_json(jsontext, rval, &schema, &schema_error));
	check(rval, avro_file_writer_create(outpath, schema, &file_writer));

	/* TODO: create a method for generating random data from schema */
	interop = avro_record("Interop", "org.apache.avro");
	avro_record_set(interop, atoms.intField, avro_int32(42));
	avro_record_set(interop, atoms.longField, avro_int64(4242));
	avro_record_set(interop, atoms.stringField,
			avro_wrapstring("Follow your bliss."));
	avro_record_set(interop, atoms.boolField, avro_boolean(1));
	avro_record_set(interop, atoms.floatField, avro_float(3.14159265));
	avro_record_set(interop, atoms.doubleField, avro_double(2.71828183));
	avro_record_set(interop, atoms.bytesField, avro_bytes("abcd", 4));
	avro_record_set(interop, atoms.nullField, avro_null());

	array_datum = avro_array();
	avro_array_append_datum(array_datum, avro_double(1.0));
	avro_array_append_datum(array_datum, avro_double(2.0));
	avro_array_append_datum(array_datum, avro_double(3.0));
	avro_record_set(interop, atoms.arrayField, array_datum);

	avro_record_set(interop, atoms.mapField, avro_map());
	union_datum = avro_union(1, avro_double(1.61803399));
	avro_record_set(interop, atoms.unionField, union_datum);
	avro_record_set(interop, atoms.enumField, avro_enum("Kind", KIND_A));
	avro_record_set(interop, atoms.fixedField,
			avro_fixed("MD5", "1234567890123456", 16));

	node_datum = avro_record("Node", NULL);
	avro_record_set(node_datum, atoms.label,
			avro_wrapstring("If you label me, you negate me."));
	avro_record_set(node_datum, atoms.children, avro_array());
	avro_record_set(interop, atoms.recordField, node_datum);

	rval = avro_file_writer_append(file_writer, interop);
	if (rval) {
		fprintf(stderr, "Unable to append data to interop file!\n");
		exit(EXIT_FAILURE);
	} else {
		fprintf(stderr, "Successfully appended datum to file\n");
	}

	check(rval, avro_file_writer_close(file_writer));
	fprintf(stderr, "Closed writer.\n");

	check(rval, avro_file_reader(outpath, &file_reader));
	fprintf(stderr, "Re-reading datum to verify\n");
	check(rval, avro_file_reader_read(file_reader, NULL, &out_datum));
	fprintf(stderr, "Verifying datum...");
	if (!avro_datum_equal(interop, out_datum)) {
		fprintf(stderr, "fail!\n");
		exit(EXIT_FAILURE);
	}
	fprintf(stderr, "ok\n");
	check(rval, avro_file_reader_close(file_reader));
	fprintf(stderr, "Closed reader.\n");
	cleanup_atoms();
	avro_shutdown();
	return 0;
}
