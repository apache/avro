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

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "avro.h"
#include "avro_private.h"


/*-- PROCESSING A FILE --*/

static void
process_file(const char *filename)
{
	avro_file_reader_t  reader;

	if (filename == NULL) {
		if (avro_file_reader_fp(stdin, "<stdin>", 0, &reader)) {
			fprintf(stderr, "Error opening <stdin>:\n  %s\n",
				strerror(errno));
			exit(1);
		}
	} else {
		if (avro_file_reader(filename, &reader)) {
			fprintf(stderr, "Error opening %s:\n  %s\n",
				filename, strerror(errno));
			exit(1);
		}
	}

	avro_schema_t  wschema;
	avro_value_iface_t  *iface;
	avro_value_t  value;

	wschema = avro_file_reader_get_writer_schema(reader);
	iface = avro_generic_class_from_schema(wschema);
	avro_generic_value_new(iface, &value);

	while (avro_file_reader_read_value(reader, &value) == 0) {
		char  *json;
		avro_value_to_json(&value, 1, &json);
		printf("%s\n", json);
		free(json);
		avro_value_reset(&value);
	}

	avro_file_reader_close(reader);
	avro_value_decref(&value);
	avro_value_iface_decref(iface);
}


/*-- MAIN PROGRAM --*/

static void usage(void)
{
	fprintf(stderr,
		"Usage: avrocat <avro data file>\n");
}


int main(int argc, char **argv)
{
	char  *data_filename;

	if (argc == 2) {
		data_filename = argv[1];
	} else if (argc == 1) {
		data_filename = NULL;
	} else {
		fprintf(stderr, "Can't read from multiple input files.\n");
		usage();
		exit(1);
	}

	/* Process the data file */
	process_file(data_filename);
	return 0;
}
