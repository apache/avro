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

#include <errno.h>
#include <getopt.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "avro.h"
#include "avro_private.h"


/* The compression codec to use. */
static const char  *codec = "null";

/* The block size to use. */
static size_t  block_size = 0;

/*-- PROCESSING A FILE --*/

static void
process_file(const char *in_filename, const char *out_filename)
{
	avro_file_reader_t  reader;
	avro_file_writer_t  writer;

	if (in_filename == NULL) {
		if (avro_file_reader_fp(stdin, "<stdin>", 0, &reader)) {
			fprintf(stderr, "Error opening <stdin>:\n  %s\n",
				avro_strerror());
			exit(1);
		}
	} else {
		if (avro_file_reader(in_filename, &reader)) {
			fprintf(stderr, "Error opening %s:\n  %s\n",
				in_filename, avro_strerror());
			exit(1);
		}
	}

	avro_schema_t  wschema;
	avro_value_iface_t  *iface;
	avro_value_t  value;
	int rval;

	wschema = avro_file_reader_get_writer_schema(reader);
	iface = avro_generic_class_from_schema(wschema);
	avro_generic_value_new(iface, &value);

	if (avro_file_writer_create_with_codec
	    (out_filename, wschema, &writer, codec, block_size)) {
		fprintf(stderr, "Error creating %s:\n  %s\n",
			out_filename, avro_strerror());
		exit(1);
	}

	while ((rval = avro_file_reader_read_value(reader, &value)) == 0) {
		if (avro_file_writer_append_value(writer, &value)) {
			fprintf(stderr, "Error writing to %s:\n  %s\n",
				out_filename, avro_strerror());
			exit(1);
		}
		avro_value_reset(&value);
	}

	if (rval != EOF) {
		fprintf(stderr, "Error reading value: %s", avro_strerror());
	}

	avro_file_reader_close(reader);
	avro_file_writer_close(writer);
	avro_value_decref(&value);
	avro_value_iface_decref(iface);
	avro_schema_decref(wschema);
}


/*-- MAIN PROGRAM --*/
static void usage(void)
{
	fprintf(stderr,
		"Usage: avromod [--codec=<compression codec>]\n"
		"               [--block-size=<block size>]\n"
		"               [<input avro file>]\n"
		"                <output avro file>\n");
}

static void
parse_block_size(const char *optarg)
{
	unsigned long  ul;
	char  *end;

	ul = strtoul(optarg, &end, 10);
	if ((ul == 0 && end == optarg) ||
	    (ul == ULONG_MAX && errno == ERANGE)) {
		fprintf(stderr, "Invalid block size: %s\n\n", optarg);
		usage();
		exit(1);
	}
	block_size = ul;
}


int main(int argc, char **argv)
{
	char  *in_filename;
	char  *out_filename;

        int i = 1;
        int ind_point = -1;
        int ind[2] = {0};
        char *p_str;
        while (i < argc) {
            if (argv[i][0] == '-') {
                if (argv[i][1] == '-') {
                    if (strncmp("block-size", argv[i]+2, strlen("block-size")) == 0) {
                        p_str = strchr(argv[i], '=');
                        if (p_str != NULL) {
                            parse_block_size(p_str+1);
                        } else {
                            goto opt_error;
                        }
                    } else if (strncmp("codec", argv[i]+2, strlen("codec")) == 0) {
                        p_str = strchr(argv[i], '=');
                        if (p_str != NULL) {
                            codec = p_str+1;
                        } else {
                            goto opt_error;
                        }
                    } else {
                        goto opt_error;
                    }
                } else {
                    goto opt_error;
                }
            } else {
                if (ind_point < 2) {
                    ind_point++;
                    ind[ind_point] = i;
                } else {
                    fprintf(stderr, "Can't read from multiple input files.\n");
                    goto opt_error;
                }
            }
            i++;
        }

        if (ind_point == 1) {
                in_filename = argv[ind[0]];
                out_filename = argv[ind[1]];
        } else if (ind_point == 0) {
                in_filename = NULL;
                out_filename = argv[ind[0]];
        } else {
                fprintf(stderr, "Can't read from multiple input files.\n");
                goto opt_error;
	}

	/* Process the data file */
	process_file(in_filename, out_filename);
	return 0;

opt_error:
        usage();
        exit(1);
}
