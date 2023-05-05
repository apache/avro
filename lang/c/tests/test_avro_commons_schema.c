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
#include "avro_private.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#ifdef _WIN32
 #include "msdirent.h"
#else
 #include <dirent.h>
#endif

avro_writer_t avro_stderr;

static avro_schema_t read_common_schema_test(const char *dirpath) {
    char schemafilepath[1024];
    char jsontext[4096];

    avro_schema_t schema;
    int n = snprintf(schemafilepath, sizeof(schemafilepath),  "%s/schema.json", dirpath);
    if (n < 0) {
        fprintf(stderr, "Size of dir path is too long %s !\n", dirpath);
        exit(EXIT_FAILURE);
    }
    FILE* fp = fopen(schemafilepath, "r");
    if (!fp) {
        fprintf(stderr, "can't open file %s !\n", schemafilepath);
        exit(EXIT_FAILURE);
    }
    int rval = fread(jsontext, 1, sizeof(jsontext) - 1, fp);
    fclose(fp);
    jsontext[rval] = '\0';

    int test_rval = avro_schema_from_json(jsontext, 0, &schema, NULL);
    if (test_rval != 0) {
        fprintf(stderr, "fail! Can' read schema from file %s\n", schemafilepath);
        exit(EXIT_FAILURE);
    }
    return schema;
}

static void create_writer(avro_schema_t schema, avro_file_writer_t* writer)
{
    // create / reset copy.avro file.
    FILE* copyFile = fopen("./copy.avro", "w");
    if (!copyFile) {
        fprintf(stderr, "can't create file copy.avro !\n");
        exit(EXIT_FAILURE);
    }
    fclose(copyFile);

    // create avro writer on file.
	if (avro_file_writer_create("./copy.avro", schema, writer))	{
		fprintf(stdout, "\nThere was an error creating db: %s", avro_strerror());
		exit(EXIT_FAILURE);
	}
}

static void read_data(const char *dirpath, avro_schema_t schema) {
    char datafilepath[1024];
    int n = snprintf(datafilepath, sizeof(datafilepath),  "%s/data.avro", dirpath);
    if (n < 0) {
        fprintf(stderr, "Size of dir path is too long %s/data.avro !\n", dirpath);
        exit(EXIT_FAILURE);
    }

    avro_file_reader_t reader;
    avro_datum_t  datum;
    int rval = avro_file_reader(datafilepath, &reader);
    if (rval) {
        exit(EXIT_FAILURE);
    }

    avro_file_writer_t writer;
    create_writer(schema, &writer);

    int records_read = 0;
    while ((rval = avro_file_reader_read(reader, schema, &datum)) == 0) {
        records_read++;
        if (avro_file_writer_append(writer, datum)) {
            fprintf(stdout, "\nCan't write record: %s\n", avro_strerror());
        	exit(EXIT_FAILURE);
        }

    	avro_datum_decref(datum);
    }
    fprintf(stdout, "\nExit run test OK => %d records", records_read);
    remove("./copy.avro");
    fflush(stdout);
    avro_file_reader_close(reader);
    avro_file_writer_close(writer);
}

static void run_tests(const char *dirpath)
{
    fprintf(stdout, "\nRun test for path '%s'", dirpath);
    avro_schema_t schema = read_common_schema_test(dirpath);
    read_data(dirpath, schema);
    avro_schema_decref(schema);
}



int main(int argc, char *argv[])
{
	char *srcdir = "../../../share/test/data/schemas";
	AVRO_UNUSED(argc);
	AVRO_UNUSED(argv);

	avro_stderr = avro_writer_file(stderr);

	DIR* dir = opendir(srcdir);
    if (dir == NULL) {
    	fprintf(stdout, "Unable to open '%s'\n", srcdir);
    	fflush(stdout);
    	exit(EXIT_FAILURE);
    }
    struct dirent *dent;
    do {
       dent = readdir(dir);

        if (dent && dent->d_name[0] != '.' && dent->d_type == DT_DIR) {
            char filepath[1024];
            snprintf(filepath, sizeof(filepath), "%s/%s", srcdir, dent->d_name);
            run_tests(filepath);
        }
    }
    while(dent != NULL);
    closedir(dir);

	avro_writer_free(avro_stderr);
	return EXIT_SUCCESS;
}
