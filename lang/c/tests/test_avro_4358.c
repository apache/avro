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

#include <stdio.h>
#include <stdlib.h>

#include "avro.h"

#define try(call, msg) \
	do { \
		if (call) { \
			fprintf(stderr, msg ":\n  %s\n", avro_strerror()); \
			return EXIT_FAILURE; \
		} \
	} while (0)

static const char  WRITER_JSON[] =
"{ \"type\": \"enum\", \"name\": \"Color\","
"  \"symbols\": [\"RED\", \"GREEN\", \"BLUE\"] }";

/* Reorders the symbols and leaves out GREEN */
static const char  READER_JSON[] =
"{ \"type\": \"enum\", \"name\": \"Color\","
"  \"symbols\": [\"BLUE\", \"RED\"] }";

int main(void)
{
	avro_schema_t  wschema;
	avro_schema_t  rschema;
	avro_value_t  src;
	avro_value_t  dest;
	avro_value_t  resolved_writer;
	avro_value_t  resolved_reader;
	int  val;

	try(avro_schema_from_json_literal(WRITER_JSON, &wschema),
	    "Cannot parse writer schema");
	try(avro_schema_from_json_literal(READER_JSON, &rschema),
	    "Cannot parse reader schema");

	avro_value_iface_t  *wclass = avro_generic_class_from_schema(wschema);
	avro_value_iface_t  *rclass = avro_generic_class_from_schema(rschema);
	avro_value_iface_t  *writer_resolver =
	    avro_resolved_writer_new(wschema, rschema);
	avro_value_iface_t  *reader_resolver =
	    avro_resolved_reader_new(wschema, rschema);
	if (writer_resolver == NULL || reader_resolver == NULL) {
		fprintf(stderr, "Cannot create resolvers:\n  %s\n",
			avro_strerror());
		return EXIT_FAILURE;
	}

	try(avro_generic_value_new(wclass, &src),
	    "Cannot create writer value");
	try(avro_generic_value_new(rclass, &dest),
	    "Cannot create reader value");
	try(avro_resolved_writer_new_value(writer_resolver, &resolved_writer),
	    "Cannot create resolved writer value");
	try(avro_resolved_reader_new_value(reader_resolver, &resolved_reader),
	    "Cannot create resolved reader value");
	avro_resolved_writer_set_dest(&resolved_writer, &dest);
	avro_resolved_reader_set_source(&resolved_reader, &src);

	/* RED is index 0 for the writer, and index 1 for the reader */
	try(avro_value_set_enum(&resolved_writer, 0), "Cannot set RED");
	try(avro_value_get_enum(&dest, &val), "Cannot get enum value");
	if (val != 1) {
		fprintf(stderr, "Resolved writer didn't match RED by name\n");
		return EXIT_FAILURE;
	}

	try(avro_value_set_enum(&src, 0), "Cannot set RED");
	try(avro_value_get_enum(&resolved_reader, &val),
	    "Cannot get enum value");
	if (val != 1) {
		fprintf(stderr, "Resolved reader didn't match RED by name\n");
		return EXIT_FAILURE;
	}

	/* GREEN isn't in the reader's enum */
	if (avro_value_set_enum(&resolved_writer, 1) == 0) {
		fprintf(stderr, "Resolved writer accepted a missing symbol\n");
		return EXIT_FAILURE;
	}

	try(avro_value_set_enum(&src, 1), "Cannot set GREEN");
	if (avro_value_get_enum(&resolved_reader, &val) == 0) {
		fprintf(stderr, "Resolved reader accepted a missing symbol\n");
		return EXIT_FAILURE;
	}

	avro_value_decref(&resolved_reader);
	avro_value_decref(&resolved_writer);
	avro_value_decref(&dest);
	avro_value_decref(&src);
	avro_value_iface_decref(reader_resolver);
	avro_value_iface_decref(writer_resolver);
	avro_value_iface_decref(rclass);
	avro_value_iface_decref(wclass);
	avro_schema_decref(rschema);
	avro_schema_decref(wschema);
	return EXIT_SUCCESS;
}
