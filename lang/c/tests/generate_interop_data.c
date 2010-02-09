#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <avro.h>
#include "avro_private.h"

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
	avro_datum_t out_datum;

	if (argc != 3) {
		exit(EXIT_FAILURE);
	}
	snprintf(outpath, sizeof(outpath), "%s/c.avro", argv[2]);
	fprintf(stderr, "Writing %s\n", outpath);

	fp = fopen(argv[1], "r");
	rval = fread(jsontext, 1, sizeof(jsontext) - 1, fp);
	jsontext[rval] = '\0';

	check(rval,
	      avro_schema_from_json(jsontext, rval, &schema, &schema_error));
	check(rval, avro_file_writer_create(outpath, schema, &file_writer));

	/* TODO: create a method for generating random data from schema */
	interop = avro_record("interop", "org.apache.avro");
	avro_record_set(interop, "intField", avro_int32(42));
	avro_record_set(interop, "longField", avro_int64(4242));
	avro_record_set(interop, "stringField",
			avro_wrapstring("Follow your bliss."));
	avro_record_set(interop, "boolField", avro_boolean(1));
	avro_record_set(interop, "floatField", avro_float(3.14159265));
	avro_record_set(interop, "doubleField", avro_double(2.71828183));
	avro_record_set(interop, "bytesField", avro_bytes("abcd", 4));
	avro_record_set(interop, "nullField", avro_null());

	array_datum = avro_array();
	avro_array_append_datum(array_datum, avro_double(1.0));
	avro_array_append_datum(array_datum, avro_double(2.0));
	avro_array_append_datum(array_datum, avro_double(3.0));
	avro_record_set(interop, "arrayField", array_datum);

	avro_record_set(interop, "mapField", avro_map());
	avro_record_set(interop, "unionField", avro_double(1.61803399));
	avro_record_set(interop, "enumField", avro_enum("Kind", "B"));
	avro_record_set(interop, "fixedField",
			avro_fixed("MD5", "1234567890123456", 16));

	node_datum = avro_record("Node", NULL);
	avro_record_set(node_datum, "label",
			avro_wrapstring("If you label me, you negate me."));
	avro_record_set(node_datum, "children", avro_array());
	avro_record_set(interop, "recordField", node_datum);

	rval = avro_file_writer_append(file_writer, interop);
	if (rval) {
		fprintf(stderr, "Unable to append data to interop file!\n");
	}
	check(rval, avro_file_writer_close(file_writer));

	check(rval, avro_file_reader(outpath, &file_reader));
	check(rval, avro_file_reader_read(file_reader, NULL, &out_datum));
	if (!avro_datum_equal(interop, out_datum)) {
		exit(EXIT_FAILURE);
	}
	check(rval, avro_file_reader_close(file_reader));
	return 0;
}
