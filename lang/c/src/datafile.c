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
#include "avro/allocation.h"
#include "avro/errors.h"
#include "encoding.h"
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <string.h>

struct avro_file_reader_t_ {
	avro_schema_t writers_schema;
	avro_reader_t reader;
	char sync[16];
	int64_t blocks_read;
	int64_t blocks_total;
	int64_t current_blocklen;
};

struct avro_file_writer_t_ {
	avro_schema_t writers_schema;
	avro_writer_t writer;
	char sync[16];
	int block_count;
	size_t block_size;
	avro_writer_t datum_writer;
	char datum_buffer[16 * 1024];
};

/* TODO: should we just read /dev/random? */
static void generate_sync(avro_file_writer_t w)
{
	unsigned int i;
	srand(time(NULL));
	for (i = 0; i < sizeof(w->sync); i++) {
		w->sync[i] = ((double)rand() / (RAND_MAX + 1.0)) * 255;
	}
}

static int write_sync(avro_file_writer_t w)
{
	return avro_write(w->writer, w->sync, sizeof(w->sync));
}

static int write_header(avro_file_writer_t w)
{
	int rval;
	uint8_t version = 1;
	/* TODO: remove this static buffer */
	avro_writer_t schema_writer;
	char schema_buf[64 * 1024];
	const avro_encoding_t *enc = &avro_binary_encoding;
	int64_t schema_len;

	/* Generate random sync */
	generate_sync(w);

	check(rval, avro_write(w->writer, "Obj", 3));
	check(rval, avro_write(w->writer, &version, 1));

	check(rval, enc->write_long(w->writer, 3));
	check(rval, enc->write_string(w->writer, "avro.sync"));
	check(rval, enc->write_bytes(w->writer, w->sync, sizeof(w->sync)));
	check(rval, enc->write_string(w->writer, "avro.codec"));
	check(rval, enc->write_bytes(w->writer, "null", 4));
	check(rval, enc->write_string(w->writer, "avro.schema"));
	schema_writer = avro_writer_memory(schema_buf, sizeof(schema_buf));
	rval = avro_schema_to_json(w->writers_schema, schema_writer);
	if (rval) {
		avro_writer_free(schema_writer);
		return rval;
	}
	schema_len = avro_writer_tell(schema_writer);
	avro_writer_free(schema_writer);
	check(rval,
	      enc->write_bytes(w->writer, schema_buf, schema_len));
	check(rval, enc->write_long(w->writer, 0));
	return write_sync(w);
}

static int
file_writer_init_fp(const char *path, const char *mode, avro_file_writer_t w)
{
	FILE *fp = fopen(path, mode);
	if (!fp) {
		avro_set_error("Cannot open file for %s", path);
		return ENOMEM;
	}
	w->writer = avro_writer_file(fp);
	if (!w->writer) {
		avro_set_error("Cannot create file writer for %s", path);
		return ENOMEM;
	}
	return 0;
}

static int
file_writer_create(const char *path, avro_schema_t schema, avro_file_writer_t w)
{
	int rval;
	w->block_count = 0;
	rval = file_writer_init_fp(path, "wx", w);
	if (rval) {
		check(rval, file_writer_init_fp(path, "w", w));
	}

	w->datum_writer =
	    avro_writer_memory(w->datum_buffer, sizeof(w->datum_buffer));
	if (!w->datum_writer) {
		avro_set_error("Cannot create datum writer for file %s", path);
		avro_writer_free(w->writer);
		return ENOMEM;
	}

	w->writers_schema = schema;
	return write_header(w);
}

int
avro_file_writer_create(const char *path, avro_schema_t schema,
			avro_file_writer_t * writer)
{
	avro_file_writer_t w;
	int rval;
	check_param(EINVAL, path, "path");
	check_param(EINVAL, is_avro_schema(schema), "schema");
	check_param(EINVAL, writer, "writer");

	w = avro_new(struct avro_file_writer_t_);
	if (!w) {
		avro_set_error("Cannot allocate new file writer");
		return ENOMEM;
	}
	rval = file_writer_create(path, schema, w);
	if (rval) {
		avro_freet(struct avro_file_writer_t_, w);
		return rval;
	}
	*writer = w;
	return 0;
}

static int file_read_header(avro_reader_t reader,
			    avro_schema_t * writers_schema, char *sync,
			    int synclen)
{
	int rval;
	avro_schema_t meta_schema;
	avro_schema_t meta_values_schema;
	avro_datum_t meta;
	char magic[4];
	avro_datum_t schema_bytes;
	char *p;
	int64_t len;
	avro_schema_error_t schema_error;

	check(rval, avro_read(reader, magic, sizeof(magic)));
	if (magic[0] != 'O' || magic[1] != 'b' || magic[2] != 'j'
	    || magic[3] != 1) {
		avro_set_error("Incorrect Avro container file magic number");
		return EILSEQ;
	}

	meta_values_schema = avro_schema_bytes();
	meta_schema = avro_schema_map(meta_values_schema);
	rval = avro_read_data(reader, meta_schema, NULL, &meta);
	if (rval) {
		avro_prefix_error("Cannot read file header: ");
		return EILSEQ;
	}
	avro_schema_decref(meta_schema);

	rval = avro_map_get(meta, "avro.schema", &schema_bytes);
	if (rval) {
		avro_set_error("File header doesn't contain a schema");
		avro_datum_decref(meta);
		return rval;
	}

	avro_bytes_get(schema_bytes, &p, &len);
	rval = avro_schema_from_json(p, len, writers_schema, &schema_error);
	if (rval) {
		avro_prefix_error("Cannot parse file header: ");
		avro_datum_decref(meta);
		return rval;
	}

	avro_datum_decref(meta);
	return avro_read(reader, sync, synclen);
}

static int file_writer_open(const char *path, avro_file_writer_t w)
{
	int rval;
	FILE *fp;
	avro_reader_t reader;

	fp = fopen(path, "r");
	if (!fp) {
		avro_set_error("Error opening file: %s",
			       strerror(errno));
		return errno;
	}
	reader = avro_reader_file(fp);
	if (!reader) {
		fclose(fp);
		avro_set_error("Cannot create file reader for %s", path);
		return ENOMEM;
	}
	rval =
	    file_read_header(reader, &w->writers_schema, w->sync,
			     sizeof(w->sync));
	avro_reader_free(reader);
	/* Position to end of file and get ready to write */
	rval = file_writer_init_fp(path, "a", w);
	if (rval) {
		avro_freet(struct avro_file_writer_t_, w);
	}
	return rval;
}

int avro_file_writer_open(const char *path, avro_file_writer_t * writer)
{
	avro_file_writer_t w;
	int rval;
	check_param(EINVAL, path, "path");
	check_param(EINVAL, writer, "writer");

	w = avro_new(struct avro_file_writer_t_);
	if (!w) {
		avro_set_error("Cannot create new file writer for %s", path);
		return ENOMEM;
	}
	rval = file_writer_open(path, w);
	if (rval) {
		avro_freet(struct avro_file_writer_t_, w);
		return rval;
	}

	*writer = w;
	return 0;
}

static int file_read_block_count(avro_file_reader_t r)
{
	int rval;
	const avro_encoding_t *enc = &avro_binary_encoding;
	check_prefix(rval, enc->read_long(r->reader, &r->blocks_total),
		     "Cannot read file block count: ");
	check_prefix(rval, enc->read_long(r->reader, &r->current_blocklen),
		     "Cannot read file block size: ");
	r->blocks_read = 0;
	return 0;
}

int avro_file_reader(const char *path, avro_file_reader_t * reader)
{
	int rval;
	FILE *fp;
	avro_file_reader_t r = avro_new(struct avro_file_reader_t_);
	if (!r) {
		avro_set_error("Cannot allocate file reader for %s", path);
		return ENOMEM;
	}

	fp = fopen(path, "r");
	if (!fp) {
		avro_freet(struct avro_file_reader_t_, r);
		return errno;
	}
	r->reader = avro_reader_file(fp);
	if (!r->reader) {
		avro_set_error("Cannot allocate reader for file %s", path);
		avro_freet(struct avro_file_reader_t_, r);
		return ENOMEM;
	}

	rval = file_read_header(r->reader, &r->writers_schema, r->sync,
				sizeof(r->sync));
	if (rval) {
		avro_freet(struct avro_file_reader_t_, r);
		return rval;
	}

	rval = file_read_block_count(r);
	if (rval) {
		avro_freet(struct avro_file_reader_t_, r);
		return rval;
	}

	*reader = r;
	return rval;
}

static int file_write_block(avro_file_writer_t w)
{
	const avro_encoding_t *enc = &avro_binary_encoding;
	int rval;

	if (w->block_count) {
		/* Write the block count */
		check_prefix(rval, enc->write_long(w->writer, w->block_count),
			     "Cannot write file block count: ");
		/* Write the block length */
		check_prefix(rval, enc->write_long(w->writer, w->block_size),
			     "Cannot write file block size: ");
		/* Write the block */
		check_prefix(rval, avro_write(w->writer, w->datum_buffer, w->block_size),
			     "Cannot write file block: ");
		/* Write the sync marker */
		check_prefix(rval, write_sync(w),
			     "Cannot write sync marker: ");
		/* Reset the datum writer */
		avro_writer_reset(w->datum_writer);
		w->block_count = 0;
		w->block_size = 0;
	}
	return 0;
}

int avro_file_writer_append(avro_file_writer_t w, avro_datum_t datum)
{
	int rval;
	check_param(EINVAL, w, "writer");
	check_param(EINVAL, datum, "datum");

	rval = avro_write_data(w->datum_writer, w->writers_schema, datum);
	if (rval) {
		check(rval, file_write_block(w));
		rval =
		    avro_write_data(w->datum_writer, w->writers_schema, datum);
		if (rval) {
			avro_set_error("Datum too large for file block size");
			/* TODO: if the datum encoder larger than our buffer,
			   just write a single large datum */
			return rval;
		}
	}
	w->block_count++;
	w->block_size = avro_writer_tell(w->datum_writer);
	return 0;
}

int avro_file_writer_sync(avro_file_writer_t w)
{
	return file_write_block(w);
}

int avro_file_writer_flush(avro_file_writer_t w)
{
	int rval;
	check(rval, file_write_block(w));
	avro_writer_flush(w->writer);
	return 0;
}

int avro_file_writer_close(avro_file_writer_t w)
{
	int rval;
	check(rval, avro_file_writer_flush(w));
	avro_writer_free(w->datum_writer);
	avro_writer_free(w->writer);
	avro_freet(struct avro_file_writer_t_, w);
	return 0;
}

int avro_file_reader_read(avro_file_reader_t r, avro_schema_t readers_schema,
			  avro_datum_t * datum)
{
	int rval;
	char sync[16];

	check_param(EINVAL, r, "reader");
	check_param(EINVAL, datum, "datum");

	check(rval,
	      avro_read_data(r->reader, r->writers_schema, readers_schema,
			     datum));
	r->blocks_read++;

	if (r->blocks_read == r->blocks_total) {
		check(rval, avro_read(r->reader, sync, sizeof(sync)));
		if (memcmp(r->sync, sync, sizeof(r->sync)) != 0) {
			/* wrong sync bytes */
			avro_set_error("Incorrect sync bytes");
			return EILSEQ;
		}
		/* For now, ignore errors (e.g. EOF) */
		file_read_block_count(r);
	}
	return 0;
}

int avro_file_reader_close(avro_file_reader_t reader)
{
	avro_schema_decref(reader->writers_schema);
	avro_reader_free(reader->reader);
	avro_freet(struct avro_file_reader_t_, reader);
	return 0;
}
