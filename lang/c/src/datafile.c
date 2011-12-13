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
#include "avro/generic.h"
#include "avro/errors.h"
#include "avro/value.h"
#include "encoding.h"
#include "codec.h"
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <string.h>

struct avro_file_reader_t_ {
	avro_schema_t writers_schema;
	avro_reader_t reader;
	avro_reader_t block_reader;
	avro_codec_t codec;
	char sync[16];
	int64_t blocks_read;
	int64_t blocks_total;
	int64_t current_blocklen;
	char * current_blockdata;
};

struct avro_file_writer_t_ {
	avro_schema_t writers_schema;
	avro_writer_t writer;
	avro_codec_t codec;
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
	check(rval, enc->write_bytes(w->writer, w->codec->name, strlen(w->codec->name)));
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
	return avro_file_writer_create_with_codec(path, schema, writer, "null");
}

int avro_file_writer_create_with_codec(const char *path,
			avro_schema_t schema, avro_file_writer_t * writer,
			const char *codec)
{
	avro_file_writer_t w;
	int rval;
	check_param(EINVAL, path, "path");
	check_param(EINVAL, is_avro_schema(schema), "schema");
	check_param(EINVAL, writer, "writer");
	check_param(EINVAL, codec, "codec");

	w = avro_new(struct avro_file_writer_t_);
	if (!w) {
		avro_set_error("Cannot allocate new file writer");
		return ENOMEM;
	}
	w->codec = avro_new(struct avro_codec_t_);
	if (!w->codec) {
		avro_set_error("Cannot allocate new codec");
		return ENOMEM;
	}	
	rval = avro_codec(w->codec, codec);
	if (rval) {
		avro_freet(struct avro_codec_t_, w->codec);
		return rval;
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
			    avro_schema_t * writers_schema, avro_codec_t codec,
			    char *sync, int synclen)
{
	int rval;
	avro_schema_t meta_schema;
	avro_schema_t meta_values_schema;
	avro_value_iface_t *meta_iface;
	avro_value_t meta;
	char magic[4];
	avro_value_t codec_val;
	avro_value_t schema_bytes;
	const void *p;
	size_t len;
	avro_schema_error_t schema_error;

	check(rval, avro_read(reader, magic, sizeof(magic)));
	if (magic[0] != 'O' || magic[1] != 'b' || magic[2] != 'j'
	    || magic[3] != 1) {
		avro_set_error("Incorrect Avro container file magic number");
		return EILSEQ;
	}

	meta_values_schema = avro_schema_bytes();
	meta_schema = avro_schema_map(meta_values_schema);
	meta_iface = avro_generic_class_from_schema(meta_schema);
	if (meta_iface == NULL) {
		return EILSEQ;
	}
	check(rval, avro_generic_value_new(meta_iface, &meta));
	rval = avro_value_read(reader, &meta);
	if (rval) {
		avro_prefix_error("Cannot read file header: ");
		return EILSEQ;
	}
	avro_schema_decref(meta_schema);

	rval = avro_value_get_by_name(&meta, "avro.codec", &codec_val, NULL);
	if (rval) {
		avro_set_error("File header doesn't contain a codec");
		avro_value_decref(&meta);
		return rval;
	} else {
		const void *buf;
		size_t size;
		const char *codec_name;

		avro_type_t type = avro_value_get_type(&codec_val);

		if (type != AVRO_BYTES) {
			avro_set_error("Value type of codec is unexpected");
			avro_value_decref(&meta);
			return EILSEQ;
		}

		avro_value_get_bytes(&codec_val, &buf, &size);

		if (size == 4 && strncmp((const char *) buf, "null", 4) == 0) {
			codec_name = "null";
		} else if (size == 7
			   && strncmp((const char *) buf, "deflate", 7) == 0) {
			codec_name = "deflate";
		} else {
			avro_set_error("File header contains an unknown codec");
			avro_value_decref(&meta);
			return EILSEQ;
		}

		avro_codec(codec, codec_name);
	}

	rval = avro_value_get_by_name(&meta, "avro.schema", &schema_bytes, NULL);
	if (rval) {
		avro_set_error("File header doesn't contain a schema");
		avro_value_decref(&meta);
		return rval;
	}

	avro_value_get_bytes(&schema_bytes, &p, &len);
	rval = avro_schema_from_json(p, len, writers_schema, &schema_error);
	if (rval) {
		avro_prefix_error("Cannot parse file header: ");
		avro_value_decref(&meta);
		return rval;
	}

	avro_value_decref(&meta);
	avro_value_iface_decref(meta_iface);
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
	    file_read_header(reader, &w->writers_schema, w->codec, w->sync,
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
	int64_t len;
	const avro_encoding_t *enc = &avro_binary_encoding;
	check_prefix(rval, enc->read_long(r->reader, &r->blocks_total),
		     "Cannot read file block count: ");
	check_prefix(rval, enc->read_long(r->reader, &len),
		     "Cannot read file block size: ");

	if (r->current_blockdata) {
		r->current_blockdata = avro_realloc(r->current_blockdata, r->current_blocklen, len);
	} else {
		r->current_blockdata = avro_malloc(len);
	}

	r->current_blocklen = len;

	check_prefix(rval, avro_read(r->reader, r->current_blockdata, len),
		     "Cannot read file block: ");
	if (len < r->current_blocklen) {
		fprintf(stderr, "Len: %lld Blocklen: %lld\n", len, r->current_blocklen);
		avro_set_error("Could not read entire block");
		return 1;
	}

	avro_codec_decode(r->codec, r->current_blockdata, r->current_blocklen);

	avro_reader_memory_set_source(r->block_reader, r->codec->block_data, r->codec->block_size);

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
	r->block_reader = avro_reader_memory(0, 0);
	if (!r->block_reader) {
		avro_set_error("Cannot allocate block reader for file %s", path);
		avro_freet(struct avro_file_reader_t_, r);
		return ENOMEM;
	}

	r->codec = avro_new(struct avro_codec_t_);
	if (!r->codec) {
		avro_set_error("Could not allocate codec for file %s", path);
		avro_freet(struct avro_file_reader_t_, r);
		return ENOMEM;
	}

	rval = file_read_header(r->reader, &r->writers_schema, r->codec,
				r->sync, sizeof(r->sync));
	if (rval) {
		avro_freet(struct avro_file_reader_t_, r);
		return rval;
	}

	r->current_blockdata = NULL;
	r->current_blocklen = 0;

	rval = file_read_block_count(r);
	if (rval) {
		avro_freet(struct avro_file_reader_t_, r);
		return rval;
	}

	*reader = r;
	return rval;
}

avro_schema_t
avro_file_reader_get_writer_schema(avro_file_reader_t r)
{
	check_param(NULL, r, "reader");
	return r->writers_schema;
}

static int file_write_block(avro_file_writer_t w)
{
	const avro_encoding_t *enc = &avro_binary_encoding;
	int rval;

	if (w->block_count) {
		/* Write the block count */
		check_prefix(rval, enc->write_long(w->writer, w->block_count),
			     "Cannot write file block count: ");
		/* Encode the block */
		check_prefix(rval, avro_codec_encode(w->codec, w->datum_buffer, w->block_size),
			     "Cannot encode file block: ");		
		/* Write the block length */
		check_prefix(rval, enc->write_long(w->writer, w->codec->block_size),
			     "Cannot write file block size: ");
		/* Write the block */
		check_prefix(rval, avro_write(w->writer, w->codec->block_data, w->codec->block_size),
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

int
avro_file_writer_append_value(avro_file_writer_t w, avro_value_t *value)
{
	int rval;
	check_param(EINVAL, w, "writer");
	check_param(EINVAL, value, "value");

	rval = avro_value_write(w->datum_writer, value);
	if (rval) {
		check(rval, file_write_block(w));
		rval = avro_value_write(w->datum_writer, value);
		if (rval) {
			avro_set_error("Value too large for file block size");
			/* TODO: if the value encoder larger than our buffer,
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
	avro_codec_reset(w->codec);
	avro_freet(struct avro_codec_t_, w->codec);
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
	      avro_read_data(r->block_reader, r->writers_schema, readers_schema,
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

int
avro_file_reader_read_value(avro_file_reader_t r, avro_value_t *value)
{
	int rval;
	char sync[16];

	check_param(EINVAL, r, "reader");
	check_param(EINVAL, value, "value");

	check(rval, avro_value_read(r->block_reader, value));
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
	avro_reader_free(reader->block_reader);
	avro_codec_reset(reader->codec);
	avro_freet(struct avro_codec_t_, reader->codec);
	if (reader->current_blockdata) {
		avro_free(reader->current_blockdata, reader->current_blocklen);
	}
	avro_freet(struct avro_file_reader_t_, reader);
	return 0;
}
