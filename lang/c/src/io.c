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
#include <stdint.h>
#include <errno.h>
#include <string.h>
#include "avro.h"
#include "container_of.h"
#include "dump.h"

enum avro_io_type_t {
	AVRO_FILE_IO,
	AVRO_MEMORY_IO
};
typedef enum avro_io_type_t avro_io_type_t;

struct avro_reader_t {
	avro_io_type_t type;
	unsigned long refcount;
};

struct avro_writer_t {
	avro_io_type_t type;
	unsigned long refcount;
};

struct avro_file_reader_t {
	struct avro_reader_t reader;
	FILE *fp;
};

struct avro_file_writer_t {
	struct avro_writer_t writer;
	FILE *fp;
};

struct avro_memory_reader_t {
	struct avro_reader_t reader;
	const char *buf;
	int64_t len;
	int64_t read;
};

struct avro_memory_writer_t {
	struct avro_writer_t writer;
	const char *buf;
	int64_t len;
	int64_t written;
};

#define avro_io_typeof(obj)      ((obj)->type)
#define is_memory_io(obj)        (obj && avro_io_typeof(obj) == AVRO_MEMORY_IO)
#define is_file_io(obj)          (obj && avro_io_typeof(obj) == AVRO_FILE_IO)

#define avro_reader_to_memory(reader_)  container_of(reader_, struct avro_memory_reader_t, reader)
#define avro_reader_to_file(reader_)    container_of(reader_, struct avro_file_reader_t, reader)
#define avro_writer_to_memory(writer_)  container_of(writer_, struct avro_memory_writer_t, writer)
#define avro_writer_to_file(writer_)    container_of(writer_, struct avro_file_writer_t, writer)

static void reader_init(avro_reader_t reader, avro_io_type_t type)
{
	reader->type = type;
	reader->refcount = 1;
}

static void writer_init(avro_writer_t writer, avro_io_type_t type)
{
	writer->type = type;
	writer->refcount = 1;
}

avro_reader_t avro_reader_file(FILE * fp)
{
	struct avro_file_reader_t *file_reader =
	    malloc(sizeof(struct avro_file_reader_t));
	if (!file_reader) {
		return NULL;
	}
	file_reader->fp = fp;
	reader_init(&file_reader->reader, AVRO_FILE_IO);
	return &file_reader->reader;
}

avro_writer_t avro_writer_file(FILE * fp)
{
	struct avro_file_writer_t *file_writer =
	    malloc(sizeof(struct avro_file_writer_t));
	if (!file_writer) {
		return NULL;
	}
	file_writer->fp = fp;
	writer_init(&file_writer->writer, AVRO_FILE_IO);
	return &file_writer->writer;
}

avro_reader_t avro_reader_memory(const char *buf, int64_t len)
{
	struct avro_memory_reader_t *mem_reader =
	    malloc(sizeof(struct avro_memory_reader_t));
	if (!mem_reader) {
		return NULL;
	}
	mem_reader->buf = buf;
	mem_reader->len = len;
	mem_reader->read = 0;
	reader_init(&mem_reader->reader, AVRO_MEMORY_IO);
	return &mem_reader->reader;
}

avro_writer_t avro_writer_memory(const char *buf, int64_t len)
{
	struct avro_memory_writer_t *mem_writer =
	    malloc(sizeof(struct avro_memory_writer_t));
	if (!mem_writer) {
		return NULL;
	}
	mem_writer->buf = buf;
	mem_writer->len = len;
	mem_writer->written = 0;
	writer_init(&mem_writer->writer, AVRO_MEMORY_IO);
	return &mem_writer->writer;
}

static int
avro_read_memory(struct avro_memory_reader_t *reader, void *buf, int64_t len)
{
	if (len) {
		if ((reader->len - reader->read) < len) {
			return ENOSPC;
		}
		memcpy(buf, reader->buf + reader->read, len);
		reader->read += len;
	}
	return 0;
}

static int
avro_read_file(struct avro_file_reader_t *reader, void *buf, int64_t len)
{
	int rval = fread(buf, len, 1, reader->fp);

	if (rval == 0) {
		return ferror(reader->fp) ? -1 : 0;
	}
	return 0;
}

int avro_read(avro_reader_t reader, void *buf, int64_t len)
{
	if (buf && len >= 0) {
		if (is_memory_io(reader)) {
			return avro_read_memory(avro_reader_to_memory(reader),
						buf, len);
		} else if (is_file_io(reader)) {
			return avro_read_file(avro_reader_to_file(reader), buf,
					      len);
		}
	}
	return EINVAL;
}

static int
avro_write_memory(struct avro_memory_writer_t *writer, void *buf, int64_t len)
{
	if (len) {
		if ((writer->len - writer->written) < len) {
			return ENOSPC;
		}
		memcpy((void *)(writer->buf + writer->written), buf, len);
		writer->written += len;
	}
	return 0;
}

static int
avro_write_file(struct avro_file_writer_t *writer, void *buf, int64_t len)
{
	int rval;
	if (len > 0) {
		rval = fwrite(buf, len, 1, writer->fp);
		if (rval == 0) {
			return feof(writer->fp) ? -1 : 0;
		}
	}
	return 0;
}

int avro_write(avro_writer_t writer, void *buf, int64_t len)
{
	if (buf && len >= 0) {
		if (is_memory_io(writer)) {
			return avro_write_memory(avro_writer_to_memory(writer),
						 buf, len);
		} else if (is_memory_io(writer)) {
			return avro_write_file(avro_writer_to_file(writer), buf,
					       len);
		}
	}
	return EINVAL;
}

int avro_skip(avro_reader_t reader, int64_t len)
{
	/*
	 * TODO 
	 */
	return -1;
}

void avro_writer_dump(avro_writer_t writer, FILE * fp)
{
	if (is_memory_io(writer)) {
		dump(fp, (char *)avro_writer_to_memory(writer)->buf,
		     avro_writer_to_memory(writer)->written);
	}
}

void avro_reader_dump(avro_reader_t reader, FILE * fp)
{
	if (is_memory_io(reader)) {
		dump(fp, (char *)avro_reader_to_memory(reader)->buf,
		     avro_reader_to_memory(reader)->read);
	}
}

void avro_reader_free(avro_reader_t reader)
{
	if (is_memory_io(reader)) {
		free(avro_reader_to_memory(reader));
	} else if (is_file_io(reader)) {
		free(avro_reader_to_file(reader));
	}
}

void avro_writer_free(avro_writer_t writer)
{
	if (is_memory_io(writer)) {
		free(avro_writer_to_memory(writer));
	} else if (is_file_io(writer)) {
		free(avro_writer_to_file(writer));
	}
}
