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

#include <zlib.h>
#include <string.h>
#include "avro/errors.h"
#include "avro/allocation.h"
#include "codec.h"

static int
codec_deflate(avro_codec_t codec)
{
	codec->name = "deflate";
	codec->type = AVRO_CODEC_DEFLATE;
	codec->block_size = 0;
	codec->block_data = NULL;
	codec->codec_data = avro_new(z_stream);

	if (!codec->codec_data) {
		avro_set_error("Cannot allocate memory for zlib");
		return 1;
	}

	z_stream *s = (z_stream *)codec->codec_data;

	s->zalloc = Z_NULL;
	s->zfree = Z_NULL;
	s->opaque = Z_NULL;

	if (deflateInit2(s, Z_BEST_COMPRESSION, Z_DEFLATED, -15, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
		avro_freet(z_stream, codec->codec_data);
		avro_set_error("Cannot initialize zlib");
		return 1;
	}

	return 0;
}

static int
codec_null(avro_codec_t codec)
{
	codec->name = "null";
	codec->type = AVRO_CODEC_NULL;
	codec->block_size = 0;
	codec->block_data = NULL;
	codec->codec_data = NULL;

	return codec;
}

int avro_codec(avro_codec_t codec, const char *type)
{
	if (strcmp("deflate", type) == 0) {
		return codec_deflate(codec);
	} else {
		return codec_null(codec);
	}

	return 0;
}

static int encode_deflate(avro_codec_t c, void * data, int64_t len)
{
	size_t defl_len;
	int err;

	defl_len = compressBound((uLong)len * 1.2);

	if (!c->block_data) {
		c->block_data = avro_malloc(defl_len);
	} else {
		c->block_data = avro_realloc(c->block_data, c->block_size, defl_len);
	}

	if (!c->block_data)
	{
		avro_set_error("Cannot allocate memory for deflate");
		return 1;
	}

	c->block_size = defl_len;

	z_stream *s = (z_stream *)c->codec_data;

	s->next_in = (Bytef*)data;
	s->avail_in = (uInt)len;

	s->next_out = c->block_data;
	s->avail_out = (uInt)c->block_size;

	s->total_out = 0;

	err = deflate(s, Z_FINISH);
	if (err != Z_STREAM_END) {
		deflateEnd(s);
		return err == Z_OK ? 1 : 0;
	}

	c->block_size = s->total_out;

	if (deflateReset(s) != Z_OK) {
		return 1;
	}

	return 0;
}

static int encode_null(avro_codec_t c, void * data, int64_t len)
{
	c->block_data = data;
	c->block_size = len;

	return 0;
}

int avro_codec_encode(avro_codec_t c, void * data, int64_t len)
{
	if (c->type == AVRO_CODEC_NULL) {
		return encode_null(c, data, len);
	} else if (c->type == AVRO_CODEC_DEFLATE) {
		return encode_deflate(c, data, len);
	}

	return 0;
}

static int reset_deflate(avro_codec_t c)
{
	if (c->block_data) {
		avro_free(c->block_data, c->block_size);
	}
	if (c->codec_data) {
		deflateEnd((z_stream *)c->codec_data);
		avro_freet(z_stream, c->codec_data);
	}

	c->block_data = NULL;
	c->block_size = 0;
	c->codec_data = NULL;

	return 0;
}

static int reset_null(avro_codec_t c)
{
	c->block_data = NULL;
	c->block_size = 0;
	c->codec_data = NULL;

	return 0;
}

int avro_codec_reset(avro_codec_t c)
{
	if (c->type == AVRO_CODEC_NULL) {
		return reset_null(c);
	} else if (c->type == AVRO_CODEC_DEFLATE) {
		return reset_deflate(c);
	}

	return 0;
}