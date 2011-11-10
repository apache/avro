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
avro_codec_deflate(avro_codec_t codec)
{
	codec->name = "deflate";
	codec->type = AVRO_CODEC_DEFLATE;
	codec->block_size = 0;
	codec->block_data = NULL;

	return 0;
}

int avro_codec(avro_codec_t codec, const char *type)
{
	if (strcmp("deflate", type) == 0) {
		return avro_codec_deflate(codec);
	} else {
		codec->name = "null";
		codec->type = AVRO_CODEC_NULL;
		codec->block_size = 0;
		codec->block_data = NULL;
	}

	return 0;
}

static int encode_deflate(avro_codec_t c, void * data, int64_t len)
{
	size_t defl_len;

	defl_len = compressBound(len);

	if (!c->block_data) {
		c->block_data = avro_new(defl_len);
	} else {
		c->block_data = avro_realloc(c->block_data, c->block_size, defl_len);
	}

	if (!c->block_data)
	{
		avro_set_error("Cannot allocate memory for deflate");
		return 1;
	}

	int ret = compress(c->block_data, &c->block_size, data, len);

	if (!ret) {
		return 0;
	}

	return 1;
}

int avro_codec_encode(avro_codec_t c, void * data, int64_t len)
{
	if (c->type == AVRO_CODEC_NULL) {
		c->block_data = data;
		c->block_size = len;
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

	c->block_data = NULL;
	c->block_size = 0;

	return 0;
}

int avro_codec_reset(avro_codec_t c)
{
	if (c->type == AVRO_CODEC_DEFLATE) {
		return reset_deflate(c);
	} else {
		c->block_data = NULL;
		c->block_size = 0;
	}

	return 0;
}