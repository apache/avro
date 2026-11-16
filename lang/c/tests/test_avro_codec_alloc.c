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

/*
 * A small compressed block that inflates past what the allocator will
 * hand out must make avro_codec_decode fail cleanly, not write through
 * a NULL block buffer or leak the old one.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "avro.h"
#include "avro_private.h"
#include "codec.h"

#define ALLOC_CAP	(1024 * 1024)
#define RAW_SIZE	(4 * 1024 * 1024)

static int  cap_enabled = 0;

static void *
capped_allocator(void *ud, void *ptr, size_t osize, size_t nsize)
{
	AVRO_UNUSED(ud);
	AVRO_UNUSED(osize);

	if (nsize == 0) {
		free(ptr);
		return NULL;
	}
	if (cap_enabled && nsize > ALLOC_CAP) {
		return NULL;
	}
	return realloc(ptr, nsize);
}

static int
test_codec(const char *name)
{
	struct avro_codec_t_  encoder;
	struct avro_codec_t_  decoder;
	char  *raw;
	char  *compressed;
	int64_t  compressed_len;
	int  rval;

	raw = (char *) calloc(1, RAW_SIZE);
	if (!raw) {
		fprintf(stderr, "Cannot allocate test buffer\n");
		return 1;
	}

	if (avro_codec(&encoder, name) ||
	    avro_codec_encode(&encoder, raw, RAW_SIZE)) {
		fprintf(stderr, "Cannot encode %s block: %s\n",
			name, avro_strerror());
		free(raw);
		return 1;
	}
	free(raw);

	compressed_len = encoder.used_size;
	compressed = (char *) malloc(compressed_len);
	memcpy(compressed, encoder.block_data, compressed_len);
	avro_codec_reset(&encoder);

	if (avro_codec(&decoder, name)) {
		fprintf(stderr, "Cannot create %s codec: %s\n",
			name, avro_strerror());
		free(compressed);
		return 1;
	}

	cap_enabled = 1;
	rval = avro_codec_decode(&decoder, compressed, compressed_len);
	cap_enabled = 0;

	avro_codec_reset(&decoder);
	free(compressed);

	if (rval == 0) {
		fprintf(stderr, "%s: decoding past the allocator limit "
			"should have failed\n", name);
		return 1;
	}

	fprintf(stderr, "%s: %s\n", name, avro_strerror());
	return 0;
}

int main(void)
{
	int  failed = 0;

	avro_set_allocator(capped_allocator, NULL);

#ifdef DEFLATE_CODEC
	failed |= test_codec("deflate");
#endif
#ifdef LZMA_CODEC
	failed |= test_codec("lzma");
#endif

	return failed ? EXIT_FAILURE : EXIT_SUCCESS;
}
