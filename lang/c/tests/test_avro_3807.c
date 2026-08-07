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
#include "codec.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * A snappy block carries a four byte CRC32 trailer, so anything shorter
 * than that cannot be a valid block. The length comes straight from the
 * container file and decode_snappy used to compute len - 4 before checking
 * it, which underflows for blocks of 0 to 3 bytes and reads out of bounds.
 * Make sure those undersized blocks are rejected rather than decoded.
 */

int main(void)
{
#ifdef SNAPPY_CODEC
	int i;

	avro_codec_t codec = (avro_codec_t) avro_new(struct avro_codec_t_);
	if (codec == NULL) {
		fprintf(stderr, "Cannot allocate codec\n");
		return EXIT_FAILURE;
	}

	if (avro_codec(codec, "snappy") != 0) {
		fprintf(stderr, "Cannot create snappy codec: %s\n",
			avro_strerror());
		avro_freet(struct avro_codec_t_, codec);
		return EXIT_FAILURE;
	}

	/*
	 * Use an exact sized heap buffer for every length so a checker such as
	 * valgrind catches any read past it. The bytes have the varint
	 * continuation bit set so that, without the guard, the underflowed
	 * len-4 lets snappy keep reading the length prefix off the end of the
	 * block.
	 */
	for (i = 0; i < 4; i++) {
		size_t size = (i == 0) ? 1 : (size_t) i;
		char *buf = (char *) avro_malloc(size);
		if (buf == NULL) {
			avro_codec_reset(codec);
			avro_freet(struct avro_codec_t_, codec);
			return EXIT_FAILURE;
		}
		memset(buf, 0xff, size);

		if (avro_codec_decode(codec, buf, i) == 0) {
			fprintf(stderr,
				"snappy block of %d bytes should be rejected\n",
				i);
			avro_free(buf, size);
			avro_codec_reset(codec);
			avro_freet(struct avro_codec_t_, codec);
			return EXIT_FAILURE;
		}

		avro_free(buf, size);
	}

	avro_codec_reset(codec);
	avro_freet(struct avro_codec_t_, codec);
#else
	fprintf(stderr, "Snappy codec not available, skipping\n");
#endif
	return EXIT_SUCCESS;
}
