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
 * AVRO-4284: a data-file block is decompressed according to the file's codec.
 * A block with a very high compression ratio (or a malformed block) can expand
 * to far more memory than its compressed size. Decompression must reject a
 * block whose decompressed size would exceed the configured maximum, which
 * these tests set to a small value via AVRO_MAX_DECOMPRESS_LENGTH.
 */

#include <avro.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "codec.h"

/* Size of the highly compressible payload used to build an over-large block. */
#define PAYLOAD_SIZE  (4 * 1024 * 1024)  /* 4 MiB of zeros */
/* Decompression limit for the test, smaller than the payload. */
#define TEST_LIMIT    "1048576"          /* 1 MiB */

/*
 * Compress `payload` with the named codec and attempt to decompress it with a
 * decompression limit smaller than the payload. Returns 0 if the codec is not
 * available (test skipped for that codec), 1 on test failure, 2 on success.
 */
static int
check_codec_rejects_oversized(const char *name, const char *payload, int64_t payload_len)
{
	struct avro_codec_t_ codec;
	memset(&codec, 0, sizeof(codec));

	if (avro_codec(&codec, name) != 0) {
		fprintf(stderr, "  codec %s not available, skipping\n", name);
		return 0;
	}

	if (avro_codec_encode(&codec, (void *) payload, payload_len) != 0) {
		fprintf(stderr, "  codec %s: encode failed: %s\n", name, avro_strerror());
		avro_codec_reset(&codec);
		return 1;
	}

	/* Copy the compressed bytes; decode reuses the codec's block buffer. */
	int64_t compressed_len = codec.used_size;
	if (compressed_len <= 0) {
		fprintf(stderr, "  codec %s: unexpected compressed length %lld\n",
			name, (long long) compressed_len);
		avro_codec_reset(&codec);
		return 1;
	}
	char *compressed = (char *) malloc((size_t) compressed_len);
	if (compressed == NULL) {
		avro_codec_reset(&codec);
		return 1;
	}
	memcpy(compressed, codec.block_data, (size_t) compressed_len);

	int rc = avro_codec_decode(&codec, compressed, compressed_len);
	/* Capture the error before reset/free may clobber it. */
	char err_copy[512];
	err_copy[0] = '\0';
	if (rc != 0) {
		strncpy(err_copy, avro_strerror(), sizeof(err_copy) - 1);
		err_copy[sizeof(err_copy) - 1] = '\0';
	}

	free(compressed);
	avro_codec_reset(&codec);

	if (rc == 0) {
		fprintf(stderr, "  codec %s: expected decompression to be rejected but it succeeded\n", name);
		return 1;
	}
	/* Ensure it was rejected specifically for exceeding the size limit, not
	 * some unrelated failure (e.g. a CRC error or regression). */
	if (strstr(err_copy, "exceeds the maximum") == NULL) {
		fprintf(stderr, "  codec %s: rejected but not for the size limit: %s\n", name, err_copy);
		return 1;
	}
	fprintf(stderr, "  codec %s: over-limit block rejected as expected\n", name);
	return 2;
}

int main(void)
{
#ifdef _WIN32
	_putenv_s("AVRO_MAX_DECOMPRESS_LENGTH", TEST_LIMIT);
#else
	setenv("AVRO_MAX_DECOMPRESS_LENGTH", TEST_LIMIT, 1);
#endif

	char *payload = (char *) calloc(1, PAYLOAD_SIZE);
	if (payload == NULL) {
		fprintf(stderr, "Cannot allocate test payload\n");
		exit(EXIT_FAILURE);
	}

	int checked = 0;
	const char *codecs[] = { "deflate", "snappy", "lzma" };
	size_t i;
	for (i = 0; i < sizeof(codecs) / sizeof(codecs[0]); i++) {
		int r = check_codec_rejects_oversized(codecs[i], payload, PAYLOAD_SIZE);
		if (r == 1) {
			free(payload);
			exit(EXIT_FAILURE);
		}
		if (r == 2) {
			checked++;
		}
	}

	free(payload);

	if (checked == 0) {
		fprintf(stderr, "No compression codecs available; nothing exercised\n");
		/* Not a failure: the build may have been configured without codecs. */
	}

	exit(EXIT_SUCCESS);
}
