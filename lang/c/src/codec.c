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

#include <string.h>
#include <stdlib.h>
#ifdef SNAPPY_CODEC
#include <snappy-c.h>
#  if defined(__APPLE__)
#    include <libkern/OSByteOrder.h>
#    define __bswap_32 OSSwapInt32
#  elif defined(__FreeBSD__)
#    include <sys/endian.h>
#    define __bswap_32 bswap32
#  elif defined(_WIN32)
#    include <stdlib.h>
#    define __bswap_32 _byteswap_ulong
#  elif defined(__ANDROID__)
#    include <byteswap.h>
#    define __bswap_32 bswap_32
#  else
#    include <byteswap.h>
#  endif
#endif
#ifdef DEFLATE_CODEC
#include <zlib.h>
#endif
#ifdef LZMA_CODEC
#include <lzma.h>
#endif
#include "avro/errors.h"
#include "avro/allocation.h"
#include "codec.h"
#include <errno.h>
#include <limits.h>
#include <stdint.h>

#define DEFAULT_BLOCK_SIZE	(16 * 1024)

/*
 * When reading a data file, each block is decompressed according to the file's
 * codec. A block with a very high compression ratio (or a malformed block) can
 * expand to far more memory than its compressed size. To guard against
 * unbounded allocation, the decompressed size of a single block is capped. This
 * mirrors the Java SDK's decompression limit (AVRO-4247). The default can be
 * overridden with the AVRO_MAX_DECOMPRESS_LENGTH environment variable.
 */
#define AVRO_DEFAULT_MAX_DECOMPRESS_LENGTH	((int64_t) 200 * 1024 * 1024)  /* 200 MiB */

static int64_t
avro_max_decompress_length(void)
{
	const char *env = getenv("AVRO_MAX_DECOMPRESS_LENGTH");
	if (env != NULL && *env != '\0') {
		char *end = NULL;
		errno = 0;
		long long value = strtoll(env, &end, 10);
		if (errno == 0 && end != NULL && *end == '\0' && value > 0) {
			int64_t v = (int64_t) value;
			/* Clamp to what size_t can address so the value is always
			 * safe to use as an allocation cap. On 32-bit platforms
			 * size_t is narrower than int64_t, and an unclamped value
			 * would truncate when passed to avro_malloc/avro_realloc. */
			if ((uint64_t) v > (uint64_t) SIZE_MAX) {
				return (int64_t) SIZE_MAX;
			}
			return v;
		}
	}
	return AVRO_DEFAULT_MAX_DECOMPRESS_LENGTH;
}


/* NULL codec */

static int
codec_null(avro_codec_t codec)
{
	codec->name = "null";
	codec->type = AVRO_CODEC_NULL;
	codec->block_size = 0;
	codec->used_size = 0;
	codec->block_data = NULL;
	codec->codec_data = NULL;

	return 0;
}

static int encode_null(avro_codec_t c, void * data, int64_t len)
{
	c->block_data = data;
	c->block_size = len;
	c->used_size = len;

	return 0;
}

static int decode_null(avro_codec_t c, void * data, int64_t len)
{
	c->block_data = data;
	c->block_size = len;
	c->used_size = len;

	return 0;
}

static int reset_null(avro_codec_t c)
{
	c->block_data = NULL;
	c->block_size = 0;
	c->used_size = 0;
	c->codec_data = NULL;

	return 0;
}

/* Snappy codec */

#ifdef SNAPPY_CODEC

static int
codec_snappy(avro_codec_t codec)
{
	codec->name = "snappy";
	codec->type = AVRO_CODEC_SNAPPY;
	codec->block_size = 0;
	codec->used_size = 0;
	codec->block_data = NULL;
	codec->codec_data = NULL;

	return 0;
}

static int encode_snappy(avro_codec_t c, void * data, int64_t len)
{
        uint32_t crc;
        size_t outlen = snappy_max_compressed_length(len);

	if (!c->block_data) {
		c->block_data = avro_malloc(outlen+4);
		c->block_size = outlen+4;
	} else if (c->block_size < (int64_t) (outlen+4)) {
            c->block_data = avro_realloc(c->block_data, c->block_size, (outlen+4));
		c->block_size = outlen+4;
	}

	if (!c->block_data) {
		avro_set_error("Cannot allocate memory for snappy");
		return 1;
	}

        if (snappy_compress((const char *)data, len, (char*)c->block_data, &outlen) != SNAPPY_OK)
        {
                avro_set_error("Error compressing block with Snappy");
		return 1;
	}

        crc = __bswap_32(crc32(0, (const Bytef *)data, len));
        memcpy((char*)c->block_data+outlen, &crc, 4);
        c->used_size = outlen+4;

	return 0;
}

static int decode_snappy(avro_codec_t c, void * data, int64_t len)
{
        uint32_t crc;
        size_t outlen;
        int64_t max_len = avro_max_decompress_length();

        /* The block carries a trailing 4-byte CRC; a length below that is
         * malformed and would underflow len - 4. */
        if (len < 4) {
                avro_set_error("Snappy block too small (%lld bytes), expected at least 4",
                               (long long) len);
                return 1;
        }

        if (snappy_uncompressed_length((const char*)data, len-4, &outlen) != SNAPPY_OK) {
		avro_set_error("Uncompressed length error in snappy");
		return 1;
        }

	/* Compare as unsigned 64-bit: casting outlen (size_t) to int64_t could
	 * wrap negative for a declared length above INT64_MAX and bypass the cap. */
	if ((uint64_t) outlen > (uint64_t) max_len) {
		avro_set_error("Decompressed block size %llu exceeds the maximum allowed of %lld bytes",
			       (unsigned long long) outlen, (long long) max_len);
		return 1;
	}

	if (!c->block_data) {
		c->block_data = avro_malloc(outlen);
		c->block_size = outlen;
	} else if ( (size_t)c->block_size < outlen) {
		c->block_data = avro_realloc(c->block_data, c->block_size, outlen);
		c->block_size = outlen;
	}

	if (!c->block_data)
	{
		avro_set_error("Cannot allocate memory for snappy");
		return 1;
	}

        if (snappy_uncompress((const char*)data, len-4, (char*)c->block_data, &outlen) != SNAPPY_OK)
        {
                avro_set_error("Error uncompressing block with Snappy");
		return 1;
	}

        crc = __bswap_32(crc32(0, (const Bytef *)c->block_data, outlen));
        if (memcmp(&crc, (char*)data+len-4, 4))
        {
                avro_set_error("CRC32 check failure uncompressing block with Snappy");
		return 1;
	}

        c->used_size = outlen;

	return 0;
}

static int reset_snappy(avro_codec_t c)
{
	if (c->block_data) {
		avro_free(c->block_data, c->block_size);
	}

	c->block_data = NULL;
	c->block_size = 0;
	c->used_size = 0;
	c->codec_data = NULL;

	return 0;
}

#endif // SNAPPY_CODEC

/* Deflate codec */

#ifdef DEFLATE_CODEC

struct codec_data_deflate {
	z_stream deflate;
	z_stream inflate;
};
#define codec_data_deflate_stream(cd)	&((struct codec_data_deflate *)cd)->deflate
#define codec_data_inflate_stream(cd)	&((struct codec_data_deflate *)cd)->inflate


static int
codec_deflate(avro_codec_t codec)
{
	codec->name = "deflate";
	codec->type = AVRO_CODEC_DEFLATE;
	codec->block_size = 0;
	codec->used_size = 0;
	codec->block_data = NULL;
	codec->codec_data = avro_new(struct codec_data_deflate);

	if (!codec->codec_data) {
		avro_set_error("Cannot allocate memory for zlib");
		return 1;
	}

	z_stream *ds = codec_data_deflate_stream(codec->codec_data);
	z_stream *is = codec_data_inflate_stream(codec->codec_data);

	memset(ds, 0, sizeof(z_stream));
	memset(is, 0, sizeof(z_stream));

	ds->zalloc = is->zalloc = Z_NULL;
	ds->zfree  = is->zfree  = Z_NULL;
	ds->opaque = is->opaque = Z_NULL;

	if (deflateInit2(ds, Z_BEST_COMPRESSION, Z_DEFLATED, -15, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
		avro_freet(struct codec_data_deflate, codec->codec_data);
		codec->codec_data = NULL;
		avro_set_error("Cannot initialize zlib deflate");
		return 1;
	}

	if (inflateInit2(is, -15) != Z_OK) {
		avro_freet(struct codec_data_deflate, codec->codec_data);
		codec->codec_data = NULL;
		avro_set_error("Cannot initialize zlib inflate");
		return 1;
	}

	return 0;
}

static int encode_deflate(avro_codec_t c, void * data, int64_t len)
{
	int err;
	int64_t defl_len = compressBound((uLong)len * 1.2);

	if (!c->block_data) {
		c->block_data = avro_malloc(defl_len);
		c->block_size = defl_len;
	} else if ( c->block_size < defl_len) {
		c->block_data = avro_realloc(c->block_data, c->block_size, defl_len);
		c->block_size = defl_len;
	}

	if (!c->block_data)
	{
		avro_set_error("Cannot allocate memory for deflate");
		return 1;
	}

	c->used_size = 0;

	z_stream *s = codec_data_deflate_stream(c->codec_data);

	s->next_in = (Bytef*)data;
	s->avail_in = (uInt)len;

	s->next_out = c->block_data;
	s->avail_out = (uInt)c->block_size;

	s->total_out = 0;

	err = deflate(s, Z_FINISH);
	if (err != Z_STREAM_END) {
		deflateEnd(s);
		if (err != Z_OK) {
			avro_set_error("Error compressing block with deflate (%i)", err);
			return 1;
		}
		return 0;
	}

	// zlib resizes the buffer?
	c->block_size = s->total_out;
	c->used_size = s->total_out;

	if (deflateReset(s) != Z_OK) {
		return 1;
	}

	return 0;
}

static int decode_deflate(avro_codec_t c, void * data, int64_t len)
{
	int err;
	int64_t max_len = avro_max_decompress_length();
	z_stream *s = codec_data_inflate_stream(c->codec_data);

	/* zlib's avail_out is a uInt; keep the working cap within its range so
	 * the buffer-growth arithmetic below cannot truncate when updating
	 * avail_out, which would leave zlib's state inconsistent. */
	if (max_len > (int64_t) UINT_MAX) {
		max_len = (int64_t) UINT_MAX;
	}

	if (!c->block_data) {
		/* Do not allocate more than the configured decompression cap: if
		 * max_len is smaller than DEFAULT_BLOCK_SIZE, start at max_len. */
		size_t init_size = ((int64_t) DEFAULT_BLOCK_SIZE > max_len)
		    ? (size_t) max_len : (size_t) DEFAULT_BLOCK_SIZE;
		if (init_size == 0) {
			init_size = 1;
		}
		c->block_data = avro_malloc(init_size);
		c->block_size = init_size;
	}

	if (!c->block_data)
	{
		avro_set_error("Cannot allocate memory for deflate");
		return 1;
	}

	c->used_size = 0;

	s->next_in = data;
	s->avail_in = len;

	s->next_out = c->block_data;
	s->avail_out = c->block_size;

	s->total_out = 0;

	do
	{
		err = inflate(s, Z_FINISH);

		// Reject a block that decompresses to more than the allowed maximum,
		// to guard against unbounded allocation from a high-ratio block.
		if ((int64_t) s->total_out > max_len) {
			inflateEnd(s);
			avro_set_error("Decompressed block size exceeds the maximum allowed of %lld bytes",
				       (long long) max_len);
			return 1;
		}

		// Apparently if there is yet available space in the output then something
		// has gone wrong in decompressing the data (according to cpython zlibmodule.c)
		if (err == Z_BUF_ERROR && s->avail_out > 0) {
			inflateEnd(s);
			avro_set_error("Error decompressing block with deflate, possible data error");
			return 1;
		}

		// The buffer was not big enough. resize it, without growing beyond
		// the configured maximum decompressed size.
		if (err == Z_BUF_ERROR)
		{
			// Double the buffer, guarding against signed overflow when
			// block_size is already large, and never growing beyond the
			// configured maximum decompressed size.
			int64_t new_size;
			if (c->block_size > max_len / 2) {
				new_size = max_len;
			} else {
				new_size = c->block_size * 2;
			}
			if (new_size <= c->block_size) {
				inflateEnd(s);
				avro_set_error("Decompressed block size exceeds the maximum allowed of %lld bytes",
					       (long long) max_len);
				return 1;
			}
			void *new_data = avro_realloc(c->block_data, c->block_size, new_size);
			if (new_data == NULL) {
				inflateEnd(s);
				avro_set_error("Cannot allocate memory for deflate");
				return 1;
			}
			c->block_data = new_data;
			s->next_out = (Bytef *) c->block_data + s->total_out;
			s->avail_out += (uInt)(new_size - c->block_size);
			c->block_size = new_size;
		}
	} while (err == Z_BUF_ERROR);

	if (err != Z_STREAM_END) {
		inflateEnd(s);
		if (err != Z_OK) {
			avro_set_error("Error decompressing block with deflate (%i)", err);
			return 1;
		}
		return 0;
	}

	c->used_size = s->total_out;

	if (inflateReset(s) != Z_OK) {
		avro_set_error("Error resetting deflate decompression");
		return 1;
	}

	return 0;
}

static int reset_deflate(avro_codec_t c)
{
	if (c->block_data) {
		avro_free(c->block_data, c->block_size);
	}
	if (c->codec_data) {
		deflateEnd(codec_data_deflate_stream(c->codec_data));
		inflateEnd(codec_data_inflate_stream(c->codec_data));
		avro_freet(struct codec_data_deflate, c->codec_data);
	}

	c->block_data = NULL;
	c->block_size = 0;
	c->used_size = 0;
	c->codec_data = NULL;

	return 0;
}

#endif // DEFLATE_CODEC

/* LZMA codec */

#ifdef LZMA_CODEC

struct codec_data_lzma {
	lzma_filter filters[2];
	lzma_options_lzma options;
};
#define codec_data_lzma_filters(cd)	((struct codec_data_lzma *)cd)->filters
#define codec_data_lzma_options(cd)	&((struct codec_data_lzma *)cd)->options

static int
codec_lzma(avro_codec_t codec)
{
	codec->name = "lzma";
	codec->type = AVRO_CODEC_LZMA;
	codec->block_size = 0;
	codec->used_size = 0;
	codec->block_data = NULL;
	codec->codec_data = avro_new(struct codec_data_lzma);

	if (!codec->codec_data) {
		avro_set_error("Cannot allocate memory for lzma");
		return 1;
	}

	lzma_options_lzma* opt = codec_data_lzma_options(codec->codec_data);
	lzma_lzma_preset(opt, LZMA_PRESET_DEFAULT);

	lzma_filter* filters = codec_data_lzma_filters(codec->codec_data);
	filters[0].id = LZMA_FILTER_LZMA2;
	filters[0].options = opt;
	filters[1].id = LZMA_VLI_UNKNOWN;
	filters[1].options = NULL;

	return 0;
}

static int encode_lzma(avro_codec_t codec, void * data, int64_t len)
{
	lzma_ret ret;
	size_t written = 0;
	lzma_filter* filters = codec_data_lzma_filters(codec->codec_data);

	int64_t buff_len = len + lzma_raw_encoder_memusage(filters);

	if (!codec->block_data) {
		codec->block_data = avro_malloc(buff_len);
		codec->block_size = buff_len;
	}

	if (!codec->block_data)
	{
		avro_set_error("Cannot allocate memory for lzma encoder");
		return 1;
	}

	ret = lzma_raw_buffer_encode(filters, NULL, data, len, codec->block_data, &written, codec->block_size);

	codec->used_size = written;

	if (ret != LZMA_OK) {
		avro_set_error("Error in lzma encoder");
		return 1;
	}

	return 0;
}

static int decode_lzma(avro_codec_t codec, void * data, int64_t len)
{
	size_t read_pos = 0;
	size_t write_pos = 0;
	int64_t max_len = avro_max_decompress_length();
	lzma_ret ret;
	lzma_filter* filters = codec_data_lzma_filters(codec->codec_data);

	if (!codec->block_data) {
		/* Do not allocate more than the configured decompression cap: if
		 * max_len is smaller than DEFAULT_BLOCK_SIZE, start at max_len. */
		size_t init_size = ((int64_t) DEFAULT_BLOCK_SIZE > max_len)
		    ? (size_t) max_len : (size_t) DEFAULT_BLOCK_SIZE;
		if (init_size == 0) {
			init_size = 1;
		}
		codec->block_data = avro_malloc(init_size);
		codec->block_size = init_size;
	}

	if (!codec->block_data) {
		avro_set_error("Cannot allocate memory for lzma decoder");
		return 1;
	}

	do
	{
		ret = lzma_raw_buffer_decode(filters, NULL, data,
			&read_pos, len, codec->block_data, &write_pos,
			codec->block_size);

		codec->used_size = write_pos;

		// Reject a block that decompresses to more than the allowed maximum,
		// to guard against unbounded allocation from a high-ratio block.
		if ((int64_t) write_pos > max_len) {
			avro_set_error("Decompressed block size exceeds the maximum allowed of %lld bytes",
				       (long long) max_len);
			return 1;
		}

		// If it ran out of space to decode, give it more (without growing
		// beyond the configured maximum decompressed size).
		if (ret == LZMA_BUF_ERROR) {
			// Double the buffer, guarding against signed overflow when
			// block_size is already large, and never growing beyond the
			// configured maximum decompressed size.
			int64_t new_size;
			if (codec->block_size > max_len / 2) {
				new_size = max_len;
			} else {
				new_size = codec->block_size * 2;
			}
			if (new_size <= codec->block_size) {
				avro_set_error("Decompressed block size exceeds the maximum allowed of %lld bytes",
					       (long long) max_len);
				return 1;
			}
			void *new_data = avro_realloc(codec->block_data, codec->block_size, new_size);
			if (new_data == NULL) {
				avro_set_error("Cannot allocate memory for lzma decoder");
				return 1;
			}
			codec->block_data = new_data;
			codec->block_size = new_size;
		}

	} while (ret == LZMA_BUF_ERROR);

	if (ret != LZMA_OK) {
		avro_set_error("Error in lzma decoder");
		return 1;
	}

	return 0;
}

static int reset_lzma(avro_codec_t c)
{
	if (c->block_data) {
		avro_free(c->block_data, c->block_size);
	}
	if (c->codec_data) {
		avro_freet(struct codec_data_lzma, c->codec_data);
	}

	c->block_data = NULL;
	c->block_size = 0;
	c->used_size = 0;
	c->codec_data = NULL;

	return 0;
}

#endif // LZMA_CODEC

/* Common interface */

int avro_codec(avro_codec_t codec, const char *type)
{
	if (type == NULL) {
		return codec_null(codec);
	}

#ifdef SNAPPY_CODEC
	if (strcmp("snappy", type) == 0) {
		return codec_snappy(codec);
	}
#endif

#ifdef DEFLATE_CODEC
	if (strcmp("deflate", type) == 0) {
		return codec_deflate(codec);
	}
#endif

#ifdef LZMA_CODEC
	if (strcmp("lzma", type) == 0) {
		return codec_lzma(codec);
	}
#endif

	if (strcmp("null", type) == 0) {
		return codec_null(codec);
	}

	avro_set_error("Unknown codec %s", type);
	return 1;
}

int avro_codec_encode(avro_codec_t c, void * data, int64_t len)
{
	switch(c->type)
	{
	case AVRO_CODEC_NULL:
		return encode_null(c, data, len);
#ifdef SNAPPY_CODEC
	case AVRO_CODEC_SNAPPY:
		return encode_snappy(c, data, len);
#endif
#ifdef DEFLATE_CODEC
	case AVRO_CODEC_DEFLATE:
		return encode_deflate(c, data, len);
#endif
#ifdef LZMA_CODEC
	case AVRO_CODEC_LZMA:
		return encode_lzma(c, data, len);
#endif
	default:
		return 1;
	}
}

int avro_codec_decode(avro_codec_t c, void * data, int64_t len)
{
	switch(c->type)
	{
	case AVRO_CODEC_NULL:
		return decode_null(c, data, len);
#ifdef SNAPPY_CODEC
	case AVRO_CODEC_SNAPPY:
		return decode_snappy(c, data, len);
#endif
#ifdef DEFLATE_CODEC
	case AVRO_CODEC_DEFLATE:
		return decode_deflate(c, data, len);
#endif
#ifdef LZMA_CODEC
	case AVRO_CODEC_LZMA:
		return decode_lzma(c, data, len);
#endif
	default:
		return 1;
	}
}

int avro_codec_reset(avro_codec_t c)
{
	switch(c->type)
	{
	case AVRO_CODEC_NULL:
		return reset_null(c);
#ifdef SNAPPY_CODEC
	case AVRO_CODEC_SNAPPY:
		return reset_snappy(c);
#endif
#ifdef DEFLATE_CODEC
	case AVRO_CODEC_DEFLATE:
		return reset_deflate(c);
#endif
#ifdef LZMA_CODEC
	case AVRO_CODEC_LZMA:
		return reset_lzma(c);
#endif
	default:
		return 1;
	}
}
