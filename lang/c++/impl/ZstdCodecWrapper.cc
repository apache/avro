/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifdef ZSTD_CODEC_AVAILABLE

#include "ZstdCodecWrapper.hh"
#include "Exception.hh"

#include <zstd.h>

namespace avro {

std::vector<char> ZstdCodecWrapper::compress(const std::vector<char> &uncompressed) {
    if (cctx_ == nullptr) {
        initCCtx();
    }
    // Pre-allocate buffer for compressed data
    size_t max_compressed_size = ZSTD_compressBound(uncompressed.size());
    std::vector<char> compressed(max_compressed_size);

    // Compress the data using ZSTD block API
    size_t compressed_size = ZSTD_compress(
        compressed.data(), max_compressed_size,
        uncompressed.data(), uncompressed.size(),
        ZSTD_CLEVEL_DEFAULT);

    if (ZSTD_isError(compressed_size)) {
        throw Exception("ZSTD compression error: {}", ZSTD_getErrorName(compressed_size));
    }
    compressed.resize(compressed_size);
    return compressed;
}

std::string ZstdCodecWrapper::decompress(const std::vector<char> &compressed) {
    if (dctx_ == nullptr) {
        initDCtx();
    }
    std::string uncompressed;
    // Get the decompressed size
    size_t decompressed_size = ZSTD_getFrameContentSize(
        reinterpret_cast<const char *>(compressed.data()), compressed.size());
    if (decompressed_size == ZSTD_CONTENTSIZE_ERROR) {
        throw Exception("ZSTD: Not a valid compressed frame");
    } else if (decompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
        // Stream decompress the data
        ZSTD_inBuffer in{compressed.data(), compressed.size(), 0};
        std::vector<char> tmp(ZSTD_DStreamOutSize());
        ZSTD_outBuffer out{tmp.data(), tmp.size(), 0};
        size_t ret;
        do {
            out.pos = 0;
            ret = ZSTD_decompressStream(dctx_, &out, &in);
            if (ZSTD_isError(ret)) {
                throw Exception("ZSTD decompression error: {}", ZSTD_getErrorName(ret));
            }
            uncompressed.append(tmp.data(), out.pos);
        } while (ret != 0);
    } else {
        // Batch decompress the data
        uncompressed.resize(decompressed_size);
        size_t result = ZSTD_decompress(
            uncompressed.data(), decompressed_size,
            reinterpret_cast<const char *>(compressed.data()), compressed.size());

        if (ZSTD_isError(result)) {
            throw Exception("ZSTD decompression error: {}", ZSTD_getErrorName(result));
        }
        if (result != decompressed_size) {
            throw Exception("ZSTD: Decompressed size mismatch: expected {}, got {}",
                            decompressed_size, result);
        }
    }
    return uncompressed;
}

void ZstdCodecWrapper::initCCtx() {
    cctx_ = ZSTD_createCCtx();
    if (!cctx_) {
        throw Exception("ZSTD_createCCtx() failed");
    }
}

void ZstdCodecWrapper::initDCtx() {
    dctx_ = ZSTD_createDCtx();
    if (!dctx_) {
        throw Exception("ZSTD_createDCtx() failed");
    }
}

ZstdCodecWrapper::~ZstdCodecWrapper() {
    ZSTD_freeCCtx(cctx_);
    ZSTD_freeDCtx(dctx_);
}

} // namespace avro

#endif // ZSTD_CODEC_AVAILABLE
