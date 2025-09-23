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

#include "ZstdCompressWrapper.hh"
#include "Exception.hh"

#include <zstd.h>

namespace avro {

std::vector<char> ZstdCompressWrapper::compress(const std::vector<char> &uncompressed) {
    // Pre-allocate buffer for compressed data
    size_t max_compressed_size = ZSTD_compressBound(uncompressed.size());
    if (ZSTD_isError(max_compressed_size)) {
        throw Exception("ZSTD compression error: {}", ZSTD_getErrorName(max_compressed_size));
    }
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

ZstdCompressWrapper::ZstdCompressWrapper() {
    cctx_ = ZSTD_createCCtx();
    if (!cctx_) {
        throw Exception("ZSTD_createCCtx() failed");
    }
}

ZstdCompressWrapper::~ZstdCompressWrapper() {
    ZSTD_freeCCtx(cctx_);
}

} // namespace avro

#endif // ZSTD_CODEC_AVAILABLE
