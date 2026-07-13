/**
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

#include "Decoder.hh"
#include "Exception.hh"
#include "Zigzag.hh"
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>

namespace avro {

using std::make_shared;

// Structural cap on the number of elements to skip in an array or map (an
// overflow / defense-in-depth guard). Mirrors the read-path limit in Generic.cc
// and matches the value used by the other Avro SDKs (Integer.MAX_VALUE - 8).
static constexpr int64_t kDefaultMaxCollectionStructural = 2147483639;

// Returns the structural collection cap. It can be overridden by the
// AVRO_MAX_COLLECTION_ITEMS environment variable (a non-negative integer),
// matching the read path and the other SDKs so a single knob configures all of
// them; an invalid or unset value uses the default.
static int64_t maxCollectionStructural() {
    const char *env = std::getenv("AVRO_MAX_COLLECTION_ITEMS");
    if (env != nullptr && *env != '\0') {
        char *end = nullptr;
        long long value = std::strtoll(env, &end, 10);
        if (*end == '\0' && value >= 0) {
            return static_cast<int64_t>(value);
        }
    }
    return kDefaultMaxCollectionStructural;
}

class BinaryDecoder : public Decoder {
    StreamReader in_;

    void init(InputStream &is) final;
    void decodeNull() final;
    bool decodeBool() final;
    int32_t decodeInt() final;
    int64_t decodeLong() final;
    float decodeFloat() final;
    double decodeDouble() final;
    void decodeString(std::string &value) final;
    void skipString() final;
    void decodeBytes(std::vector<uint8_t> &value) final;
    void skipBytes() final;
    void decodeFixed(size_t n, std::vector<uint8_t> &value) final;
    void skipFixed(size_t n) final;
    size_t decodeEnum() final;
    size_t arrayStart() final;
    size_t arrayNext() final;
    size_t skipArray() final;
    size_t mapStart() final;
    size_t mapNext() final;
    size_t skipMap() final;
    size_t decodeUnionIndex() final;

    int64_t doDecodeLong();
    size_t doDecodeItemCount();
    size_t doDecodeLength();
    void checkAvailableBytes(size_t len);
    void drain() final;

    int64_t bytesRemaining() const final {
        return in_.remainingBytes();
    }
};

DecoderPtr binaryDecoder() {
    return make_shared<BinaryDecoder>();
}

void BinaryDecoder::init(InputStream &is) {
    in_.reset(is);
}

void BinaryDecoder::decodeNull() {
}

bool BinaryDecoder::decodeBool() {
    auto v = in_.read();
    if (v == 0) {
        return false;
    } else if (v == 1) {
        return true;
    }
    throw Exception("Invalid value for bool: {}", v);
}

int32_t BinaryDecoder::decodeInt() {
    auto val = doDecodeLong();
    if (val < INT32_MIN || val > INT32_MAX) {
        throw Exception("Value out of range for Avro int: {}", val);
    }
    return static_cast<int32_t>(val);
}

int64_t BinaryDecoder::decodeLong() {
    return doDecodeLong();
}

float BinaryDecoder::decodeFloat() {
    float result;
    in_.readBytes(reinterpret_cast<uint8_t *>(&result), sizeof(float));
    return result;
}

double BinaryDecoder::decodeDouble() {
    double result;
    in_.readBytes(reinterpret_cast<uint8_t *>(&result), sizeof(double));
    return result;
}

size_t BinaryDecoder::doDecodeLength() {
    ssize_t len = decodeInt();
    if (len < 0) {
        throw Exception("Cannot have negative length: {}", len);
    }
    return len;
}

void BinaryDecoder::checkAvailableBytes(size_t len) {
    // Reject a declared length that exceeds the data actually available before
    // allocating for it, to guard against an out-of-memory attack from a
    // malicious or truncated input. Only enforced when the stream can report
    // how many bytes remain.
    int64_t remaining = in_.remainingBytes();
    if (remaining >= 0 && static_cast<uint64_t>(len) > static_cast<uint64_t>(remaining)) {
        throw Exception(
            "Length {} exceeds the {} bytes available in the stream", len, remaining);
    }
}

void BinaryDecoder::drain() {
    in_.drain(false);
}

void BinaryDecoder::decodeString(std::string &value) {
    size_t len = doDecodeLength();
    checkAvailableBytes(len);
    value.resize(len);
    if (len > 0) {
        in_.readBytes(const_cast<uint8_t *>(
                          reinterpret_cast<const uint8_t *>(value.c_str())),
                      len);
    }
}

void BinaryDecoder::skipString() {
    size_t len = doDecodeLength();
    checkAvailableBytes(len);
    in_.skipBytes(len);
}

void BinaryDecoder::decodeBytes(std::vector<uint8_t> &value) {
    size_t len = doDecodeLength();
    checkAvailableBytes(len);
    value.resize(len);
    if (len > 0) {
        in_.readBytes(value.data(), len);
    }
}

void BinaryDecoder::skipBytes() {
    size_t len = doDecodeLength();
    checkAvailableBytes(len);
    in_.skipBytes(len);
}

void BinaryDecoder::decodeFixed(size_t n, std::vector<uint8_t> &value) {
    value.resize(n);
    if (n > 0) {
        in_.readBytes(value.data(), n);
    }
}

void BinaryDecoder::skipFixed(size_t n) {
    in_.skipBytes(n);
}

size_t BinaryDecoder::decodeEnum() {
    return static_cast<size_t>(doDecodeLong());
}

size_t BinaryDecoder::arrayStart() {
    return doDecodeItemCount();
}

size_t BinaryDecoder::doDecodeItemCount() {
    auto result = doDecodeLong();
    if (result < 0) {
        // INT64_MIN cannot be negated in int64_t (it would overflow); reject it
        // rather than propagating 2^63 as an item count that inevitably fails a
        // huge allocation downstream.
        if (result == INT64_MIN) {
            throw Exception("Invalid negative block count: {}", result);
        }
        int64_t blockSize = doDecodeLong();
        if (blockSize < 0) {
            // The byte-size that follows a negative block count is a byte count
            // and must be non-negative; reject malformed input here too (as
            // skipArray already does) so arrayStart()/mapStart() fail fast.
            throw Exception("Invalid negative block byte-size: {}", blockSize);
        }
        result = -result;
    }
    // On builds where size_t is narrower than int64_t (e.g. 32-bit), reject a
    // count that would truncate on the cast -- otherwise a huge block could wrap
    // to a small one and bypass the downstream structural caps.
    if constexpr (sizeof(size_t) < sizeof(int64_t)) {
        if (static_cast<uint64_t>(result) > std::numeric_limits<size_t>::max()) {
            throw Exception("Block count {} exceeds the maximum supported size", result);
        }
    }
    return static_cast<size_t>(result);
}

size_t BinaryDecoder::arrayNext() {
    return doDecodeItemCount();
}

size_t BinaryDecoder::skipArray() {
    for (;;) {
        auto r = doDecodeLong();
        if (r < 0) {
            auto byteSize = doDecodeLong();
            if (byteSize < 0) {
                // A negative block byte-size would convert to a huge size_t and
                // drive an unbounded skip; reject it.
                throw Exception("Invalid negative block size: {}", byteSize);
            }
            // Reject a byte-size that would truncate on the cast (32-bit builds)
            // and one that exceeds the bytes remaining, so a truncated block is
            // not silently skipped past EOF by the memory-backed skip().
            if constexpr (sizeof(size_t) < sizeof(int64_t)) {
                if (static_cast<uint64_t>(byteSize) > std::numeric_limits<size_t>::max()) {
                    throw Exception("Block size {} exceeds the maximum supported size", byteSize);
                }
            }
            checkAvailableBytes(static_cast<size_t>(byteSize));
            in_.skipBytes(static_cast<size_t>(byteSize));
        } else {
            // Bound the block count: skipping a huge block of zero-byte elements
            // would otherwise loop unboundedly (a CPU exhaustion) even though it
            // reads/allocates nothing. The decoder has no element schema here, so
            // apply the structural cap (AVRO_MAX_COLLECTION_ITEMS, default
            // Integer.MAX_VALUE - 8). Read the limit each call so a runtime
            // change to the environment variable is honoured, matching Generic.cc.
            const int64_t structural = maxCollectionStructural();
            if (r > structural) {
                throw Exception(
                    "Cannot skip a collection of more than {} elements; "
                    "set AVRO_MAX_COLLECTION_ITEMS if this is legitimate",
                    structural);
            }
            // On builds where size_t is narrower than int64_t (e.g. 32-bit),
            // reject a count that would truncate on the cast, matching
            // doDecodeItemCount so a huge block can't wrap to a small one.
            if constexpr (sizeof(size_t) < sizeof(int64_t)) {
                if (static_cast<uint64_t>(r) > std::numeric_limits<size_t>::max()) {
                    throw Exception("Block count {} exceeds the maximum supported size", r);
                }
            }
            return static_cast<size_t>(r);
        }
    }
}

size_t BinaryDecoder::mapStart() {
    return doDecodeItemCount();
}

size_t BinaryDecoder::mapNext() {
    return doDecodeItemCount();
}

size_t BinaryDecoder::skipMap() {
    return skipArray();
}

size_t BinaryDecoder::decodeUnionIndex() {
    return static_cast<size_t>(doDecodeLong());
}

int64_t BinaryDecoder::doDecodeLong() {
    uint64_t encoded = 0;
    int shift = 0;
    uint8_t u;
    do {
        if (shift >= 64) {
            throw Exception("Invalid Avro varint");
        }
        u = in_.read();
        encoded |= static_cast<uint64_t>(u & 0x7f) << shift;
        shift += 7;
    } while (u & 0x80);

    return decodeZigzag64(encoded);
}

} // namespace avro
