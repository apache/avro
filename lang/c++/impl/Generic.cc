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

#include "Generic.hh"
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <utility>

namespace avro {

using std::ostringstream;
using std::string;
using std::vector;

// Minimum number of bytes a single value of the given schema node can occupy on
// the wire. Used to reject an array/map block count that could not be backed by
// the bytes remaining. A type that can encode to zero bytes (null) returns 0,
// which disables the collection check for it (so an array of nulls is not
// falsely rejected). A depth limit breaks self-referencing (symbolic) schemas.
static int64_t minBytesPerElement(const NodePtr &node, int depth) {
    if (!node) {
        return 0;
    }
    switch (node->type()) {
        case AVRO_NULL:
            return 0;
        case AVRO_FLOAT:
            return 4;
        case AVRO_DOUBLE:
            return 8;
        case AVRO_FIXED: {
            // fixedSize() is a size_t; clamp to int64_t so a huge fixed size
            // cannot wrap negative.
            return static_cast<int64_t>(std::min<uint64_t>(
                node->fixedSize(),
                static_cast<uint64_t>(std::numeric_limits<int64_t>::max())));
        }
        case AVRO_RECORD: {
            if (depth > 64) {
                // Purely a recursion (stack-overflow) safety net for a
                // pathologically deep schema. A truly cyclic schema never
                // reaches this: a self-reference is an AVRO_SYMBOLIC node,
                // handled by the default case below (returning 1 without
                // following the link), so recursion always terminates. This
                // guard therefore only trips on a genuinely deep *acyclic*
                // record, whose true minimum can still be 0 (e.g. a long chain
                // of records whose only leaves are null). Return 0 rather than
                // over-estimating, so a valid array/map of such elements is not
                // falsely rejected; 0 is always a valid lower bound.
                return 0;
            }
            int64_t total = 0;
            for (size_t i = 0; i < node->leaves(); ++i) {
                int64_t fieldMin = minBytesPerElement(node->leafAt(i), depth + 1);
                // Saturate rather than overflow: a wrapped (negative) total
                // would disable the collection check.
                if (fieldMin > std::numeric_limits<int64_t>::max() - total) {
                    return std::numeric_limits<int64_t>::max();
                }
                total += fieldMin;
            }
            return total;
        }
        default:
            // boolean, int, long, bytes, string, enum, union, array, map and
            // symbolic references all encode to at least one byte.
            return 1;
    }
}

// Default maximum number of zero-byte-encoded collection elements (e.g. an
// array of nulls) to allocate from a single decode. Such elements consume no
// input, so ensureCollectionAvailable cannot bound their count; without a cap a
// tiny payload can declare a huge block count and exhaust memory. Overridable
// via the AVRO_MAX_COLLECTION_ITEMS environment variable.
static constexpr int64_t kDefaultMaxCollectionItems = 10000000;

// Structural cap on the number of elements in any array or map (an overflow /
// defense-in-depth guard). It matches the value used by the other Avro SDKs
// (Integer.MAX_VALUE - 8); non-zero-byte elements are also bounded by the bytes
// remaining.
static constexpr int64_t kDefaultMaxCollectionStructural = 2147483639;

struct CollectionLimits {
    int64_t zeroByte;
    int64_t structural;
};

// AVRO_MAX_COLLECTION_ITEMS, when a non-negative integer, overrides both limits
// to that value; otherwise zero-byte elements use the tighter default and all
// collections use the structural default.
static CollectionLimits collectionLimits() {
    const char *env = std::getenv("AVRO_MAX_COLLECTION_ITEMS");
    if (env != nullptr && *env != '\0') {
        char *end = nullptr;
        long long value = std::strtoll(env, &end, 10);
        if (*end == '\0' && value >= 0) {
            return {static_cast<int64_t>(value), static_cast<int64_t>(value)};
        }
    }
    return {kDefaultMaxCollectionItems, kDefaultMaxCollectionStructural};
}

// Reject a collection (array or map) block that could drive an unbounded
// allocation, before resizing for it. A block whose declared element count
// could not be backed by the bytes actually remaining is rejected (only when
// the per-element minimum is positive and the decoder can report the bytes
// remaining); and every collection is bounded by the structural cap, which also
// covers zero-byte elements and decoders that cannot report the bytes remaining.
// The comparisons divide/subtract to avoid overflow.
static void ensureCollectionAvailable(Decoder &d, size_t existing, size_t count, int64_t minBytes) {
    if (count == 0) {
        return;
    }
    if (minBytes > 0) {
        int64_t remaining = d.bytesRemaining();
        if (remaining >= 0 &&
            static_cast<uint64_t>(count) >
                static_cast<uint64_t>(remaining) / static_cast<uint64_t>(minBytes)) {
            throw Exception(
                "Collection claims {} elements with at least {} bytes each, "
                "but only {} bytes are available",
                count, minBytes, remaining);
        }
    }
    const int64_t structural = collectionLimits().structural;
    const uint64_t limit = static_cast<uint64_t>(structural);
    if (static_cast<uint64_t>(count) > limit ||
        static_cast<uint64_t>(existing) > limit - static_cast<uint64_t>(count)) {
        throw Exception(
            "Cannot read a collection of more than {} elements; "
            "set AVRO_MAX_COLLECTION_ITEMS if this is legitimate",
            structural);
    }
}

// Reject a collection of zero-byte elements (e.g. null) whose cumulative count
// exceeds the configured limit. These elements consume no input, so they cannot
// be bounded by the bytes remaining; the count is the only signal.
static void ensureZeroByteCollectionWithinLimit(size_t existing, size_t count) {
    const int64_t zeroByte = collectionLimits().zeroByte;
    const uint64_t limit = static_cast<uint64_t>(zeroByte);
    // Compare without adding, so existing + count cannot overflow.
    if (static_cast<uint64_t>(count) > limit ||
        static_cast<uint64_t>(existing) > limit - static_cast<uint64_t>(count)) {
        throw Exception(
            "Cannot read a collection of more than {} zero-byte elements; "
            "set AVRO_MAX_COLLECTION_ITEMS if this is legitimate",
            zeroByte);
    }
}

// Guard against size_t overflow / an over-large request when growing a
// collection container by `count` elements before calling resize().
template<typename Container>
static void ensureCanGrow(const Container &c, size_t count) {
    if (count > c.max_size() - c.size()) {
        throw Exception(
            "Collection block count {} exceeds the maximum container size",
            count);
    }
}

typedef vector<uint8_t> bytes;

void GenericContainer::assertType(const NodePtr &schema, Type type) {
    if (schema->type() != type) {
        throw Exception("Schema type {} expected {}", schema->type(), type);
    }
}

GenericReader::GenericReader(ValidSchema s, const DecoderPtr &decoder) : schema_(std::move(s)), isResolving_(dynamic_cast<ResolvingDecoder *>(&(*decoder)) != nullptr),
                                                                         decoder_(decoder) {
}

GenericReader::GenericReader(const ValidSchema &writerSchema,
                             const ValidSchema &readerSchema, const DecoderPtr &decoder) : schema_(readerSchema),
                                                                                           isResolving_(true),
                                                                                           decoder_(resolvingDecoder(writerSchema, readerSchema, decoder)) {
}

void GenericReader::read(GenericDatum &datum) const {
    datum = GenericDatum(schema_.root());
    read(datum, *decoder_, isResolving_);
}

void GenericReader::read(GenericDatum &datum, Decoder &d, bool isResolving) {
    if (datum.isUnion()) {
        datum.selectBranch(d.decodeUnionIndex());
    }
    switch (datum.type()) {
        case AVRO_NULL:
            d.decodeNull();
            break;
        case AVRO_BOOL:
            datum.value<bool>() = d.decodeBool();
            break;
        case AVRO_INT:
            datum.value<int32_t>() = d.decodeInt();
            break;
        case AVRO_LONG:
            datum.value<int64_t>() = d.decodeLong();
            break;
        case AVRO_FLOAT:
            datum.value<float>() = d.decodeFloat();
            break;
        case AVRO_DOUBLE:
            datum.value<double>() = d.decodeDouble();
            break;
        case AVRO_STRING:
            d.decodeString(datum.value<string>());
            break;
        case AVRO_BYTES:
            d.decodeBytes(datum.value<bytes>());
            break;
        case AVRO_FIXED: {
            auto &f = datum.value<GenericFixed>();
            d.decodeFixed(f.schema()->fixedSize(), f.value());
        } break;
        case AVRO_RECORD: {
            auto &r = datum.value<GenericRecord>();
            size_t c = r.schema()->leaves();
            if (isResolving) {
                std::vector<size_t> fo =
                    static_cast<ResolvingDecoder &>(d).fieldOrder();
                for (size_t i = 0; i < c; ++i) {
                    read(r.fieldAt(fo[i]), d, isResolving);
                }
            } else {
                for (size_t i = 0; i < c; ++i) {
                    read(r.fieldAt(i), d, isResolving);
                }
            }
        } break;
        case AVRO_ENUM:
            datum.value<GenericEnum>().set(d.decodeEnum());
            break;
        case AVRO_ARRAY: {
            auto &v = datum.value<GenericArray>();
            vector<GenericDatum> &r = v.value();
            const NodePtr &nn = v.schema()->leafAt(0);
            r.resize(0);
            size_t start = 0;
            // Only when not resolving: the datum schema then matches the wire
            // schema, so minBytesPerElement is a true lower bound. Under
            // resolution the wire (writer) type may be smaller than the datum
            // (reader) type, which would over-estimate and reject valid data.
            int64_t trueMin = minBytesPerElement(nn, 0);
            // Under resolution the on-wire (writer) element can be zero bytes
            // even when the reader element is not (e.g. reader-only fields filled
            // from defaults), so the bytes check is disabled and we cannot tell
            // whether an element is zero-byte on the wire. Apply the tighter
            // zero-byte cap conservatively in that case, so the up-front resize
            // cannot be driven past it.
            bool zeroByte = isResolving || trueMin == 0;
            int64_t minBytes = isResolving ? 0 : trueMin;
            for (size_t m = d.arrayStart(); m != 0; m = d.arrayNext()) {
                ensureCollectionAvailable(d, r.size(), m, minBytes);
                // Zero-byte elements are not bounded by the bytes check, so cap
                // their cumulative count (r.size() is the count so far).
                if (zeroByte) {
                    ensureZeroByteCollectionWithinLimit(r.size(), m);
                }
                ensureCanGrow(r, m);
                r.resize(r.size() + m);
                for (; start < r.size(); ++start) {
                    r[start] = GenericDatum(nn);
                    read(r[start], d, isResolving);
                }
            }
        } break;
        case AVRO_MAP: {
            auto &v = datum.value<GenericMap>();
            GenericMap::Value &r = v.value();
            const NodePtr &nn = v.schema()->leafAt(1);
            r.resize(0);
            size_t start = 0;
            // Map keys are strings (>= 1 byte length prefix) plus the value.
            // Saturate the +1 so a maxed-out value minimum cannot wrap.
            int64_t valuesMin = minBytesPerElement(nn, 0);
            // A map entry always includes a >= 1 byte key, so it is never a
            // zero-byte element and even under resolution the count is bounded by
            // the bytes remaining (each entry consumes at least the key). Use 1
            // when resolving (the value type may differ), else the key + value.
            int64_t minBytes = isResolving
                                   ? 1
                                   : (valuesMin < std::numeric_limits<int64_t>::max()
                                          ? valuesMin + 1
                                          : valuesMin);
            for (size_t m = d.mapStart(); m != 0; m = d.mapNext()) {
                ensureCollectionAvailable(d, r.size(), m, minBytes);
                ensureCanGrow(r, m);
                r.resize(r.size() + m);
                for (; start < r.size(); ++start) {
                    d.decodeString(r[start].first);
                    r[start].second = GenericDatum(nn);
                    read(r[start].second, d, isResolving);
                }
            }
        } break;
        default:
            throw Exception("Unknown schema type {}", datum.type());
    }
}

void GenericReader::read(Decoder &d, GenericDatum &g, const ValidSchema &s) {
    g = GenericDatum(s);
    read(d, g);
}

void GenericReader::read(Decoder &d, GenericDatum &g) {
    read(g, d, dynamic_cast<ResolvingDecoder *>(&d) != nullptr);
}

GenericWriter::GenericWriter(ValidSchema s, EncoderPtr encoder) : schema_(std::move(s)), encoder_(std::move(encoder)) {
}

void GenericWriter::write(const GenericDatum &datum) const {
    write(datum, *encoder_);
}

void GenericWriter::write(const GenericDatum &datum, Encoder &e) {
    if (datum.isUnion()) {
        e.encodeUnionIndex(datum.unionBranch());
    }
    switch (datum.type()) {
        case AVRO_NULL:
            e.encodeNull();
            break;
        case AVRO_BOOL:
            e.encodeBool(datum.value<bool>());
            break;
        case AVRO_INT:
            e.encodeInt(datum.value<int32_t>());
            break;
        case AVRO_LONG:
            e.encodeLong(datum.value<int64_t>());
            break;
        case AVRO_FLOAT:
            e.encodeFloat(datum.value<float>());
            break;
        case AVRO_DOUBLE:
            e.encodeDouble(datum.value<double>());
            break;
        case AVRO_STRING:
            e.encodeString(datum.value<string>());
            break;
        case AVRO_BYTES:
            e.encodeBytes(datum.value<bytes>());
            break;
        case AVRO_FIXED:
            e.encodeFixed(datum.value<GenericFixed>().value());
            break;
        case AVRO_RECORD: {
            const auto &r = datum.value<GenericRecord>();
            size_t c = r.schema()->leaves();
            for (size_t i = 0; i < c; ++i) {
                write(r.fieldAt(i), e);
            }
        } break;
        case AVRO_ENUM:
            e.encodeEnum(datum.value<GenericEnum>().value());
            break;
        case AVRO_ARRAY: {
            const GenericArray::Value &r = datum.value<GenericArray>().value();
            e.arrayStart();
            if (!r.empty()) {
                e.setItemCount(r.size());
                for (const auto &it : r) {
                    e.startItem();
                    write(it, e);
                }
            }
            e.arrayEnd();
        } break;
        case AVRO_MAP: {
            const GenericMap::Value &r = datum.value<GenericMap>().value();
            e.mapStart();
            if (!r.empty()) {
                e.setItemCount(r.size());
                for (const auto &it : r) {
                    e.startItem();
                    e.encodeString(it.first);
                    write(it.second, e);
                }
            }
            e.mapEnd();
        } break;
        default:
            throw Exception("Unknown schema type {}", datum.type());
    }
}

void GenericWriter::write(Encoder &e, const GenericDatum &g) {
    write(g, e);
}

} // namespace avro
