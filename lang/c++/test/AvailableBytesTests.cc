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

// A bytes/string value is encoded as a length prefix followed by that many
// bytes of data. A malicious or truncated input can declare a huge length with
// little or no actual data, which would otherwise trigger a correspondingly
// huge allocation before the shortfall is noticed. When the input stream can
// report how many bytes remain (a memory-backed stream), the declared length
// must be rejected before allocating for it.

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include <boost/test/included/unit_test.hpp>

#include "Compiler.hh"
#include "Decoder.hh"
#include "Encoder.hh"
#include "Exception.hh"
#include "Generic.hh"
#include "Stream.hh"
#include "ValidSchema.hh"

namespace avro {

// Reading a bytes value whose declared length far exceeds the tiny backing
// buffer must throw instead of attempting a huge allocation.
static void testDecodeBytesRejectsOversizedLength() {
    // 0xFE 0x01 is the zig-zag long 127; the buffer carries no data after it.
    const uint8_t data[] = {0xFE, 0x01};
    InputStreamPtr in = memoryInputStream(data, sizeof(data));
    DecoderPtr d = binaryDecoder();
    d->init(*in);
    std::vector<uint8_t> value;
    BOOST_CHECK_THROW(d->decodeBytes(value), Exception);
}

static void testDecodeStringRejectsOversizedLength() {
    const uint8_t data[] = {0xFE, 0x01};
    InputStreamPtr in = memoryInputStream(data, sizeof(data));
    DecoderPtr d = binaryDecoder();
    d->init(*in);
    std::string value;
    BOOST_CHECK_THROW(d->decodeString(value), Exception);
}

// A well-formed value whose declared length fits the buffer still decodes.
static void testDecodeStringWithinLimitStillReads() {
    // length 3 (zig-zag 6 -> 0x06) followed by "abc".
    const uint8_t data[] = {0x06, 'a', 'b', 'c'};
    InputStreamPtr in = memoryInputStream(data, sizeof(data));
    DecoderPtr d = binaryDecoder();
    d->init(*in);
    std::string value;
    d->decodeString(value);
    BOOST_CHECK_EQUAL(value, std::string("abc"));
}

static void testDecodeBytesWithinLimitStillReads() {
    const uint8_t data[] = {0x06, 0x01, 0x02, 0x03};
    InputStreamPtr in = memoryInputStream(data, sizeof(data));
    DecoderPtr d = binaryDecoder();
    d->init(*in);
    std::vector<uint8_t> value;
    d->decodeBytes(value);
    BOOST_CHECK_EQUAL(value.size(), 3u);
    BOOST_CHECK_EQUAL(value[0], 1);
    BOOST_CHECK_EQUAL(value[2], 3);
}

// An array/map block declares an element count; a malicious or truncated input
// can declare far more elements than the remaining bytes could hold. The count
// is validated against the bytes remaining before resizing, using the minimum
// on-wire size of the element schema (so 0-byte elements like null are not
// falsely rejected).
static GenericDatum decodeCollectionHeader(const ValidSchema &s, int64_t blockCount,
                                           bool addEndMarker) {
    // Keep the output stream alive for the whole decode: memoryInputStream
    // references the output stream's buffer rather than copying it.
    std::unique_ptr<OutputStream> os = memoryOutputStream();
    EncoderPtr e = binaryEncoder();
    e->init(*os);
    e->encodeLong(blockCount);
    if (addEndMarker) {
        e->encodeLong(0);
    }
    e->flush();

    InputStreamPtr in = memoryInputStream(*os);
    DecoderPtr d = binaryDecoder();
    d->init(*in);
    GenericDatum datum;
    GenericReader::read(*d, datum, s);
    return datum;
}

static void testReadArrayRejectsOversizedCount() {
    ValidSchema s = compileJsonSchemaFromString(
        "{\"type\":\"array\",\"items\":\"long\"}");
    // 1,000,000 long elements declared, but no element data follows.
    BOOST_CHECK_THROW(decodeCollectionHeader(s, 1000000, false), Exception);
}

static void testReadMapRejectsOversizedCount() {
    ValidSchema s = compileJsonSchemaFromString(
        "{\"type\":\"map\",\"values\":\"long\"}");
    BOOST_CHECK_THROW(decodeCollectionHeader(s, 1000000, false), Exception);
}

static void testReadArrayOfNullNotFalselyRejected() {
    ValidSchema s = compileJsonSchemaFromString(
        "{\"type\":\"array\",\"items\":\"null\"}");
    // 100,000 nulls (zero bytes each) is legitimate and must decode.
    GenericDatum datum = decodeCollectionHeader(s, 100000, true);
    BOOST_CHECK_EQUAL(datum.value<GenericArray>().value().size(), 100000u);
}

// A deeply but acyclically nested record whose only leaf is null encodes to
// zero bytes, so the per-element minimum must be 0 and a large array of such
// records must not be falsely rejected. This exercises the recursion depth
// guard in minBytesPerElement(), which must yield a conservative lower bound
// (0) rather than over-estimating for a legitimately deep schema.
static void testReadArrayOfDeeplyNestedNullNotFalselyRejected() {
    // Build ~70 nested records: R0 { null f; }, R1 { R0 f; }, ... The nesting
    // exceeds the depth guard, yet every leaf is null so the true minimum is 0.
    std::string schema = "\"null\"";
    for (int i = 0; i < 70; ++i) {
        schema = "{\"type\":\"record\",\"name\":\"R" + std::to_string(i) +
                 "\",\"fields\":[{\"name\":\"f\",\"type\":" + schema + "}]}";
    }
    ValidSchema s = compileJsonSchemaFromString(
        ("{\"type\":\"array\",\"items\":" + schema + "}").c_str());
    // 100,000 zero-byte elements is legitimate and must decode.
    GenericDatum datum = decodeCollectionHeader(s, 100000, true);
    BOOST_CHECK_EQUAL(datum.value<GenericArray>().value().size(), 100000u);
}

// Calling bytesRemaining() immediately after init(), before any data has been
// buffered, must be well-defined (the underlying StreamReader must not subtract
// null buffer pointers) and report the whole stream as available.
static void testBytesRemainingRightAfterInit() {
    const uint8_t data[] = {0x06, 'a', 'b', 'c'};
    InputStreamPtr in = memoryInputStream(data, sizeof(data));
    DecoderPtr d = binaryDecoder();
    d->init(*in);
    BOOST_CHECK_EQUAL(d->bytesRemaining(), static_cast<int64_t>(sizeof(data)));
}

// Zero-byte elements (null, a record with only zero-byte fields) consume no
// input, so ensureCollectionAvailable cannot bound their count. A huge declared
// block count of such elements is capped against a configurable limit.
static GenericDatum decodeLongs(const ValidSchema &s, const std::vector<int64_t> &longs) {
    std::unique_ptr<OutputStream> os = memoryOutputStream();
    EncoderPtr e = binaryEncoder();
    e->init(*os);
    for (int64_t v : longs) {
        e->encodeLong(v);
    }
    e->flush();
    InputStreamPtr in = memoryInputStream(*os);
    DecoderPtr d = binaryDecoder();
    d->init(*in);
    GenericDatum datum;
    GenericReader::read(*d, datum, s);
    return datum;
}

static void testReadArrayOfNullRejectsCountAboveDefaultLimit() {
    ValidSchema s = compileJsonSchemaFromString(
        "{\"type\":\"array\",\"items\":\"null\"}");
    // The reported exploit: 200,000,000 nulls rejected by the default limit.
    BOOST_CHECK_THROW(decodeCollectionHeader(s, 200000000, true), Exception);
}

static void testReadArrayOfAllNullRecordRejectsHugeCount() {
    ValidSchema s = compileJsonSchemaFromString(
        "{\"type\":\"array\",\"items\":"
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"n\",\"type\":\"null\"}]}}");
    BOOST_CHECK_THROW(decodeCollectionHeader(s, 200000000, true), Exception);
}

static void testReadArrayOfNullRejectsNegativeCount() {
    ValidSchema s = compileJsonSchemaFromString(
        "{\"type\":\"array\",\"items\":\"null\"}");
    // Negative count (-200M), block byte-size 0, end marker 0: normalized to
    // 200M and still bounded.
    BOOST_CHECK_THROW(decodeLongs(s, {-200000000, 0, 0}), Exception);
}

static void testReadMapOfNullRejectedByAvailableBytes() {
    ValidSchema s = compileJsonSchemaFromString(
        "{\"type\":\"map\",\"values\":\"null\"}");
    // Each map entry carries a >= 1 byte key, so a huge map<null> is bounded by
    // the bytes-remaining check.
    BOOST_CHECK_THROW(decodeCollectionHeader(s, 200000000, false), Exception);
}

static void testReadArrayOfNullRespectsConfiguredLimit() {
    ValidSchema s = compileJsonSchemaFromString(
        "{\"type\":\"array\",\"items\":\"null\"}");
    setenv("AVRO_MAX_COLLECTION_ITEMS", "1000", 1);
    // Within the limit decodes; over the limit and cumulative are rejected.
    GenericDatum ok = decodeCollectionHeader(s, 1000, true);
    BOOST_CHECK_EQUAL(ok.value<GenericArray>().value().size(), 1000u);
    BOOST_CHECK_THROW(decodeCollectionHeader(s, 1001, true), Exception);
    BOOST_CHECK_THROW(decodeLongs(s, {600, 600, 0}), Exception);
    unsetenv("AVRO_MAX_COLLECTION_ITEMS");
}

// A backed non-zero-byte array that passes the bytes check is still bounded by
// the structural cap (exercised with a lowered limit).
static void testReadArrayOfLongRejectedByStructuralCap() {
    ValidSchema s = compileJsonSchemaFromString(
        "{\"type\":\"array\",\"items\":\"long\"}");
    setenv("AVRO_MAX_COLLECTION_ITEMS", "5", 1);
    // 10 real longs: block count 10, ten 1-byte longs, end marker.
    std::vector<int64_t> longs = {10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
    BOOST_CHECK_THROW(decodeLongs(s, longs), Exception);
    unsetenv("AVRO_MAX_COLLECTION_ITEMS");
}

// Skipping a huge array (via the decoder's skipArray) is bounded by the
// structural cap.
static void testSkipArrayRejectsHugeCount() {
    setenv("AVRO_MAX_COLLECTION_ITEMS", "1000", 1);
    std::unique_ptr<OutputStream> os = memoryOutputStream();
    EncoderPtr e = binaryEncoder();
    e->init(*os);
    e->encodeLong(2000); // block count 2000, no data needed (skip throws first)
    e->flush();
    InputStreamPtr in = memoryInputStream(*os);
    DecoderPtr d = binaryDecoder();
    d->init(*in);
    BOOST_CHECK_THROW(d->skipArray(), Exception);
    unsetenv("AVRO_MAX_COLLECTION_ITEMS");
}

} // namespace avro

boost::unit_test::test_suite *
init_unit_test_suite(int, char *[]) {
    using namespace boost::unit_test;

    auto *ts = BOOST_TEST_SUITE("Avro C++ available-bytes tests");
    ts->add(BOOST_TEST_CASE(&avro::testDecodeBytesRejectsOversizedLength));
    ts->add(BOOST_TEST_CASE(&avro::testDecodeStringRejectsOversizedLength));
    ts->add(BOOST_TEST_CASE(&avro::testDecodeStringWithinLimitStillReads));
    ts->add(BOOST_TEST_CASE(&avro::testDecodeBytesWithinLimitStillReads));
    ts->add(BOOST_TEST_CASE(&avro::testReadArrayRejectsOversizedCount));
    ts->add(BOOST_TEST_CASE(&avro::testReadMapRejectsOversizedCount));
    ts->add(BOOST_TEST_CASE(&avro::testReadArrayOfNullNotFalselyRejected));
    ts->add(BOOST_TEST_CASE(&avro::testReadArrayOfDeeplyNestedNullNotFalselyRejected));
    ts->add(BOOST_TEST_CASE(&avro::testBytesRemainingRightAfterInit));
    ts->add(BOOST_TEST_CASE(&avro::testReadArrayOfNullRejectsCountAboveDefaultLimit));
    ts->add(BOOST_TEST_CASE(&avro::testReadArrayOfAllNullRecordRejectsHugeCount));
    ts->add(BOOST_TEST_CASE(&avro::testReadArrayOfNullRejectsNegativeCount));
    ts->add(BOOST_TEST_CASE(&avro::testReadMapOfNullRejectedByAvailableBytes));
    ts->add(BOOST_TEST_CASE(&avro::testReadArrayOfNullRespectsConfiguredLimit));
    ts->add(BOOST_TEST_CASE(&avro::testReadArrayOfLongRejectedByStructuralCap));
    ts->add(BOOST_TEST_CASE(&avro::testSkipArrayRejectsHugeCount));
    return ts;
}
