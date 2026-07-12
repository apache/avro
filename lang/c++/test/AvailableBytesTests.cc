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
    return ts;
}
