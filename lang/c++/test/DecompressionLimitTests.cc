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

// AVRO-4285: a data-file block is decompressed according to the file's codec. A
// block with a very high compression ratio (or a malformed block) can expand to
// far more memory than its compressed size. Reading such a block must be
// rejected once its decompressed size would exceed the configured maximum, which
// these tests set to a small value via AVRO_MAX_DECOMPRESS_LENGTH.

#include <cstdlib>
#include <filesystem>
#include <sstream>
#include <string>

#include <boost/test/included/unit_test.hpp>

#include "Compiler.hh"
#include "DataFile.hh"
#include "Exception.hh"
#include "ValidSchema.hh"

namespace avro {

static void setDecompressLimit(const char *value) {
#ifdef _WIN32
    _putenv_s("AVRO_MAX_DECOMPRESS_LENGTH", value);
#else
    setenv("AVRO_MAX_DECOMPRESS_LENGTH", value, 1);
#endif
}

static ValidSchema stringSchema() {
    std::istringstream iss("\"string\"");
    ValidSchema vs;
    compileJsonSchema(iss, vs);
    return vs;
}

static std::string tempFile(const char *name) {
    return (std::filesystem::temp_directory_path() / name).string();
}

// Write a single, highly compressible value with the given codec, then read it
// back with a small decompression limit and confirm the read is rejected.
static void checkCodecRejectsOversized(Codec codec, const char *name) {
    ValidSchema schema = stringSchema();
    std::string path = tempFile(name);
    std::string big(4 * 1024 * 1024, 'a'); // 4 MiB, compresses tiny

    // Let any writer exception propagate: a failure here is a real problem
    // (e.g. permissions or I/O), not a reason to silently skip. Codecs that are
    // not compiled in are excluded via the #ifdef guards on the callers below.
    {
        DataFileWriter<std::string> writer(path.c_str(), schema, 64 * 1024 * 1024, codec);
        writer.write(big);
        writer.close();
    }

    setDecompressLimit("1048576"); // 1 MiB, smaller than the 4 MiB block

    bool rejected = false;
    try {
        DataFileReader<std::string> reader(path.c_str(), schema);
        std::string out;
        reader.read(out); // triggers block decompression
    } catch (const Exception &) {
        rejected = true;
    }
    std::filesystem::remove(path);
    BOOST_CHECK_MESSAGE(rejected, std::string("codec not bounded: ") + name);
}

static void testDeflateDecompressionLimit() {
    checkCodecRejectsOversized(DEFLATE_CODEC, "avro_decompress_limit_deflate.avro");
}

static void testSnappyDecompressionLimit() {
#ifdef SNAPPY_CODEC_AVAILABLE
    checkCodecRejectsOversized(SNAPPY_CODEC, "avro_decompress_limit_snappy.avro");
#else
    BOOST_TEST_MESSAGE("Snappy codec not available; skipping");
#endif
}

static void testZstdDecompressionLimit() {
#ifdef ZSTD_CODEC_AVAILABLE
    checkCodecRejectsOversized(ZSTD_CODEC, "avro_decompress_limit_zstd.avro");
#else
    BOOST_TEST_MESSAGE("Zstandard codec not available; skipping");
#endif
}

static void testWithinLimitStillReads() {
    ValidSchema schema = stringSchema();
    std::string path = tempFile("avro_decompress_within_limit.avro");
    std::string payload = "hello world";

    {
        DataFileWriter<std::string> writer(path.c_str(), schema, 64 * 1024 * 1024, DEFLATE_CODEC);
        writer.write(payload);
        writer.close();
    }

    setDecompressLimit("1048576");

    std::string out;
    {
        DataFileReader<std::string> reader(path.c_str(), schema);
        BOOST_CHECK(reader.read(out));
    }
    std::filesystem::remove(path);
    BOOST_CHECK_EQUAL(out, payload);
}

} // namespace avro

boost::unit_test::test_suite *
init_unit_test_suite(int, char *[]) {
    using namespace boost::unit_test;

    auto *ts = BOOST_TEST_SUITE("Avro C++ decompression limit tests");
    ts->add(BOOST_TEST_CASE(&avro::testDeflateDecompressionLimit));
    ts->add(BOOST_TEST_CASE(&avro::testSnappyDecompressionLimit));
    ts->add(BOOST_TEST_CASE(&avro::testZstdDecompressionLimit));
    ts->add(BOOST_TEST_CASE(&avro::testWithinLimitStillReads));
    return ts;
}
