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

#include "Compiler.hh"
#include "big_union.hh"
#include "bigrecord.hh"
#include "bigrecord_r.hh"
#include "tweet.hh"
#include "union_array_union.hh"
#include "union_empty_record.hh"
#include "union_map_union.hh"
#include "union_redundant_types.hh"

#include <array>
#include <boost/test/included/unit_test.hpp>

#ifdef min
#undef min
#endif

#ifdef max
#undef max
#endif

using std::ifstream;
using std::map;
using std::string;
using std::unique_ptr;
using std::vector;

using avro::binaryDecoder;
using avro::binaryEncoder;
using avro::Decoder;
using avro::DecoderPtr;
using avro::Encoder;
using avro::EncoderPtr;
using avro::InputStream;
using avro::memoryInputStream;
using avro::memoryOutputStream;
using avro::OutputStream;
using avro::validatingDecoder;
using avro::validatingEncoder;
using avro::ValidSchema;

void setRecord(testgen::RootRecord &myRecord) {
    uint8_t fixed[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

    myRecord.mylong = 212;
    myRecord.nestedrecord.inval1 = std::numeric_limits<double>::min();
    myRecord.nestedrecord.inval2 = "hello world";
    myRecord.nestedrecord.inval3 = std::numeric_limits<int32_t>::max();

    myRecord.mymap["one"] = 100;
    myRecord.mymap["two"] = 200;

    myRecord.myarray.push_back(3434.9);
    myRecord.myarray.push_back(7343.9);
    myRecord.myarray.push_back(-63445.9);
    myRecord.myenum = testgen::ExampleEnum::one;

    map<string, int32_t> m;
    m["one"] = 1;
    m["two"] = 2;
    myRecord.myunion.set_map(m);

    vector<uint8_t> v;
    v.push_back(1);
    v.push_back(2);
    myRecord.anotherunion.set_bytes(v);

    myRecord.mybool = true;
    myRecord.anothernested.inval1 = std::numeric_limits<double>::max();
    myRecord.anothernested.inval2 = "goodbye world";
    myRecord.anothernested.inval3 = std::numeric_limits<int32_t>::min();
    memcpy(&myRecord.myfixed[0], fixed, myRecord.myfixed.size());
    myRecord.anotherint = 4534;
    myRecord.bytes.push_back(10);
    myRecord.bytes.push_back(20);
}

template<typename T1, typename T2>
void checkRecord(const T1 &r1, const T2 &r2) {
    BOOST_CHECK_EQUAL(r1.mylong, r2.mylong);
    BOOST_CHECK_EQUAL(r1.nestedrecord.inval1, r2.nestedrecord.inval1);
    BOOST_CHECK_EQUAL(r1.nestedrecord.inval2, r2.nestedrecord.inval2);
    BOOST_CHECK_EQUAL(r1.nestedrecord.inval3, r2.nestedrecord.inval3);
    BOOST_CHECK(r1.mymap == r2.mymap);
    BOOST_CHECK(r1.myarray == r2.myarray);
    BOOST_CHECK_EQUAL(r1.myunion.idx(), r2.myunion.idx());
    BOOST_CHECK(r1.myunion.get_map() == r2.myunion.get_map());
    BOOST_CHECK_EQUAL(r1.anotherunion.idx(), r2.anotherunion.idx());
    BOOST_CHECK(r1.anotherunion.get_bytes() == r2.anotherunion.get_bytes());
    BOOST_CHECK_EQUAL(r1.mybool, r2.mybool);
    BOOST_CHECK_EQUAL(r1.anothernested.inval1, r2.anothernested.inval1);
    BOOST_CHECK_EQUAL(r1.anothernested.inval2, r2.anothernested.inval2);
    BOOST_CHECK_EQUAL(r1.anothernested.inval3, r2.anothernested.inval3);
    BOOST_CHECK_EQUAL_COLLECTIONS(r1.myfixed.begin(), r1.myfixed.end(),
                                  r2.myfixed.begin(), r2.myfixed.end());
    BOOST_CHECK_EQUAL(r1.anotherint, r2.anotherint);
    BOOST_CHECK_EQUAL(r1.bytes.size(), r2.bytes.size());
    BOOST_CHECK_EQUAL_COLLECTIONS(r1.bytes.begin(), r1.bytes.end(),
                                  r2.bytes.begin(), r2.bytes.end());
    /**
     * Usually, comparing two different enums is not reliable. But here it fine because we
     * know the generated code and are merely checking if Avro did the right job.
     * Also, converting enum into unsigned int is not always safe. There are cases there could be
     * truncation. Again, we have a controlled situation and it is safe here.
     */
    BOOST_CHECK_EQUAL(static_cast<unsigned int>(r1.myenum), static_cast<unsigned int>(r2.myenum));
}

void checkDefaultValues(const testgen_r::RootRecord &r) {
    BOOST_CHECK_EQUAL(r.withDefaultValue.s1, "\"sval\\u8352\"");
    BOOST_CHECK_EQUAL(r.withDefaultValue.i1, 99);
    BOOST_CHECK_CLOSE(r.withDefaultValue.d1, 5.67, 1e-10);
    BOOST_CHECK_EQUAL(r.myarraywithDefaultValue[0], 2);
    BOOST_CHECK_EQUAL(r.myarraywithDefaultValue[1], 3);
    BOOST_CHECK_EQUAL(r.myfixedwithDefaultValue.get_val()[0], 0x01);
    BOOST_CHECK_EQUAL(r.byteswithDefaultValue.get_bytes()[0], 0xff);
    BOOST_CHECK_EQUAL(r.byteswithDefaultValue.get_bytes()[1], 0xaa);
}

// enable use of BOOST_CHECK_EQUAL
template<>
struct boost::test_tools::tt_detail::print_log_value<big_union::RootRecord::big_union_t::Branch> {
    void operator()(std::ostream &stream, const big_union::RootRecord::big_union_t::Branch &branch) const {
        stream << "big_union_t::Branch{" << static_cast<size_t>(branch) << "}";
    }
};

void testEncoding() {
    ValidSchema s;
    ifstream ifs("jsonschemas/bigrecord");
    compileJsonSchema(ifs, s);
    unique_ptr<OutputStream> os = memoryOutputStream();
    EncoderPtr e = validatingEncoder(s, binaryEncoder());
    e->init(*os);
    testgen::RootRecord t1;
    setRecord(t1);
    avro::encode(*e, t1);
    e->flush();

    DecoderPtr d = validatingDecoder(s, binaryDecoder());
    unique_ptr<InputStream> is = memoryInputStream(*os);
    d->init(*is);
    testgen::RootRecord t2;
    avro::decode(*d, t2);

    checkRecord(t2, t1);
}

void testResolution() {
    ValidSchema s_w;
    ifstream ifs_w("jsonschemas/bigrecord");
    compileJsonSchema(ifs_w, s_w);
    unique_ptr<OutputStream> os = memoryOutputStream();
    EncoderPtr e = validatingEncoder(s_w, binaryEncoder());
    e->init(*os);
    testgen::RootRecord t1;
    setRecord(t1);
    avro::encode(*e, t1);
    e->flush();

    ValidSchema s_r;
    ifstream ifs_r("jsonschemas/bigrecord_r");
    compileJsonSchema(ifs_r, s_r);
    DecoderPtr dd = binaryDecoder();
    unique_ptr<InputStream> is = memoryInputStream(*os);
    dd->init(*is);
    DecoderPtr rd = resolvingDecoder(s_w, s_r, dd);
    testgen_r::RootRecord t2;
    avro::decode(*rd, t2);

    checkRecord(t2, t1);
    checkDefaultValues(t2);

    //Re-use the resolving decoder to decode again.
    unique_ptr<InputStream> is1 = memoryInputStream(*os);
    rd->init(*is1);
    testgen_r::RootRecord t3;
    avro::decode(*rd, t3);
    checkRecord(t3, t1);
    checkDefaultValues(t3);

    // Test serialization of default values.
    // Serialize to string then compile from string.
    std::ostringstream oss;
    s_r.toJson(oss);
    ValidSchema s_rs = avro::compileJsonSchemaFromString(oss.str());

    std::unique_ptr<InputStream> is2 = memoryInputStream(*os);
    dd->init(*is2);
    rd = resolvingDecoder(s_w, s_rs, dd);
    testgen_r::RootRecord t4;
    avro::decode(*rd, t4);
    checkDefaultValues(t4);

    std::ostringstream oss_r;
    std::ostringstream oss_rs;
    s_r.toJson(oss_r);
    s_rs.toJson(oss_rs);
    BOOST_CHECK_EQUAL(oss_r.str(), oss_rs.str());
}

void testNamespace() {
    ValidSchema s;
    ifstream ifs("jsonschemas/tweet");
    // basic compilation should work
    compileJsonSchema(ifs, s);
    // an AvroPoint was defined and then referred to from within a namespace
    testgen3::AvroPoint point;
    point.latitude = 42.3570;
    point.longitude = -71.1109;
    // set it in something that referred to it in the schema
    testgen3::_tweet_Union__1__ twPoint;
    twPoint.set_AvroPoint(point);
}

void setRecord(uau::r1 &) {
}

void check(const uau::r1 &, const uau::r1 &) {
}

void setRecord(umu::r1 &) {
}

void check(const umu::r1 &, const umu::r1 &) {
}

template<typename T>
struct schemaFilename {};
template<>
struct schemaFilename<uau::r1> {
    static const char value[];
};
const char schemaFilename<uau::r1>::value[] = "jsonschemas/union_array_union";
template<>
struct schemaFilename<umu::r1> {
    static const char value[];
};
const char schemaFilename<umu::r1>::value[] = "jsonschemas/union_map_union";

template<typename T>
void testEncoding2() {
    ValidSchema s;
    ifstream ifs(schemaFilename<T>::value);
    compileJsonSchema(ifs, s);

    unique_ptr<OutputStream> os = memoryOutputStream();
    EncoderPtr e = validatingEncoder(s, binaryEncoder());
    e->init(*os);
    T t1;
    setRecord(t1);
    avro::encode(*e, t1);
    e->flush();

    DecoderPtr d = validatingDecoder(s, binaryDecoder());
    unique_ptr<InputStream> is = memoryInputStream(*os);
    d->init(*is);
    T t2;
    avro::decode(*d, t2);

    check(t2, t1);
}

void testEmptyRecord() {
    uer::StackCalculator calc;
    uer::StackCalculator::stack_item_t item;
    item.set_int(3);
    calc.stack.push_back(item);
    item.set_Dup(uer::Dup());
    calc.stack.push_back(item);
    item.set_Add(uer::Add());
    calc.stack.push_back(item);

    ValidSchema s;
    ifstream ifs("jsonschemas/union_empty_record");
    compileJsonSchema(ifs, s);

    unique_ptr<OutputStream> os = memoryOutputStream();
    EncoderPtr e = validatingEncoder(s, binaryEncoder());
    e->init(*os);
    avro::encode(*e, calc);
    e->flush();

    DecoderPtr d = validatingDecoder(s, binaryDecoder());
    unique_ptr<InputStream> is = memoryInputStream(*os);
    d->init(*is);
    uer::StackCalculator calc2;
    avro::decode(*d, calc2);

    BOOST_CHECK_EQUAL(calc.stack.size(), calc2.stack.size());
    BOOST_CHECK_EQUAL(calc2.stack[0].idx(), 0);
    BOOST_CHECK_EQUAL(calc2.stack[1].idx(), 1);
    BOOST_CHECK_EQUAL(calc2.stack[2].idx(), 2);
}

void testUnionMethods() {
    ValidSchema schema;
    ifstream ifs_w("jsonschemas/bigrecord");
    compileJsonSchema(ifs_w, schema);

    testgen::RootRecord record;
    // initialize the map and set values with getter
    record.myunion.set_map({});
    record.myunion.get_map()["zero"] = 0;
    record.myunion.get_map()["one"] = 1;

    std::vector<uint8_t> bytes{1, 2, 3, 4};
    record.anotherunion.set_bytes(std::move(bytes));
    // after move assignment the local variable should be empty
    BOOST_CHECK(bytes.empty());

    unique_ptr<OutputStream> out_stream = memoryOutputStream();
    EncoderPtr encoder = validatingEncoder(schema, binaryEncoder());
    encoder->init(*out_stream);
    avro::encode(*encoder, record);
    encoder->flush();

    DecoderPtr decoder = validatingDecoder(schema, binaryDecoder());
    unique_ptr<InputStream> is = memoryInputStream(*out_stream);
    decoder->init(*is);
    testgen::RootRecord decoded_record;
    avro::decode(*decoder, decoded_record);

    // check that a reference can be obtained from a union
    BOOST_CHECK(decoded_record.myunion.branch() == testgen::RootRecord::myunion_t::Branch::map);
    const std::map<std::string, int32_t> &read_map = decoded_record.myunion.get_map();
    BOOST_CHECK_EQUAL(read_map.size(), 2);
    BOOST_CHECK_EQUAL(read_map.at("zero"), 0);
    BOOST_CHECK_EQUAL(read_map.at("one"), 1);

    BOOST_CHECK(decoded_record.anotherunion.branch() == testgen::RootRecord::anotherunion_t::Branch::bytes);
    const std::vector<uint8_t> read_bytes = decoded_record.anotherunion.get_bytes();
    const std::vector<uint8_t> expected_bytes{1, 2, 3, 4};
    BOOST_CHECK_EQUAL_COLLECTIONS(read_bytes.begin(), read_bytes.end(), expected_bytes.begin(), expected_bytes.end());
}

void testUnionBranchEnum() {
    big_union::RootRecord record;

    using Branch = big_union::RootRecord::big_union_t::Branch;

    BOOST_CHECK_EQUAL(record.big_union.branch(), Branch::null);
    record.big_union.set_null();
    BOOST_CHECK_EQUAL(record.big_union.branch(), Branch::null);

    record.big_union.set_bool(false);
    BOOST_CHECK_EQUAL(record.big_union.branch(), Branch::bool_);

    record.big_union.set_int(123);
    BOOST_CHECK_EQUAL(record.big_union.branch(), Branch::int_);

    record.big_union.set_long(456);
    BOOST_CHECK_EQUAL(record.big_union.branch(), Branch::long_);

    record.big_union.set_float(555.555f);
    BOOST_CHECK_EQUAL(record.big_union.branch(), Branch::float_);

    record.big_union.set_double(777.777);
    BOOST_CHECK_EQUAL(record.big_union.branch(), Branch::double_);

    record.big_union.set_MD5({});
    BOOST_CHECK_EQUAL(record.big_union.branch(), Branch::MD5);

    record.big_union.set_string("test");
    BOOST_CHECK_EQUAL(record.big_union.branch(), Branch::string);

    record.big_union.set_Vec2({});
    BOOST_CHECK_EQUAL(record.big_union.branch(), Branch::Vec2);

    record.big_union.set_Vec3({});
    BOOST_CHECK_EQUAL(record.big_union.branch(), Branch::Vec3);

    record.big_union.set_Suit(big_union::Suit::CLUBS);
    BOOST_CHECK_EQUAL(record.big_union.branch(), Branch::Suit);

    record.big_union.set_array({});
    BOOST_CHECK_EQUAL(record.big_union.branch(), Branch::array);

    record.big_union.set_map({});
    BOOST_CHECK_EQUAL(record.big_union.branch(), Branch::map);

    record.big_union.set_int_({});
    BOOST_CHECK_EQUAL(record.big_union.branch(), Branch::int__2);

    record.big_union.set_int__({});
    BOOST_CHECK_EQUAL(record.big_union.branch(), Branch::int__);

    record.big_union.set_Int({});
    BOOST_CHECK_EQUAL(record.big_union.branch(), Branch::Int);

    record.big_union.set__Int({});
    BOOST_CHECK_EQUAL(record.big_union.branch(), Branch::_Int);
}

// enable use of BOOST_CHECK_EQUAL
template<>
struct boost::test_tools::tt_detail::print_log_value<std::type_info> {
    void operator()(std::ostream &stream, const std::type_info &type_info) const {
        stream << "std::type_info{.name=" << type_info.name() << "}";
    }
};

void testNoRedundantUnionTypes() {
    redundant_types::RedundantUnionSchema record;
    // ensure only one class is generated for same union
    BOOST_CHECK_EQUAL(typeid(record.null_string_1), typeid(record.null_string_2));
    BOOST_CHECK_EQUAL(typeid(record.string_null_1), typeid(record.string_null_2));
    BOOST_CHECK_EQUAL(typeid(record.null_Empty_1), typeid(record.null_Empty_2));
    BOOST_CHECK_EQUAL(typeid(record.null_namespace_record_1), typeid(record.null_namespace_record_2));
    BOOST_CHECK_EQUAL(typeid(record.null_int_map_1), typeid(record.null_int_map_2));

    // different union types should have different class
    BOOST_CHECK_NE(typeid(record.null_string_1), typeid(record.string_null_1));
    BOOST_CHECK_NE(typeid(record.null_string_1), typeid(record.null_string_int));
    BOOST_CHECK_NE(typeid(record.null_fixed_8), typeid(record.null_fixed_16));
    BOOST_CHECK_NE(typeid(record.null_int_map_1), typeid(record.null_long_map));
}

void testNoRedundantUnionTypesEncodeDecode() {
    redundant_types::RedundantUnionSchema input_record;
    input_record.null_string_1.set_string("null_string_1");
    input_record.null_string_2.set_string("null_string_2");
    input_record.string_null_1.set_string("string_null_1");
    input_record.string_null_2.set_string("string_null_2");
    input_record.null_string_int.set_string("null_string_int");
    input_record.null_Empty_1.set_Empty({});
    input_record.null_Empty_2.set_Empty({});
    input_record.null_namespace_record_1.set_Record({});
    input_record.null_namespace_record_2.set_Record({});
    input_record.null_fixed_8.set_fixed_8({8});
    input_record.null_fixed_16.set_fixed_16({16});
    input_record.fixed_8_fixed_16.set_fixed_16({16});
    input_record.null_int_map_1.set_map({{"null_int_map_1", 1}});
    input_record.null_int_map_2.set_map({{"null_int_map_2", 1}});
    input_record.null_long_map.set_map({{"null_long_map", 1}});

    ValidSchema s;
    ifstream ifs("jsonschemas/union_redundant_types");
    compileJsonSchema(ifs, s);

    unique_ptr<OutputStream> os = memoryOutputStream();
    EncoderPtr e = validatingEncoder(s, binaryEncoder());
    e->init(*os);
    avro::encode(*e, input_record);
    e->flush();

    DecoderPtr d = validatingDecoder(s, binaryDecoder());
    unique_ptr<InputStream> is = memoryInputStream(*os);
    d->init(*is);
    redundant_types::RedundantUnionSchema result_record;
    avro::decode(*d, result_record);

    BOOST_CHECK_EQUAL(result_record.null_string_1.get_string(), "null_string_1");
    BOOST_CHECK_EQUAL(result_record.null_string_2.get_string(), "null_string_2");
    BOOST_CHECK_EQUAL(result_record.string_null_1.get_string(), "string_null_1");
    BOOST_CHECK_EQUAL(result_record.string_null_2.get_string(), "string_null_2");
    BOOST_CHECK_EQUAL(result_record.null_string_int.get_string(), "null_string_int");
    BOOST_CHECK(!result_record.null_Empty_1.is_null());
    BOOST_CHECK(!result_record.null_Empty_2.is_null());
    BOOST_CHECK(!result_record.null_namespace_record_1.is_null());
    BOOST_CHECK(!result_record.null_namespace_record_2.is_null());
    {
        const auto actual = result_record.null_fixed_8.get_fixed_8();
        const std::array<uint8_t, 8> expected{8};
        BOOST_CHECK_EQUAL_COLLECTIONS(actual.begin(), actual.end(), expected.begin(), expected.end());
    }
    {
        const auto actual = result_record.null_fixed_16.get_fixed_16();
        const std::array<uint8_t, 16> expected{16};
        BOOST_CHECK_EQUAL_COLLECTIONS(actual.begin(), actual.end(), expected.begin(), expected.end());
    }
    {
        const auto actual = result_record.fixed_8_fixed_16.get_fixed_16();
        const std::array<uint8_t, 16> expected{16};
        BOOST_CHECK_EQUAL_COLLECTIONS(actual.begin(), actual.end(), expected.begin(), expected.end());
    }
    {
        const auto actual = result_record.null_int_map_1.get_map();
        BOOST_CHECK_EQUAL(actual.size(), 1);
        BOOST_CHECK_EQUAL(actual.at("null_int_map_1"), 1);
    }
    {
        const auto actual = result_record.null_int_map_2.get_map();
        BOOST_CHECK_EQUAL(actual.size(), 1);
        BOOST_CHECK_EQUAL(actual.at("null_int_map_2"), 1);
    }
    {
        const auto actual = result_record.null_long_map.get_map();
        BOOST_CHECK_EQUAL(actual.size(), 1);
        BOOST_CHECK_EQUAL(actual.at("null_long_map"), 1);
    }
}

boost::unit_test::test_suite *init_unit_test_suite(int /*argc*/, char * /*argv*/[]) {
    auto *ts = BOOST_TEST_SUITE("Code generator tests");
    ts->add(BOOST_TEST_CASE(testEncoding));
    ts->add(BOOST_TEST_CASE(testResolution));
    ts->add(BOOST_TEST_CASE(testEncoding2<uau::r1>));
    ts->add(BOOST_TEST_CASE(testEncoding2<umu::r1>));
    ts->add(BOOST_TEST_CASE(testNamespace));
    ts->add(BOOST_TEST_CASE(testEmptyRecord));
    ts->add(BOOST_TEST_CASE(testUnionMethods));
    ts->add(BOOST_TEST_CASE(testUnionBranchEnum));
    ts->add(BOOST_TEST_CASE(testNoRedundantUnionTypes));
    ts->add(BOOST_TEST_CASE(testNoRedundantUnionTypesEncodeDecode));
    return ts;
}
