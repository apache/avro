/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <string.h>
#include <fstream>
#include <sstream>
#include <boost/test/included/unit_test_framework.hpp>

#include "code.hh"
#include "OutputStreamer.hh"
#include "InputStreamer.hh"
#include "Serializer.hh"
#include "Writer.hh"
#include "ValidatingWriter.hh"
#include "Reader.hh"
#include "ValidatingReader.hh"
#include "Node.hh"
#include "ValidSchema.hh"
#include "Compiler.hh"

std::string gInputSchema ("jsonschemas/bigrecord");

void serializeToScreen(const testgen::RootRecord &rec) 
{
    avro::ScreenStreamer os;
    avro::Writer writer(os);

    avro::serialize(writer, rec);
}

void serializeToScreenValid(const avro::ValidSchema &valid, const testgen::RootRecord &rec) 
{
    avro::ScreenStreamer os;
    avro::ValidatingWriter writer(valid, os);

    avro::serialize(writer, rec);
}

void checkArray(const testgen::Array_of_double &a1, const testgen::Array_of_double &a2) 
{
    BOOST_CHECK_EQUAL(a1.value.size(), 3U);
    BOOST_CHECK_EQUAL(a1.value.size(), a2.value.size());
    for(size_t i = 0; i < a1.value.size(); ++i) {
        BOOST_CHECK_EQUAL(a1.value[i], a2.value[i]);
    }
}

void checkMap(const testgen::Map_of_int &map1, const testgen::Map_of_int &map2) 
{
    BOOST_CHECK_EQUAL(map1.value.size(), map2.value.size());
    testgen::Map_of_int::MapType::const_iterator iter1 = map1.value.begin();
    testgen::Map_of_int::MapType::const_iterator end   = map1.value.end();
    testgen::Map_of_int::MapType::const_iterator iter2 = map2.value.begin();

    while(iter1 != end) {
        BOOST_CHECK_EQUAL(iter1->first, iter2->first);
        BOOST_CHECK_EQUAL(iter1->second, iter2->second);
        ++iter1;
        ++iter2;
    }
}

void checkBytes(const std::vector<uint8_t> &v1, const std::vector<uint8_t> &v2)
{
    BOOST_CHECK_EQUAL(v1.size(), 2U);
    BOOST_CHECK_EQUAL(v1.size(), v2.size());
    for(size_t i = 0; i < v1.size(); ++i) {
        BOOST_CHECK_EQUAL(v1[i], v2[i]);
    }
}

void checkNested(const testgen::Nested &rec1, const testgen::Nested &rec2)
{
    BOOST_CHECK_EQUAL(rec1.inval1, rec2.inval1);
    BOOST_CHECK_EQUAL(rec1.inval2, rec2.inval2);
    BOOST_CHECK_EQUAL(rec1.inval3, rec2.inval3);
}

void checkOk(const testgen::RootRecord &rec1, const testgen::RootRecord &rec2)
{
    BOOST_CHECK_EQUAL(rec1.mylong, rec1.mylong);

    checkNested(rec1.nestedrecord, rec2.nestedrecord);
    checkMap(rec1.mymap, rec2.mymap);
    checkArray(rec1.myarray, rec2.myarray);

    BOOST_CHECK_EQUAL(rec1.myenum.value, rec2.myenum.value);

    BOOST_CHECK_EQUAL(rec1.myunion.choice, rec2.myunion.choice);
    // in this test I know choice was 1
    {
        BOOST_CHECK_EQUAL(rec1.myunion.choice, 1);
        checkMap(rec1.myunion.getValue<testgen::Map_of_int>(), rec2.myunion.getValue<testgen::Map_of_int>());
    }

    BOOST_CHECK_EQUAL(rec1.anotherunion.choice, rec2.anotherunion.choice);
    // in this test I know choice was 0
    {
        BOOST_CHECK_EQUAL(rec1.anotherunion.choice, 0);
        typedef std::vector<uint8_t> mytype;
        checkBytes(rec1.anotherunion.getValue<mytype>(), rec2.anotherunion.getValue<testgen::Union_of_bytes_null::T0>());
    }

    checkNested(rec1.anothernested, rec2.anothernested);

    BOOST_CHECK_EQUAL(rec1.mybool, rec2.mybool);

    for(int i = 0; i < static_cast<int>(testgen::md5::fixedSize); ++i) {
        BOOST_CHECK_EQUAL(rec1.myfixed.value[i], rec2.myfixed.value[i]);
    }
    BOOST_CHECK_EQUAL(rec1.anotherint, rec1.anotherint);
}

void testParser(const testgen::RootRecord &myRecord)
{
    std::ostringstream ostring;
    avro::OStreamer os(ostring);
    avro::Writer s (os);

    avro::serialize(s, myRecord); 

    testgen::RootRecord inRecord;
    std::istringstream istring(ostring.str());
    avro::IStreamer is(istring);
    avro::Reader p(is);
    avro::parse(p, inRecord);

    checkOk(myRecord, inRecord);
}

void testParserValid(avro::ValidSchema &valid, const testgen::RootRecord &myRecord)
{
    std::ostringstream ostring;
    avro::OStreamer os(ostring);
    avro::ValidatingWriter s (valid, os);

    avro::serialize(s, myRecord);

    testgen::RootRecord inRecord;
    std::istringstream istring(ostring.str());
    avro::IStreamer is(istring);
    avro::ValidatingReader p(valid, is);
    avro::parse(p, inRecord);

    checkOk(myRecord, inRecord);
}

void testNameIndex(const avro::ValidSchema &schema)
{
    const avro::NodePtr &node = schema.root();
    size_t index = 0;
    bool found = node->nameIndex("anothernested", index);
    BOOST_CHECK_EQUAL(found, true);
    BOOST_CHECK_EQUAL(index, 8U);

    found = node->nameIndex("myenum", index);
    BOOST_CHECK_EQUAL(found, true);
    BOOST_CHECK_EQUAL(index, 4U);

    const avro::NodePtr &enumNode = node->leafAt(index);
    found = enumNode->nameIndex("one", index); 
    BOOST_CHECK_EQUAL(found, true);
    BOOST_CHECK_EQUAL(index, 1U);
}

void runTests(const testgen::RootRecord myRecord) 
{
    std::cout << "Serialize:\n";
    serializeToScreen(myRecord);
    std::cout << "end Serialize\n";

    avro::ValidSchema schema;
    std::ifstream in(gInputSchema.c_str());
    avro::compileJsonSchema(in, schema);
    std::cout << "Serialize validated:\n";
    serializeToScreenValid(schema, myRecord);
    std::cout << "end Serialize validated\n";

    testNameIndex(schema);

    testParser(myRecord);

    testParserValid(schema, myRecord);
}

void testGen() 
{
    uint8_t fixed[] =  {0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

    testgen::RootRecord myRecord;
    myRecord.mylong = 212;
    myRecord.nestedrecord.inval1 = std::numeric_limits<double>::min();
    myRecord.nestedrecord.inval2 = "hello world";
    myRecord.nestedrecord.inval3 = std::numeric_limits<int32_t>::max();
    myRecord.mymap.value.clear();
    myRecord.myarray.addValue(3434.9);
    myRecord.myarray.addValue(7343.9);
    myRecord.myarray.addValue(-63445.9);
    myRecord.myenum.value = testgen::ExampleEnum::one;
    testgen::Map_of_int map;
    map.addValue("one", 1);
    map.addValue("two", 2);
    myRecord.myunion.set_Map_of_int(map);
    std::vector<uint8_t> vec;
    vec.push_back(1);
    vec.push_back(2);
    myRecord.anotherunion.set_bytes(vec);
    myRecord.mybool = true;
    myRecord.anothernested.inval1 = std::numeric_limits<double>::max();
    myRecord.anothernested.inval2 = "goodbye world";
    myRecord.anothernested.inval3 = std::numeric_limits<int32_t>::min();
    memcpy(myRecord.myfixed.value, fixed, testgen::md5::fixedSize);
    myRecord.anotherint = 4534;
    myRecord.bytes.push_back(10);
    myRecord.bytes.push_back(20);

    runTests(myRecord);

    std::cout << "Finished code generation tests\n";
}

boost::unit_test::test_suite*
init_unit_test_suite( int argc, char* argv[] ) 
{
    using namespace boost::unit_test;


    if(argc > 1) {
        gInputSchema = argv[1];
    }

    std::cout << "Using schema " << gInputSchema << std::endl;

    test_suite* test= BOOST_TEST_SUITE( "Avro C++ generated code test suite" );
    test->add( BOOST_TEST_CASE( &testGen ) );

    return test;
}
