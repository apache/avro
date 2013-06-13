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

#include "Compiler.hh"
#include "ValidSchema.hh"

#include <boost/test/included/unit_test_framework.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/test/parameterized_test.hpp>


namespace avro {
namespace schema {

const char* basicSchemas[] = {
    "\"null\"",
    "\"boolean\"",
    "\"int\"",
    "\"long\"",
    "\"float\"",
    "\"double\"",
    "\"bytes\"",
    "\"string\"",

    // Primitive types - longer
    "{ \"type\": \"null\" }",
    "{ \"type\": \"boolean\" }",
    "{ \"type\": \"int\" }",
    "{ \"type\": \"long\" }",
    "{ \"type\": \"float\" }",
    "{ \"type\": \"double\" }",
    "{ \"type\": \"bytes\" }",
    "{ \"type\": \"string\" }",

    // Record
    "{\"type\": \"record\",\"name\": \"Test\",\"fields\": "
        "[{\"name\": \"f\",\"type\": \"long\"}]}",
    "{\"type\": \"record\",\"name\": \"Test\",\"fields\": "
        "[{\"name\": \"f1\",\"type\": \"long\"},"
        "{\"name\": \"f2\", \"type\": \"int\"}]}",
    "{\"type\": \"error\",\"name\": \"Test\",\"fields\": "
        "[{\"name\": \"f1\",\"type\": \"long\"},"
        "{\"name\": \"f2\", \"type\": \"int\"}]}",

    // Recursive.
    "{\"type\":\"record\",\"name\":\"LongList\","
        "\"fields\":[{\"name\":\"value\",\"type\":\"long\"},"
        "{\"name\":\"next\",\"type\":[\"LongList\",\"null\"]}]}",
    // Enum
    "{\"type\": \"enum\", \"name\": \"Test\", \"symbols\": [\"A\", \"B\"]}",

    // Array
    "{\"type\": \"array\", \"items\": \"long\"}",
    "{\"type\": \"array\",\"items\": {\"type\": \"enum\", "
        "\"name\": \"Test\", \"symbols\": [\"A\", \"B\"]}}",

    // Map
    "{\"type\": \"map\", \"values\": \"long\"}",
    "{\"type\": \"map\",\"values\": {\"type\": \"enum\", "
        "\"name\": \"Test\", \"symbols\": [\"A\", \"B\"]}}",

    // Union
    "[\"string\", \"null\", \"long\"]",

    // Fixed
    "{ \"type\": \"fixed\", \"name\": \"Test\", \"size\": 1}",
    "{\"type\": \"fixed\", \"name\": \"MyFixed\", "
        "\"namespace\": \"org.apache.hadoop.avro\", \"size\": 1}",
    "{ \"type\": \"fixed\", \"name\": \"Test\", \"size\": 1}",
    "{ \"type\": \"fixed\", \"name\": \"Test\", \"size\": 1}",
};

const char* basicSchemaErrors[] = {
    // Record
    // No fields
    "{\"type\":\"record\",\"name\":\"LongList\"}",
    // Fields not an array
    "{\"type\":\"record\",\"name\":\"LongList\", \"fields\": \"hi\"}",

    // Undefined name
    "{\"type\":\"record\",\"name\":\"LongList\","
        "\"fields\":[{\"name\":\"value\",\"type\":\"long\"},"
        "{\"name\":\"next\",\"type\":[\"LongListA\",\"null\"]}]}",

    // Enum
    // Symbols not an array
    "{\"type\": \"enum\", \"name\": \"Status\", \"symbols\": "
        "\"Normal Caution Critical\"}",
    // Name not a string
    "{\"type\": \"enum\", \"name\": [ 0, 1, 1, 2, 3, 5, 8 ], "
        "\"symbols\": [\"Golden\", \"Mean\"]}",
    // No name
    "{\"type\": \"enum\", \"symbols\" : [\"I\", \"will\", "
        "\"fail\", \"no\", \"name\"]}",
    // Duplicate symbol
    "{\"type\": \"enum\", \"name\": \"Test\","
        "\"symbols\" : [\"AA\", \"AA\"]}",

    // Union
    // Duplicate type
    "[\"string\", \"long\", \"long\"]",
    // Duplicate type
    "[{\"type\": \"array\", \"items\": \"long\"}, "
        "{\"type\": \"array\", \"items\": \"string\"}]",
        
    // Fixed
    // No size
    "{\"type\": \"fixed\", \"name\": \"Missing size\"}",
    // No name
    "{\"type\": \"fixed\", \"size\": 314}",
};

static void testBasic(const char* schema)
{
    BOOST_CHECKPOINT(schema);
    compileJsonSchemaFromString(schema);
}

static void testBasic_fail(const char* schema)
{
    BOOST_CHECKPOINT(schema);
    BOOST_CHECK_THROW(compileJsonSchemaFromString(schema), Exception);
}

static void testCompile(const char* schema)
{
    BOOST_CHECKPOINT(schema);
    compileJsonSchemaFromString(std::string(schema));
}

}
}

#define ENDOF(x)  (x + sizeof(x) / sizeof(x[0]))

#define ADD_PARAM_TEST(ts, func, data) \
    ts->add(BOOST_PARAM_TEST_CASE(&func, data, ENDOF(data)))
    

boost::unit_test::test_suite*
init_unit_test_suite(int argc, char* argv[]) 
{
    using namespace boost::unit_test;

    test_suite* ts= BOOST_TEST_SUITE("Avro C++ unit tests for schemas");
    ADD_PARAM_TEST(ts, avro::schema::testBasic, avro::schema::basicSchemas);
    ADD_PARAM_TEST(ts, avro::schema::testBasic_fail,
        avro::schema::basicSchemaErrors);
    ADD_PARAM_TEST(ts, avro::schema::testCompile, avro::schema::basicSchemas);

    return ts;
}
