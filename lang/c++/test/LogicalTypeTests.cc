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

#include "LogicalType.hh"
#include "Compiler.hh"
#include "ValidSchema.hh"

#include <boost/test/included/unit_test_framework.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/test/parameterized_test.hpp>

#include <sstream>
#include <iostream>

namespace avro {
namespace schema {

const char* basicSchemas[] = {
    "{ \"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\" : \"2\" }",
    "{ \"type\": \"fixed\", \"name\": \"Test\", \"logicalType\": \"decimal\", \"precision\" : \"2\", \"size\": 1 }",
};

const char* basicSchemaErrors[] = {
    // incompatible types
    "{ \"type\": \"null\", \"logicalType\": \"decimal\", \"precision\" : \"2\" }",
    "{ \"type\": \"boolean\", \"logicalType\": \"decimal\", \"precision\" : \"2\" }",
    "{ \"type\": \"int\", \"logicalType\": \"decimal\", \"precision\" : \"2\" }",
    "{ \"type\": \"long\", \"logicalType\": \"decimal\", \"precision\" : \"2\" }",
    "{ \"type\": \"float\", \"logicalType\": \"decimal\", \"precision\" : \"2\" }",
    "{ \"type\": \"double\", \"logicalType\": \"decimal\", \"precision\" : \"2\" }",
    // missing required field
    "{ \"type\": \"bytes\", \"logicalType\": \"decimal\" }",
    // invalid required field value
    "{ \"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\" : \"-12\" }",
    // invalid optional field value
    "{ \"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\" : \"2\", \"scale\" : \"-1\" }",
    // unsupported logical type
    "{ \"type\": \"bytes\", \"logicalType\": \"unsupported\" }",
};

const char* pairSchema = "{"
                "\"name\": \"Pair\","
                "\"type\": \"record\","
                "\"fields\": ["
                "    {\"name\": \"x\", \"type\": \"long\"},"
                "    {\"name\": \"y\", \"type\": \"long\"}"
                "  ],"
                "\"logicalType\": \"pair\""
                "}";


class Pair : public LogicalTypeImpl
{
public:
    Pair() : LogicalTypeImpl("pair")
    {
        LogicalTypeImpl::compatibleTypes_.push_back(AVRO_RECORD);
    }

    virtual ~Pair() { }

    void validate(Type type) const
    {
        LogicalTypeImpl::validate(type);
    }
};

class PairFactory : public LogicalTypeFactory
{
public:
    PairFactory() { }
    virtual ~PairFactory() { }

    virtual LogicalTypePtr create()
    {
        std::cout << "creating pair\n";
        return LogicalTypePtr(new Pair());
    }
};

static void testBasic(const char* schema)
{
    BOOST_CHECKPOINT(schema);
    ValidSchema validSchema = compileJsonSchemaFromString(schema);
    LogicalTypePtr logicalType = validSchema.root()->getLogicalType();
    std::vector<std::string> fields;
    logicalType->getRequiredFields(fields);
    BOOST_CHECK_EQUAL(*fields.begin(), "precision");
    BOOST_CHECK_EQUAL(logicalType->getFieldValue("precision"), "2");
    BOOST_CHECK_EQUAL(logicalType->getName(), "decimal");
}

static void testBasic_fail(const char* schema)
{
    BOOST_CHECKPOINT(schema);
    BOOST_CHECK_THROW(compileJsonSchemaFromString(schema), Exception);
}

static void testPrintJson()
{
    ValidSchema validSchema = compileJsonSchemaFromString(basicSchemas[1]);
    std::ostringstream oss;
    validSchema.toJson(oss);
    BOOST_CHECK(oss.str().find("\"logicalType\": \"decimal\",") > 0);
    BOOST_CHECK(oss.str().find("\"precision\": \"2\",") > 0);
}

static void testCreateDecimal()
{
    LogicalTypePtr decimal = logicalTypes.createLogicalType("decimal");
    BOOST_CHECK_EQUAL(decimal->getName(), "decimal");
}

static void testCustomLogicalType()
{
    std::string name = "pair";
    LogicalTypeFactoryPtr factory(new PairFactory());
    logicalTypes.registerNewLogicalType(name, factory);
    ValidSchema validSchema = compileJsonSchemaFromString(pairSchema);
    LogicalTypePtr pair = validSchema.root()->getLogicalType();
    BOOST_CHECK_EQUAL(pair->getName(), name);
}

} //namespace avro
} //namespace schema

#define ENDOF(x)  (x + sizeof(x) / sizeof(x[0]))

#define ADD_PARAM_TEST(ts, func, data) \
    ts->add(BOOST_PARAM_TEST_CASE(&func, data, ENDOF(data)))

boost::unit_test::test_suite*
init_unit_test_suite( int argc, char* argv[] )
{
    using namespace boost::unit_test;

    test_suite* ts= BOOST_TEST_SUITE("Avro C++ unit tests for logical types");

    ADD_PARAM_TEST(ts, avro::schema::testBasic, avro::schema::basicSchemas);
    ADD_PARAM_TEST(ts, avro::schema::testBasic_fail, avro::schema::basicSchemaErrors);
    ts->add(BOOST_TEST_CASE(&avro::schema::testPrintJson));
    ts->add(BOOST_TEST_CASE(&avro::schema::testCreateDecimal));
    ts->add(BOOST_TEST_CASE(&avro::schema::testCustomLogicalType));
    return ts;
}
