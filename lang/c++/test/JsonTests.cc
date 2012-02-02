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


#include <limits>

#include <boost/test/included/unit_test_framework.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/test/parameterized_test.hpp>

#include "../impl/json/JsonDom.hh"

#define S(x) #x

namespace avro {
namespace json {

template <typename T>
struct TestData {
    const char *input;
    EntityType type;
    T value;
};

TestData<bool> boolData[] = {
    { "true", etBool, true },
    { "false", etBool, false },
};

TestData<int64_t> longData[] = {
    { "0", etLong, 0 },
    { "-1", etLong, -1 },
    { "1", etLong, 1 },
    { "9223372036854775807", etLong, 9223372036854775807LL },
    { "-9223372036854775807", etLong, -9223372036854775807LL },
};

TestData<double> doubleData[] = {
    { "0.0", etDouble, 0.0 },
    { "-1.0", etDouble, -1.0 },
    { "1.0", etDouble, 1.0 },
    { "4.7e3", etDouble, 4700.0 },
    { "-7.2e-4", etDouble, -0.00072 },
};

TestData<const char*> stringData[] = {
    { "\"\"", etString, "" },
    { "\"a\"", etString, "a" },
    { "\"\\\"\"", etString, "\"" },
    { "\"\\/\"", etString, "/" },
};

template <typename T>
void testPrimitive(const TestData<T>& d)
{
    Entity n = loadEntity(d.input);
    BOOST_CHECK_EQUAL(n.type(), d.type);
    BOOST_CHECK_EQUAL(n.value<T>(), d.value);
}

void testDouble(const TestData<double>& d)
{
    Entity n = loadEntity(d.input);
    BOOST_CHECK_EQUAL(n.type(), d.type);
    BOOST_CHECK_CLOSE(n.value<double>(), d.value, 1e-10);
}

void testString(const TestData<const char*>& d)
{
    Entity n = loadEntity(d.input);
    BOOST_CHECK_EQUAL(n.type(), d.type);
    BOOST_CHECK_EQUAL(n.value<std::string>(), d.value);
}

static void testNull()
{
    Entity n = loadEntity("null");
    BOOST_CHECK_EQUAL(n.type(), etNull);

}

static void testArray0()
{
    Entity n = loadEntity("[]");
    BOOST_CHECK_EQUAL(n.type(), etArray);
    const std::vector<Entity>& a = n.value<std::vector<Entity> >();
    BOOST_CHECK_EQUAL(a.size(), 0);
}

static void testArray1()
{
    Entity n = loadEntity("[200]");
    BOOST_CHECK_EQUAL(n.type(), etArray);
    const std::vector<Entity>& a = n.value<std::vector<Entity> >();
    BOOST_CHECK_EQUAL(a.size(), 1);
    BOOST_CHECK_EQUAL(a[0].type(), etLong);
    BOOST_CHECK_EQUAL(a[0].value<int64_t>(), 200ll);
}

static void testArray2()
{
    Entity n = loadEntity("[200, \"v100\"]");
    BOOST_CHECK_EQUAL(n.type(), etArray);
    const std::vector<Entity>& a = n.value<std::vector<Entity> >();
    BOOST_CHECK_EQUAL(a.size(), 2);
    BOOST_CHECK_EQUAL(a[0].type(), etLong);
    BOOST_CHECK_EQUAL(a[0].value<int64_t>(), 200ll);
    BOOST_CHECK_EQUAL(a[1].type(), etString);
    BOOST_CHECK_EQUAL(a[1].value<std::string>(), "v100");
}

static void testObject0()
{
    Entity n = loadEntity("{}");
    BOOST_CHECK_EQUAL(n.type(), etObject);
    const std::map<std::string, Entity>& m =
        n.value<std::map<std::string, Entity> >();
    BOOST_CHECK_EQUAL(m.size(), 0);
}

static void testObject1()
{
    Entity n = loadEntity("{\"k1\": 100}");
    BOOST_CHECK_EQUAL(n.type(), etObject);
    const std::map<std::string, Entity>& m =
        n.value<std::map<std::string, Entity> >();
    BOOST_CHECK_EQUAL(m.size(), 1);
    BOOST_CHECK_EQUAL(m.begin()->first, "k1");
    BOOST_CHECK_EQUAL(m.begin()->second.type(), etLong);
    BOOST_CHECK_EQUAL(m.begin()->second.value<int64_t>(), 100ll);
}

static void testObject2()
{
    Entity n = loadEntity("{\"k1\": 100, \"k2\": [400, \"v0\"]}");
    BOOST_CHECK_EQUAL(n.type(), etObject);
    const std::map<std::string, Entity>& m =
        n.value<std::map<std::string, Entity> >();
    BOOST_CHECK_EQUAL(m.size(), 2);

    std::map<std::string, Entity>::const_iterator it = m.find("k1");
    BOOST_CHECK(it != m.end());
    BOOST_CHECK_EQUAL(it->second.type(), etLong);
    BOOST_CHECK_EQUAL(m.begin()->second.value<int64_t>(), 100ll);

    it = m.find("k2");
    BOOST_CHECK(it != m.end());
    BOOST_CHECK_EQUAL(it->second.type(), etArray);
    const std::vector<Entity>& a = it->second.value<std::vector<Entity> >();
    BOOST_CHECK_EQUAL(a.size(), 2);
    BOOST_CHECK_EQUAL(a[0].type(), etLong);
    BOOST_CHECK_EQUAL(a[0].value<int64_t>(), 400ll);
    BOOST_CHECK_EQUAL(a[1].type(), etString);
    BOOST_CHECK_EQUAL(a[1].value<std::string>(), "v0");
}

}
}

#define COUNTOF(x)  (sizeof(x) / sizeof(x[0]))

boost::unit_test::test_suite*
init_unit_test_suite( int argc, char* argv[] ) 
{
    using namespace boost::unit_test;

    test_suite* ts= BOOST_TEST_SUITE("Avro C++ unit tests for json routines");

    ts->add(BOOST_TEST_CASE(&avro::json::testNull));
    ts->add(BOOST_PARAM_TEST_CASE(&avro::json::testPrimitive<bool>,
        avro::json::boolData,
        avro::json::boolData + COUNTOF(avro::json::boolData)));
    ts->add(BOOST_PARAM_TEST_CASE(&avro::json::testPrimitive<int64_t>,
        avro::json::longData,
        avro::json::longData + COUNTOF(avro::json::longData)));
    ts->add(BOOST_PARAM_TEST_CASE(&avro::json::testDouble,
        avro::json::doubleData,
        avro::json::doubleData + COUNTOF(avro::json::doubleData)));
    ts->add(BOOST_PARAM_TEST_CASE(&avro::json::testString,
        avro::json::stringData,
        avro::json::stringData + COUNTOF(avro::json::stringData)));

    ts->add(BOOST_TEST_CASE(&avro::json::testArray0));
    ts->add(BOOST_TEST_CASE(&avro::json::testArray1));
    ts->add(BOOST_TEST_CASE(&avro::json::testArray2));

    ts->add(BOOST_TEST_CASE(&avro::json::testObject0));
    ts->add(BOOST_TEST_CASE(&avro::json::testObject1));
    ts->add(BOOST_TEST_CASE(&avro::json::testObject2));

    return ts;
}
