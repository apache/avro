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

// Layout.hh must be self-contained: include it before anything else.
#include "Layout.hh"

#include <boost/test/included/unit_test.hpp>

namespace avro {

void testCompoundLayout() {
    CompoundLayout compound(8);
    std::unique_ptr<Layout> child = std::make_unique<PrimitiveLayout>(16);
    compound.add(child);
    BOOST_CHECK_EQUAL(compound.offset(), 8U);
    BOOST_CHECK_EQUAL(compound.at(0).offset(), 16U);
}

} // namespace avro

boost::unit_test::test_suite *
init_unit_test_suite(int /*argc*/, char * /*argv*/[]) {
    using namespace boost::unit_test;

    auto *ts = BOOST_TEST_SUITE("Avro C++ unit tests for Layout");
    ts->add(BOOST_TEST_CASE(&avro::testCompoundLayout));
    return ts;
}
