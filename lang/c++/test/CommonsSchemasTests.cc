/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <boost/test/included/unit_test.hpp>
#include <boost/test/unit_test.hpp>
#include <filesystem>
#include "DataFile.hh"
#include "Compiler.hh"
#include "ValidSchema.hh"
#include "Generic.hh"


using avro::validatingDecoder;
using avro::GenericReader;
using avro::DataFileReader;
using avro::DataFileWriter;
using avro::GenericDatum;


void testCommonSchema(const std::filesystem::path &dir_path)
{
	const std::filesystem::path& schemaFile = dir_path / "schema.json";
	std::ifstream in(schemaFile.c_str());

	avro::ValidSchema schema;
	avro::compileJsonSchema(in, schema);

	const std::filesystem::path& dataFile = dir_path / "data.avro";


	GenericDatum datum(schema);
	const std::filesystem::path& outputDataFile = dir_path / "data_out.avro";


	DataFileReader<GenericDatum> reader(dataFile.c_str());
	DataFileWriter<GenericDatum> writer(outputDataFile.c_str(), schema);

	while (reader.read(datum)) {
        avro::GenericRecord& rec =  datum.value<avro::GenericRecord>();
        BOOST_CHECK(rec.fieldCount() >= 0);
        writer.write(datum);
	}
	writer.close();
	reader.close();

	GenericDatum datumOrig(schema);
	GenericDatum datumNew(schema);

	DataFileReader<GenericDatum> readerOrig(dataFile.c_str());
	DataFileReader<GenericDatum> readerNew(outputDataFile.c_str());
	while (readerOrig.read(datumOrig)) {
	    BOOST_CHECK(readerNew.read(datumNew));
        avro::GenericRecord& rec1 =  datumOrig.value<avro::GenericRecord>();
        avro::GenericRecord& rec2 =  datumNew.value<avro::GenericRecord>();
        BOOST_CHECK_EQUAL(rec1.fieldCount(), rec2.fieldCount());
	}
	BOOST_CHECK(!readerNew.read(datumNew));


	std::filesystem::remove(outputDataFile);
}



void testCommonsSchemas()
{
	const std::filesystem::path commons_schemas{"../../share/test/data/schemas"};
	if (!std::filesystem::exists(commons_schemas)) {
        std::cout << "\nWarn: Can't access share test folder '../../share/test/data/schemas'\n" << std::endl;
        return;
	}
	for (auto const& dir_entry : std::filesystem::directory_iterator{commons_schemas}) {
        if (std::filesystem::is_directory(dir_entry)) {
		    testCommonSchema(dir_entry.path());
        }
	}
}

boost::unit_test::test_suite *
init_unit_test_suite(int /*argc*/, char * /*argv*/[]) {
    using namespace boost::unit_test;

    auto *ts = BOOST_TEST_SUITE("Avro C++ unit tests for commons schemas");
    ts->add(BOOST_TEST_CASE(&testCommonsSchemas));
    return ts;
}
