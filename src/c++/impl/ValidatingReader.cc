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

#include <boost/static_assert.hpp>

#include "ValidatingReader.hh"
#include "ValidSchema.hh"
#include "OutputStreamer.hh"

namespace avro {

ValidatingReader::ValidatingReader(const ValidSchema &schema, InputStreamer &in) :
    validator_(schema),
    reader_(in)
{ }

int64_t
ValidatingReader::getCount()
{
    checkSafeToGet(AVRO_LONG);
    int64_t val;
    reader_.getValue(val);
    validator_.advanceWithCount(val);
    return val;
}

void 
ValidatingReader::getRecord()
{
    checkSafeToGet(AVRO_RECORD);
    validator_.advance();
}

int64_t 
ValidatingReader::getUnion()
{
    checkSafeToGet(AVRO_UNION);
    validator_.advance();
    return getCount();
}

int64_t 
ValidatingReader::getEnum()
{
    checkSafeToGet(AVRO_ENUM);
    validator_.advance();
    return getCount();
}

int64_t 
ValidatingReader::getMapBlockSize()
{
    checkSafeToGet(AVRO_MAP);
    validator_.advance();
    return getCount();
}

int64_t 
ValidatingReader::getArrayBlockSize()
{
    checkSafeToGet(AVRO_ARRAY);
    validator_.advance();
    return getCount();
}

} // namespace avro
