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

#include "ValidatingWriter.hh"
#include "ValidSchema.hh"
#include "OutputStreamer.hh"
#include "AvroTraits.hh"

namespace avro {

ValidatingWriter::ValidatingWriter(const ValidSchema &schema, OutputStreamer &out) :
    validator_(schema),
    writer_(out)
{ }

void
ValidatingWriter::writeBytes(const void *val, size_t size)
{
    checkSafeToPut(AVRO_BYTES);
    writer_.writeBytes(val, size);
    validator_.advance();
}

void 
ValidatingWriter::writeCount(int64_t count)
{
    checkSafeToPut(AVRO_LONG);
    writer_.writeValue(count);
    validator_.advanceWithCount(count);
}

void 
ValidatingWriter::writeRecord()
{
    checkSafeToPut(AVRO_RECORD);
    validator_.advance();
}

void 
ValidatingWriter::writeArrayBlock(int64_t size)
{
    checkSafeToPut(AVRO_ARRAY);
    validator_.advance();
    writeCount(size);
}

void 
ValidatingWriter::writeArrayEnd()
{
    writeArrayBlock(0);
}

void 
ValidatingWriter::writeMapBlock(int64_t size)
{
    checkSafeToPut(AVRO_MAP);
    validator_.advance();
    writeCount(size);
}

void 
ValidatingWriter::writeMapEnd()
{
    writeMapBlock(0);
}

void 
ValidatingWriter::writeUnion(int64_t choice)
{
    checkSafeToPut(AVRO_UNION);
    validator_.advance();
    writeCount(choice);
}

void 
ValidatingWriter::writeEnum(int64_t choice)
{
    checkSafeToPut(AVRO_ENUM);
    validator_.advance();
    writeCount(choice);
}


} // namespace avro
