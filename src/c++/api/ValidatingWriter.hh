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

#ifndef avro_ValidatingWriter_hh__
#define avro_ValidatingWriter_hh__

#include <boost/noncopyable.hpp>

#include "Writer.hh"
#include "Validator.hh"
#include "AvroTraits.hh"

namespace avro {

class ValidSchema;
class OutputStreamer;

/// This class walks the parse tree as data is being serialized, and throws if
/// attempt to serialize a data type does not match the type expected in the
/// schema.

class ValidatingWriter : private boost::noncopyable
{

  public:

    ValidatingWriter(const ValidSchema &schema, OutputStreamer &out);

    template<typename T>
    void putValue(T val) {
        checkSafeToPut(type_to_avro<T>::type);
        writer_.putValue(val);
        validator_.advance();
    }

    void putValue(const std::string &val) {
        checkSafeToPut(type_to_avro<std::string>::type);
        writer_.putValue(val);
        validator_.advance();
    }

    void putBytes(const void *val, size_t size);

    void putFixed(const uint8_t *val, size_t size) {
        checkSafeToPut(AVRO_FIXED);
        checkSizeExpected(size);
        writer_.putFixed(val, size);
        validator_.advance();
    }

    void beginRecord();

    void beginArrayBlock(int64_t size);
    void endArray();

    void beginMapBlock(int64_t size);
    void endMap();

    void beginUnion(int64_t choice);

    void beginEnum(int64_t choice);

  private:

    void putCount(int64_t count);

    void checkSafeToPut(Type type) const {
        if(! validator_.typeIsExpected(type)) {
            throw Exception("Type does not match schema");
        }
    }

    void checkSizeExpected(int size) const {
        if(validator_.nextSizeExpected() != size) {
            throw Exception("Wrong size of for fixed");
        }
    }

    Validator validator_;
    Writer writer_;

};

} // namespace avro

#endif
