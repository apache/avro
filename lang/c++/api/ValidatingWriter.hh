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

/// This class walks the parse tree as data is being serialized, and throws if
/// attempt to serialize a data type does not match the type expected in the
/// schema.

class ValidatingWriter : private boost::noncopyable
{

  public:

    ValidatingWriter(const ValidSchema &schema);

    template<typename T>
    void writeValue(T val) {
        checkSafeToPut(type_to_avro<T>::type);
        writer_.writeValue(val);
        validator_.advance();
    }

    void writeValue(const std::string &val) {
        checkSafeToPut(type_to_avro<std::string>::type);
        writer_.writeValue(val);
        validator_.advance();
    }

    void writeBytes(const void *val, size_t size);

    template <size_t N>
    void writeFixed(const uint8_t (&val)[N]) {
        checkSafeToPut(AVRO_FIXED);
        checkSizeExpected(N);
        writer_.writeFixed(val);
        validator_.advance();
    }

    template <size_t N>
    void writeFixed(const boost::array<uint8_t, N> &val) {
        checkSafeToPut(AVRO_FIXED);
        checkSizeExpected(val.size());
        writer_.writeFixed(val);
        validator_.advance();
    }

    void writeRecord();

    void writeArrayBlock(int64_t size);
    void writeArrayEnd();

    void writeMapBlock(int64_t size);
    void writeMapEnd();

    void writeUnion(int64_t choice);

    void writeEnum(int64_t choice);

    InputBuffer buffer() const {
        return writer_.buffer();
    }

  private:

    void writeCount(int64_t count);

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
