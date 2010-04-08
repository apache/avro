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

#ifndef avro_ValidatingReader_hh__
#define avro_ValidatingReader_hh__

#include <stdint.h>
#include <vector>
#include <boost/noncopyable.hpp>

#include "Reader.hh"
#include "Validator.hh"
#include "AvroTraits.hh"

namespace avro {

class ValidSchema;

/// As an avro object is being parsed from binary data to its C++
/// representation, this parser will walk the parse tree and ensure that the
/// correct type is being asked for.  If the user attempts to parse a type that
/// does not match what the schema says, an exception will be thrown.  
///
/// The ValidatingReader object can also be used to tell what the next type is,
/// so that callers can dynamically discover the contents.  It also tells
/// the attribute names of the objects or their fields, if they exist.
///

class ValidatingReader : private boost::noncopyable
{

  public:

    ValidatingReader(const ValidSchema &schema, const InputBuffer &in);

    template<typename T>
    void readValue(T &val) {
        checkSafeToGet(type_to_avro<T>::type);
        reader_.readValue(val);
        validator_.advance();
    }

    void readBytes(std::vector<uint8_t> &val) {
        checkSafeToGet(AVRO_BYTES);
        validator_.advance();
        reader_.readBytes(val);
    }

    void readFixed(uint8_t *val, size_t size) {
        checkSafeToGet(AVRO_FIXED);
        checkSizeExpected(size);
        validator_.advance();
        reader_.readFixed(val, size);
    }

    template <size_t N>
    void readFixed(uint8_t (&val)[N]) {
        readFixed(val, N);
    }

    template<size_t N>
    void readFixed(boost::array<uint8_t, N> &val) {
        readFixed(val.c_array(), N);
    }

    void readRecord();

    int64_t readArrayBlockSize();

    int64_t readUnion();

    int64_t readEnum();

    int64_t readMapBlockSize();

    Type nextType() const{
        return validator_.nextTypeExpected();
    }

    bool currentRecordName(std::string &name) const {
        return validator_.getCurrentRecordName(name);
    }

    bool nextFieldName(std::string &name) const {
        return validator_.getNextFieldName(name);
    }

  private:

    int64_t readCount();

    void checkSafeToGet(Type type) const {
        if(validator_.nextTypeExpected() != type) {
            throw Exception("Type does not match");
        }
    }

    void checkSizeExpected(int size) const {
        if(validator_.nextSizeExpected() != size) {
            throw Exception("Wrong size of for fixed");
        }
    }

    Validator validator_;
    Reader reader_;
};

} // namespace avro

#endif
