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

#ifndef avro_Reader_hh__
#define avro_Reader_hh__

#include <stdint.h>
#include <vector>
#include <boost/noncopyable.hpp>

#include "InputStreamer.hh"
#include "Zigzag.hh"
#include "Types.hh"

namespace avro {

///
/// Parses from an avro encoding to the requested type.  Assumes the next item
/// in the avro binary data is the expected type.
///

class Reader : private boost::noncopyable
{

  public:

    explicit Reader(InputStreamer &in) :
        in_(in)
    {}

    void readValue(Null &) {}

    void readValue(bool &val) {
        uint8_t ival;
        in_.readByte(ival);
        val = (ival != 0);
    }

    void readValue(int32_t &val) {
        uint32_t encoded = readVarInt();
        val = decodeZigzag32(encoded);
    }

    void readValue(int64_t &val) {
        uint64_t encoded = readVarInt();
        val = decodeZigzag64(encoded);
    }

    void readValue(float &val) {
        union { 
            float f;
            uint32_t i;
        } v;
        in_.readWord(v.i);
        val = v.f;
    }

    void readValue(double &val) {
        union { 
            double d;
            uint64_t i;
        } v;
        in_.readLongWord(v.i);
        val = v.d;
    }

    void readValue(std::string &val) {
        int64_t size = readSize();
        val.reserve(size);
        uint8_t bval;
        for(size_t bytes = 0; bytes < static_cast<size_t>(size); bytes++) {
            in_.readByte(bval);
            val.push_back(bval);
        }
    }

    void readBytes(std::vector<uint8_t> &val) {
        int64_t size = readSize();
        
        val.reserve(size);
        uint8_t bval;
        for(size_t bytes = 0; bytes < static_cast<size_t>(size); bytes++) {
            in_.readByte(bval);
            val.push_back(bval);
        }
    }


    void readFixed(std::vector<uint8_t> &val, size_t size) {
        val.reserve(size);
        uint8_t bval;
        for(size_t bytes = 0; bytes < size; bytes++) {
            in_.readByte(bval);
            val.push_back(bval);
        }
    }

    void readFixed(uint8_t *val, size_t size) {
        uint8_t bval;
        for(size_t bytes = 0; bytes < size; bytes++) {
            in_.readByte(bval);
            *val++ = bval;
        }
    }

    void readRecord() { }

    int64_t readArrayBlockSize() {
        return readSize();
    }

    int64_t readUnion() { 
        return readSize();
    }

    int64_t readEnum() {
        return readSize();
    }

    int64_t readMapBlockSize() {
        return readSize();
    }

  private:

    int64_t readSize() {
        int64_t size(0);
        readValue(size);
        return size;
    }

    uint64_t readVarInt() {
        uint64_t encoded = 0;
        uint8_t val = 0;
        int shift = 0;
        do {
            in_.readByte(val);
            uint64_t newbits = (val & 0x7f) << shift;
            encoded |= newbits;
            shift += 7;
        } while (val & 0x80);

        return encoded;
    }

    InputStreamer &in_;

};


} // namespace avro

#endif
