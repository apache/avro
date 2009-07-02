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

#ifndef avro_Writer_hh__
#define avro_Writer_hh__

#include <boost/noncopyable.hpp>

#include "OutputStreamer.hh"
#include "Zigzag.hh"
#include "Types.hh"

namespace avro {

/// Class for writing avro data to a stream.

class Writer : private boost::noncopyable
{

  public:

    explicit Writer(OutputStreamer &out) :
        out_(out)
    {}

    void putValue(const Null &) {}

    void putValue(bool val) {
        int8_t byte = (val != 0);
        out_.putByte(byte);
    }

    void putValue(int32_t val) {
        boost::array<uint8_t, 5> bytes;
        size_t size = encodeInt32(val, bytes);
        out_.putBytes(bytes.data(), size);
    }

    void putValue(int64_t val) {
        boost::array<uint8_t, 9> bytes;
        size_t size = encodeInt64(val, bytes);
        out_.putBytes(bytes.data(), size);
    }

    void putValue(float val) {
        union {
            float f;
            int32_t i;
        } v;
    
        v.f = val;
        out_.putWord(v.i);
    }

    void putValue(double val) {
        union {
            double d;
            int64_t i;
        } v;
        
        v.d = val;
        out_.putLongWord(v.i);
    }

    void putValue(const std::string &val) {
        putBytes(reinterpret_cast<const uint8_t *>(val.c_str()), val.size());
    }

    void putBytes(const void *val, size_t size) {
        this->putValue(static_cast<int64_t>(size));
        out_.putBytes(reinterpret_cast<const uint8_t *>(val), size);
    }

    void putFixed(const uint8_t *val, size_t size) {
        out_.putBytes(val, size);
    }

    void beginRecord() {}

    void beginArrayBlock(int64_t size) {
        this->putValue(static_cast<int64_t>(size));
    }

    void endArray() {
        out_.putByte(0);
    }

    void beginMapBlock(int64_t size) {
        this->putValue(static_cast<int64_t>(size));
    }

    void endMap() {
        out_.putByte(0);
    }

    void beginUnion(int64_t choice) {
        this->putValue(static_cast<int64_t>(choice));
    }

    void beginEnum(int64_t choice) {
        this->putValue(static_cast<int64_t>(choice));
    }

  private:

    OutputStreamer &out_;

};

} // namespace avro

#endif
