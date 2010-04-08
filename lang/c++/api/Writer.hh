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

#include "buffer/Buffer.hh"
#include "Zigzag.hh"
#include "Types.hh"

namespace avro {

/// Class for writing avro data to a stream.

class Writer : private boost::noncopyable
{

  public:

    Writer() {}

    void writeValue(const Null &) {}

    void writeValue(bool val) {
        int8_t byte = (val != 0);
        buffer_.writeTo(byte);
    }

    void writeValue(int32_t val) {
        boost::array<uint8_t, 5> bytes;
        size_t size = encodeInt32(val, bytes);
        buffer_.writeTo(reinterpret_cast<const char *>(bytes.data()), size);
    }

    void writeValue(int64_t val) {
        boost::array<uint8_t, 10> bytes;
        size_t size = encodeInt64(val, bytes);
        buffer_.writeTo(reinterpret_cast<const char *>(bytes.data()), size);
    }

    void writeValue(float val) {
        union {
            float f;
            int32_t i;
        } v;
    
        v.f = val;
        buffer_.writeTo(v.i);
    }

    void writeValue(double val) {
        union {
            double d;
            int64_t i;
        } v;
        
        v.d = val;
        buffer_.writeTo(v.i);
    }

    void writeValue(const std::string &val) {
        writeBytes(val.c_str(), val.size());
    }

    void writeBytes(const void *val, size_t size) {
        this->writeValue(static_cast<int64_t>(size));
        buffer_.writeTo(reinterpret_cast<const char *>(val), size);
    }

    template <size_t N>
    void writeFixed(const uint8_t (&val)[N]) {
        buffer_.writeTo(reinterpret_cast<const char *>(val), N);
    }

    template <size_t N>
    void writeFixed(const boost::array<uint8_t, N> &val) {
        buffer_.writeTo(reinterpret_cast<const char *>(val.data()), val.size());
    }

    void writeRecord() {}

    void writeArrayBlock(int64_t size) {
        this->writeValue(static_cast<int64_t>(size));
    }

    void writeArrayEnd() {
        buffer_.writeTo<uint8_t>(0);
    }

    void writeMapBlock(int64_t size) {
        this->writeValue(static_cast<int64_t>(size));
    }

    void writeMapEnd() {
        buffer_.writeTo<uint8_t>(0);
    }

    void writeUnion(int64_t choice) {
        this->writeValue(static_cast<int64_t>(choice));
    }

    void writeEnum(int64_t choice) {
        this->writeValue(static_cast<int64_t>(choice));
    }

    InputBuffer buffer() const {
        return buffer_;
    }

  private:

    OutputBuffer buffer_;

};

} // namespace avro

#endif
