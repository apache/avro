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

#ifndef avro_InputStreamer_hh__
#define avro_InputStreamer_hh__

#include <iostream>

namespace avro {

///
/// A generic object for reading inputs from a stream.  Serves as a base class,
/// so that avro parser objects can read from different sources (for example,
/// istreams or blocks of memory),  but the derived class provides the
/// implementation for the different source.
///
/// Right now this class is very bare-bones.
///
    
class InputStreamer {

  public:

    virtual ~InputStreamer()
    { }

    virtual size_t readByte(uint8_t &byte) = 0;
    virtual size_t readWord(uint32_t &word) = 0;
    virtual size_t readLongWord(uint64_t &word) = 0;
    virtual size_t readBytes(uint8_t *bytes, size_t size) = 0;
};


///
/// An implementation of InputStreamer that uses a std::istream for input.
///
/// Right now this class is very bare-bones, without much in way of error
/// handling.
///
    
class IStreamer : public InputStreamer {

  public:

    IStreamer(std::istream &is) :
        is_(is)
    {}

    size_t readByte(uint8_t &byte) {
        char val;
        is_.get(val);
        byte = val;
        return 1;
    }

    size_t readWord(uint32_t &word) {
        is_.read(reinterpret_cast<char *>(&word), sizeof(word));
        return is_.gcount();
    }

    size_t readLongWord(uint64_t &word) {
        is_.read(reinterpret_cast<char *>(&word), sizeof(word));
        return is_.gcount();
    }

    size_t readBytes(uint8_t *bytes, size_t size) {
        is_.read(reinterpret_cast<char *>(bytes), size);
        return is_.gcount();
    }

  private:

    std::istream &is_;
};

} // namespace avro

#endif
