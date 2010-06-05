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

#ifndef avro_OutputStreamer_hh__
#define avro_OutputStreamer_hh__

#include <iostream>
#include <stdint.h>

namespace avro {

///
/// A generic object for outputing data to a stream.
///
/// Serves as a base class, so that avro serializer objects can write to
/// different sources (for example, istreams or blocks of memory),  but the
/// derived class provides the implementation for the different source.
///
/// Right now this class is very bare-bones.
///
    
class OutputStreamer {

  public:

    virtual ~OutputStreamer()
    { }

    virtual size_t writeByte(uint8_t byte) = 0;
    virtual size_t writeWord(uint32_t word) = 0;
    virtual size_t writeLongWord(uint64_t word) = 0;
    virtual size_t writeBytes(const void *bytes, size_t size) = 0;
};


///
/// An implementation of OutputStreamer that writes bytes to screen in ascii
/// representation of the hex digits, used for debugging.
///
    
class ScreenStreamer : public OutputStreamer {

    size_t writeByte(uint8_t byte) {
        std::cout << "0x" << std::hex << static_cast<int32_t>(byte) << std::dec << " ";
        return 1;
    }

    size_t writeWord(uint32_t word) {
        ScreenStreamer::writeBytes(&word, sizeof(word));
        return sizeof(uint32_t);
    }

    size_t writeLongWord(uint64_t word) {
        ScreenStreamer::writeBytes(&word, sizeof(word));
        return sizeof(uint64_t);
    }

    size_t writeBytes(const void *bytes, size_t size) {
        const uint8_t *ptr = reinterpret_cast<const uint8_t *>(bytes);
        for (size_t i= 0; i < size; ++i) {
            ScreenStreamer::writeByte(*ptr++);
        }
        std::cout << std::endl;
        return size;
    }

};

///
/// An implementation of OutputStreamer that writes bytes to a std::ostream for
/// output.
///
/// Right now this class is very bare-bones, without much in way of error
/// handling.
///
    
class OStreamer : public OutputStreamer {

  public:

    OStreamer(std::ostream &os) :
        os_(os)
    {}

    size_t writeByte(uint8_t byte) {
        os_.put(byte);
        return 1;
    }

    size_t writeWord(uint32_t word) {
        os_.write(reinterpret_cast<char *>(&word), sizeof(word));
        return sizeof(uint32_t);
    }

    size_t writeLongWord(uint64_t word) {
        os_.write(reinterpret_cast<char *>(&word), sizeof(word));
        return sizeof(uint64_t);
    }

    size_t writeBytes(const void *bytes, size_t size) {
        os_.write(reinterpret_cast<const char *>(bytes), size);
        return size;
    }

  private:

    std::ostream &os_;
};

} // namespace avro

#endif
