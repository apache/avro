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

#ifndef avro_Decoder_hh__
#define avro_Decoder_hh__

#include <stdint.h>
#include <string>
#include <vector>

#include "ValidSchema.hh"
#include "Stream.hh"

#include <boost/shared_ptr.hpp>

/// \file
///
/// Low level support for decoding avro values.
/// This class has two types of funtions.  One type of functions support
/// decoding of leaf values (for example, decodeLong and
/// decodeString). These functions have analogs in Encoder.
/// 
/// The other type of functions support decoding of maps and arrays.
/// These functions are arrayStart, startItem, and arrayEnd
/// (and similar functions for maps).

namespace avro {

class Decoder {
public:
    /// All future decoding will come from is, which should be valid
    /// until replaced by another call to init() or this Decoder is
    /// destructed.
    virtual void init(InputStream& is) = 0;

    /// Decodes a null from the current stream.
    virtual void decodeNull() = 0;

    /// Decodes a bool from the current stream
    virtual bool decodeBool() = 0;

    /// Decodes a 32-bit int from the current stream.
    virtual int32_t decodeInt() = 0;

    /// Decodes a 64-bit signed int from the current stream.
    virtual int64_t decodeLong() = 0;

    /// Decodes a single-precision floating point number from current stream.
    virtual float decodeFloat() = 0;

    /// Decodes a double-precision floating point number from current stream.
    virtual double decodeDouble() = 0;

    /// Decodes a UTF-8 string from the current stream.
    std::string decodeString() {
        std::string result;
        decodeString(result);
        return result;
    }

    /**
     * Decodes a UTF-8 string from the stream and assigns it to value.
     */
    virtual void decodeString(std::string& value) = 0;

    /// Skips a string on the current stream.
    virtual void skipString() = 0;

    /// Decodes arbitray binary data from the current stream.
    std::vector<uint8_t> decodeBytes() {
        std::vector<uint8_t> result;
        decodeBytes(result);
        return result;
    }

    /// Decodes arbitray binary data from the current stream and puts it
    /// in value.
    virtual void decodeBytes(std::vector<uint8_t>& value) = 0;

    /// Skips bytes on the current stream.
    virtual void skipBytes() = 0;

    /// Decodes fixed length binary from the current stream.
    std::vector<uint8_t> decodeFixed(size_t n) {
        std::vector<uint8_t> result;
        decodeFixed(n, result);
        return result;
    }

    virtual void decodeFixed(size_t n, std::vector<uint8_t>& value) = 0;

    /// Skips fixed length binary on the current stream.
    virtual void skipFixed(size_t n) = 0;

    /// Decodes enum from the current stream.
    virtual size_t decodeEnum() = 0;

    /// Start decoding an array. Returns the number of entries in first chunk.
    virtual size_t arrayStart() = 0;

    /// Returns the number of entries in next chunk. 0 if last.
    virtual size_t arrayNext() = 0;

    /// Tries to skip an array. If it can, it returns 0. Otherwise
    /// it returns the number of elements to be skipped. The client
    /// should skip the individual items. In such cases, skipArray
    /// is identical to arrayStart.
    virtual size_t skipArray() = 0;

    /// Start decoding a map. Returns the number of entries in first chunk.
    virtual size_t mapStart() = 0;

    /// Returns the number of entries in next chunk. 0 if last.
    virtual size_t mapNext() = 0;

    /// Tries to skip a map. If it can, it returns 0. Otherwise
    /// it returns the number of elements to be skipped. The client
    /// should skip the individual items. In such cases, skipMap
    /// is identical to mapStart.
    virtual size_t skipMap() = 0;

    /// Decodes a branch of a union. The actual value is to follow.
    virtual size_t decodeUnionIndex() = 0;
};

typedef boost::shared_ptr<Decoder> DecoderPtr;

class ResolvingDecoder : public Decoder {
public:
    /// Returns the order of fields for records.
    /// The order of fields could be different from the order of their
    /// order in the schema because the writer's field order could
    /// be different. In order to avoid buffering and later use,
    /// we return the values in the writer's field order.
    virtual const std::vector<size_t>& fieldOrder() = 0;
};

typedef boost::shared_ptr<ResolvingDecoder> ResolvingDecoderPtr;
/**
 *  Returns an decoder that can decode binary Avro standard.
 */
DecoderPtr binaryDecoder();

/**
 *  Returns an decoder that validates sequence of calls to an underlying
 *  Decoder against the given schema.
 */
DecoderPtr validatingDecoder(const ValidSchema& schema,
    const DecoderPtr& base);

/**
 *  Returns an decoder that can decode Avro standard for JSON.
 */
DecoderPtr jsonDecoder(const ValidSchema& schema);

/**
 *  Returns a decoder that decodes avro data from base written according to
 *  writerSchema and resolves against readerSchema.
 *  The client uses the decoder as if the data were written using readerSchema.
 *  // FIXME: Handle out of order fields.
 */
ResolvingDecoderPtr resolvingDecoder(const ValidSchema& writer,
    const ValidSchema& reader, const DecoderPtr& base);


}   // namespace avro

#endif
