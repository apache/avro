#ifndef avro_Encoding_hh__
#define avro_Encoding_hh__

#include <stdint.h>
#include <boost/array.hpp>

/// \file
/// Functions for encoding and decoding integers with zigzag compression

namespace avro {

uint64_t encodeZigzag64(int64_t input);
int64_t decodeZigzag64(uint64_t input);

uint32_t encodeZigzag32(int32_t input);
int32_t decodeZigzag32(uint32_t input);

size_t encodeInt32(int32_t input, boost::array<uint8_t, 5> &output);
size_t encodeInt64(int64_t input, boost::array<uint8_t, 9> &output);

} // namespace avro

#endif
