#ifndef avro_Parser_hh__
#define avro_Parser_hh__

#include <stdint.h>
#include <vector>
#include <boost/noncopyable.hpp>

#include "InputStreamer.hh"
#include "Zigzag.hh"

namespace avro {

///
/// Parses from an avro encoding to the requested type.  Assumes the next item
/// in the avro binary data is the expected type.
///

class Parser : private boost::noncopyable
{

  public:

    explicit Parser(InputStreamer &in) :
        in_(in)
    {}

    void getNull() {}

    bool getBool() {
        uint8_t ival = 0;
        in_.getByte(ival);
        return(ival != 0);
    }

    int32_t getInt() {
        uint32_t encoded = getVarInt();
        return decodeZigzag32(encoded);
    }

    int64_t getLong() {
        uint64_t encoded = getVarInt();
        return decodeZigzag64(encoded);
    }

    float getFloat() {
        union { 
            float f;
            uint32_t i;
        } v;
        in_.getWord(v.i);
        return v.f;
    }

    double getDouble() {
        union { 
            double d;
            uint64_t i;
        } v;
        in_.getLongWord(v.i);
        return v.d;
    }

    void getBytes(std::vector<uint8_t> &val) {
        int64_t size = getLong();
        
        val.reserve(size);
        size_t bytes = 0;
        uint8_t bval = 0;
        while(bytes++ < static_cast<size_t>(size)) {
            in_.getByte(bval);
            val.push_back(bval);
        }
    }

    void getString(std::string &val) {
        int64_t size = getLong();
        
        val.reserve(size);
        size_t bytes = 0;
        uint8_t bval = 0;
        while(bytes++ < static_cast<size_t>(size)) {
            in_.getByte(bval);
            val.push_back(bval);
        }
    }

    void getFixed(std::vector<uint8_t> &val, size_t size) {
        
        val.reserve(size);
        size_t bytes = 0;
        uint8_t bval = 0;
        while(bytes++ < size) {
            in_.getByte(bval);
            val.push_back(bval);
        }
    }

    void getFixed(uint8_t *val, size_t size) {
        
        size_t bytes = 0;
        uint8_t bval = 0;
        while(bytes++ < size) {
            in_.getByte(bval);
            *val++ = bval;
        }
    }

    void getRecord() { }

    int64_t getArrayBlockSize() {
        return getLong();
    }

    int64_t getUnion() { 
        return getLong();
    }

    int64_t getEnum() {
        return getLong();
    }

    int64_t getMapBlockSize() {
        return getLong();
    }

  private:

    uint64_t getVarInt() {
        uint64_t encoded = 0;
        uint8_t val = 0;
        do {
            encoded <<= 8;
            in_.getByte(val);
            encoded |= (val & 0x7F);

        } while (val & 0x80);

        return encoded;
    }

    InputStreamer &in_;

};


} // namespace avro

#endif
