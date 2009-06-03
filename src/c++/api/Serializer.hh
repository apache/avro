#ifndef avro_Serializer_hh__
#define avro_Serializer_hh__

#include <boost/noncopyable.hpp>

#include "OutputStreamer.hh"
#include "Zigzag.hh"

namespace avro {

/// Class for writing avro data to a stream.

class Serializer : private boost::noncopyable
{

  public:

    explicit Serializer(OutputStreamer &out) :
        out_(out)
    {}

    void putNull() {}

    void putBool(bool val) {
        int8_t byte = (val != 0);
        out_.putByte(byte);
    }

    void putInt(int32_t val) {
        boost::array<uint8_t, 5> bytes;
        size_t size = encodeInt32(val, bytes);
        out_.putBytes(bytes.data(), size);
    }

    void putLong(int64_t val) {
        boost::array<uint8_t, 9> bytes;
        size_t size = encodeInt64(val, bytes);
        out_.putBytes(bytes.data(), size);
    }

    void putFloat(float val) {
        union {
            float f;
            int32_t i;
        } v;
    
        v.f = val;
        out_.putWord(v.i);
    }

    void putDouble(double val) {
        union {
            double d;
            int64_t i;
        } v;
        
        v.d = val;
        out_.putLongWord(v.i);
    }

    void putBytes(const uint8_t *val, size_t size) {
        this->putLong(size);
        out_.putBytes(val, size);
    }

    void putFixed(const uint8_t *val, size_t size) {
        out_.putBytes(val, size);
    }

    void putString(const std::string &val) {
        putBytes(reinterpret_cast<const uint8_t *>(val.c_str()), val.size());
    }

    /* here for compatibility with ValidatingSerializer in templates: */

    void beginRecord() {}

    void beginArrayBlock(int64_t size) {
        putLong(size);
    }

    void endArray() {
        putLong(0);
    }

    void beginMapBlock(int64_t size) {
        putLong(size);
    }

    void endMap() {
        putLong(0);
    }

    void beginUnion(int64_t choice) {
        putLong(choice);
    }

    void beginEnum(int64_t choice) {
        putLong(choice);
    }

  private:

    OutputStreamer &out_;

};

} // namespace avro

#endif
