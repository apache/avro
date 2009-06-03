#ifndef avro_OutputStreamer_hh__
#define avro_OutputStreamer_hh__

#include <iostream>

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

    virtual size_t putByte(uint8_t byte) = 0;
    virtual size_t putWord(uint32_t word) = 0;
    virtual size_t putLongWord(uint64_t word) = 0;
    virtual size_t putBytes(const uint8_t *bytes, size_t size) = 0;
};


///
/// An implementation of OutputStreamer that writes bytes to screen in ascii
/// representation of the hex digits, used for debugging.
///
    
class ScreenStreamer : public OutputStreamer {

    size_t putByte(uint8_t byte) {
        std::cout << "0x" << std::hex << static_cast<int32_t>(byte) << std::dec << " ";
        return 1;
    }

    size_t putWord(uint32_t word) {
        ScreenStreamer::putBytes(reinterpret_cast<uint8_t *>(&word), sizeof(word));
        return sizeof(uint32_t);
    }

    size_t putLongWord(uint64_t word) {
        ScreenStreamer::putBytes(reinterpret_cast<uint8_t *>(&word), sizeof(word));
        return sizeof(uint64_t);
    }

    size_t putBytes(const uint8_t *bytes, size_t size) {
        for (size_t i= 0; i < size; ++i) {
            ScreenStreamer::putByte(*bytes++);
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

    size_t putByte(uint8_t byte) {
        os_.put(byte);
        return 1;
    }

    size_t putWord(uint32_t word) {
        os_.write(reinterpret_cast<char *>(&word), sizeof(word));
        return sizeof(uint32_t);
    }

    size_t putLongWord(uint64_t word) {
        os_.write(reinterpret_cast<char *>(&word), sizeof(word));
        return sizeof(uint64_t);
    }

    size_t putBytes(const uint8_t *bytes, size_t size) {
        os_.write(reinterpret_cast<const char *>(bytes), size);
        return size;
    }

  private:

    std::ostream &os_;
};

} // namespace avro

#endif
