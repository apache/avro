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

    virtual size_t getByte(uint8_t &byte) = 0;
    virtual size_t getWord(uint32_t &word) = 0;
    virtual size_t getLongWord(uint64_t &word) = 0;
    virtual size_t getBytes(uint8_t *bytes, size_t size) = 0;
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

    size_t getByte(uint8_t &byte) {
        char val;
        is_.get(val);
        byte = val;
        return 1;
    }

    size_t getWord(uint32_t &word) {
        is_.read(reinterpret_cast<char *>(&word), sizeof(word));
        return is_.gcount();
    }

    size_t getLongWord(uint64_t &word) {
        is_.read(reinterpret_cast<char *>(&word), sizeof(word));
        return is_.gcount();
    }

    size_t getBytes(uint8_t *bytes, size_t size) {
        is_.read(reinterpret_cast<char *>(bytes), size);
        return is_.gcount();
    }

  private:

    std::istream &is_;
};

} // namespace avro

#endif
