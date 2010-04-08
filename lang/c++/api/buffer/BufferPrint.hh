#ifndef avro_BufferPrint_hh__
#define avro_BufferPrint_hh__

#include <ctype.h>
#include <iostream>
#include <iomanip>
#include "BufferReader.hh"

/** 
 * \file BufferPrint.hh
 *
 * \brief Convenience functions for printing buffer contents
 **/

namespace avro {

namespace detail {

/**
 * \fn hexPrint
 * 
 * Prints a buffer to a stream in the canonical hex+ASCII format, 
 * the same used by the program 'hexdump -C'
 *
 **/

inline void
hexPrint(std::ostream &os, BufferReader &reader)
{
    std::ios_base::fmtflags savedFlags = os.flags();

    char sixteenBytes[16];
    int offset = 0;

    os << std::setfill('0');
    os << std::hex;

    while(reader.bytesRemaining()) {

        os << std::setw(8) << offset << "  ";

        size_t inBuffer = reader.read(sixteenBytes, sizeof(sixteenBytes));
        offset += inBuffer;

        // traverse 8 bytes or inBuffer, whatever is less
        size_t cnt = std::min(inBuffer, static_cast<size_t>(8));

        size_t i = 0;
        for (; i < cnt; ++i) {
            os << std::setw(2);
            os << (static_cast<int>(sixteenBytes[i]) & 0xff) << ' ';
        }
        for (; i < 8; ++i) {
            os << "   ";
        }
        os << ' ';

        // traverse 16 bytes or inBuffer, whatever is less
        cnt = std::min(inBuffer, static_cast<size_t>(16));

        for (; i < cnt; ++i) {
            os << std::setw(2);
            os << (static_cast<int>(sixteenBytes[i]) & 0xff) << ' ';
        }
        for (; i < 16; ++i) {
            os << "   ";
        }
        os << " |";
        for(i = 0; i < inBuffer; ++i) {
            os.put(isprint(sixteenBytes[i]) ? sixteenBytes[i] : '.' );
        }
        os << "|\n";

    }

    // restore flags
    os.flags( savedFlags);
}

} // namespace detail

} // namespace

inline
std::ostream& operator<<(std::ostream& os, const avro::OutputBuffer& buffer)
{
    avro::BufferReader reader(buffer);
    avro::detail::hexPrint(os, reader);
    return os;
}

inline
std::ostream& operator<<(std::ostream& os, const avro::InputBuffer& buffer)
{
    avro::BufferReader reader(buffer);
    avro::detail::hexPrint(os, reader);
    return os;
}

#endif 
