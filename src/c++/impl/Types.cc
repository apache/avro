#include <iostream>
#include <boost/static_assert.hpp>
#include "Types.hh"

namespace avro {

namespace strings {
const std::string typeToString[] = {
    "string",
    "bytes",
    "int",
    "long",
    "float",
    "double",
    "boolean",
    "null",
    "record",
    "enum",
    "array",
    "map",
    "union",
    "fixed",
    "symbolic"
};

BOOST_STATIC_ASSERT( (sizeof(typeToString)/sizeof(std::string)) == (AVRO_NUM_TYPES) );

} // namespace strings

BOOST_STATIC_ASSERT( AVRO_NUM_TYPES < 64 );

std::ostream &operator<< (std::ostream &os, const Type type)
{
    if(isAvroType(type)) {
        os << strings::typeToString[type];
    }
    else {
        os << static_cast<int>(type);
    }
    return os;
}

} // namespace avro

