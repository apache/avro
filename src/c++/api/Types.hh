#ifndef avro_Types_hh__
#define avro_Types_hh__

#include <iostream>

namespace avro {

enum Type {

    AVRO_STRING,
    AVRO_BYTES,
    AVRO_INT,
    AVRO_LONG,
    AVRO_FLOAT,
    AVRO_DOUBLE,
    AVRO_BOOL,
    AVRO_NULL,

    AVRO_RECORD,
    AVRO_ENUM,
    AVRO_ARRAY,
    AVRO_MAP,
    AVRO_UNION,
    AVRO_FIXED,

    AVRO_SYMBOLIC,

    AVRO_NUM_TYPES,
};

inline bool isPrimitive(Type t) {
    return (t >= AVRO_STRING) && (t < AVRO_RECORD);
}

inline bool isCompound(Type t) {
    return (t>= AVRO_RECORD) && (t < AVRO_NUM_TYPES);
}

inline bool isAvroType(Type t) {
    return (t >= AVRO_STRING) && (t < AVRO_NUM_TYPES);
}

std::ostream &operator<< (std::ostream &os, const avro::Type type);

} // namespace avro


#endif
