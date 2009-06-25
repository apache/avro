#ifndef avro_AvroTraits_hh__
#define avro_AvroTraits_hh__

#include <boost/type_traits.hpp>

#include "Types.hh"

/// \file
///
/// Define an is_serializable trait for types we can serialize natively. 
/// New types will need to define the trait as well.

namespace avro {

template <typename T>
struct is_serializable : public boost::false_type{};

template <typename T>
struct type_to_avro {
    static const Type type = AVRO_NUM_TYPES;
};

#define DEFINE_PRIMITIVE(CTYPE, AVROTYPE) \
template <> \
struct is_serializable<CTYPE> : public boost::true_type{}; \
\
template <> \
struct type_to_avro<CTYPE> { \
    static const Type type = AVROTYPE; \
};

DEFINE_PRIMITIVE(int32_t, AVRO_INT)
DEFINE_PRIMITIVE(int64_t, AVRO_LONG)
DEFINE_PRIMITIVE(float, AVRO_FLOAT)
DEFINE_PRIMITIVE(double, AVRO_DOUBLE)
DEFINE_PRIMITIVE(bool, AVRO_BOOL)
DEFINE_PRIMITIVE(Null, AVRO_NULL)
DEFINE_PRIMITIVE(std::string, AVRO_STRING)
DEFINE_PRIMITIVE(std::vector<uint8_t>, AVRO_BYTES)


} // namespace avro

#endif
