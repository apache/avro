#ifndef avro_AvroTypes_hh__
#define avro_AvroTypes_hh__

#include <boost/static_assert.hpp>
#include "AvroTraits.hh"

namespace avro {

/// Null is an empty struct, convenient for representing null type in generic functions.

struct Null {};

template <>
struct is_serializable<Null> : public boost::true_type{};


} // namespace avro

#endif
