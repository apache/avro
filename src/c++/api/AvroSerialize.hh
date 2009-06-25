#ifndef avro_AvroSerialize_hh__
#define avro_AvroSerialize_hh__

#include <boost/static_assert.hpp>
#include "AvroTraits.hh"

/// \file
///
/// Standalone serialize functions for Avro types.

namespace avro {
    
/// The main serializer entry point function.  Takes a serializer (either validating or
/// plain) and the object that should be serialized.

template <typename Writer, typename T>
void serialize(Writer &s, const T& val)
{
    serialize(s, val, is_serializable<T>());
}

/// Type trait should be set to is_serializable in otherwise force the compiler to complain.

template <typename Writer, typename T>
void serialize(Writer &s, const T& val, const boost::false_type &)
{
    BOOST_STATIC_ASSERT(sizeof(T)==0);
}

/// The remainder of the file includes default implementations for serializable types.

// @{

template <typename Writer, typename T>
void serialize(Writer &s, T val, const boost::true_type &) {
    s.putValue(val);
}

template <typename Writer>
void serialize(Writer &s, const std::vector<uint8_t> &val, const boost::true_type &) {
    s.putBytes(&val[0], val.size());
}

// @}

} // namespace avro

#endif
