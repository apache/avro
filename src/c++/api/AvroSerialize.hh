#ifndef avro_AvroSerialize_hh__
#define avro_AvroSerialize_hh__

#include <boost/static_assert.hpp>
#include "AvroTypes.hh"

/// \file
///
/// Standalone serialize functions for Avro types.

namespace avro {
    
/// The main serializer entry point function.  Takes a serializer (either validating or
/// plain) and the object that should be serialized.

template <typename Serializer, typename T>
void serialize(Serializer &s, const T& val)
{
    serialize(s, val, is_serializable<T>());
}

/// Type trait should be set to is_serializable in otherwise force the compiler to complain.

template <typename Serializer, typename T>
void serialize(Serializer &s, const T& val, const boost::false_type &)
{
    BOOST_STATIC_ASSERT(sizeof(T)==0);
}

/// The remainder of the file includes default implementations for serializable types.

// @{

template <typename Serializer>
void serialize(Serializer &s, int32_t val, const boost::true_type &) {
    s.putInt(val);
}

template <typename Serializer>
void serialize(Serializer &s, int64_t val, const boost::true_type &) {
    s.putLong(val);
}

template <typename Serializer>
void serialize(Serializer &s, float val, const boost::true_type &) {
    s.putFloat(val);
}

template <typename Serializer>
void serialize(Serializer &s, double val, const boost::true_type &) {
    s.putDouble(val);
}

template <typename Serializer>
void serialize(Serializer &s, const Null &, const boost::true_type &) {
    s.putNull();
}

template <typename Serializer>
void serialize(Serializer &s, bool val, const boost::true_type &) {
    s.putBool(val);
}

template <typename Serializer>
void serialize(Serializer &s, const std::string &val, const boost::true_type &) {
    s.putString(val);
}

template <typename Serializer>
void serialize(Serializer &s, const std::vector<uint8_t> &val, const boost::true_type &) {
    s.putBytes(&val[0], val.size());
}

// @}

} // namespace avro

#endif
