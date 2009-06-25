#ifndef avro_AvroParse_hh__
#define avro_AvroParse_hh__

#include <boost/static_assert.hpp>
#include "AvroTraits.hh"

/// \file
///
/// Standalone parse functions for Avro types.

namespace avro {
    
/// The main parse entry point function.  Takes a parser (either validating or
/// plain) and the object that should receive the parsed data.

template <typename Parser, typename T>
void parse(Parser &p, T& val)
{
    parse(p, val, is_serializable<T>());
}

/// Type trait should be set to is_serializable in otherwise force the compiler to complain.

template <typename Parser, typename T>
void parse(Parser &p, T& val, const boost::false_type &)
{
    BOOST_STATIC_ASSERT(sizeof(T)==0);
}

/// The remainder of the file includes default implementations for serializable types.

// @{

template <typename Parser>
void parse(Parser &p, int32_t &val, const boost::true_type &) {
    val = p.getInt();
}

template <typename Parser>
void parse(Parser &p, int64_t &val, const boost::true_type &) {
    val = p.getLong();
}

template <typename Parser>
void parse(Parser &p, float &val, const boost::true_type &) {
    val = p.getFloat();
}

template <typename Parser>
void parse(Parser &p, double &val, const boost::true_type &) {
    val = p.getDouble();
}

template <typename Parser>
void parse(Parser &p, Null &, const boost::true_type &) {
    p.getNull();
}

template <typename Parser>
void parse(Parser &p, bool &val, const boost::true_type &) {
    val = p.getBool();
}

template <typename Parser>
void parse(Parser &p, std::string &val, const boost::true_type &) {
    p.getString(val);
}

template <typename Parser>
void parse(Parser &p, std::vector<uint8_t> &val, const boost::true_type &) {
    p.getBytes(&val);
}

// @}

} // namespace avro

#endif
