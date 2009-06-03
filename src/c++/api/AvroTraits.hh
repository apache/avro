#ifndef avro_AvroTraits_hh__
#define avro_AvroTraits_hh__

#include <boost/type_traits.hpp>

/// \file
///
/// Define an is_serializable trait for types we can serialize natively. 
/// New types will need to define the trait as well.

namespace avro {

template <typename T>
struct is_serializable : public boost::false_type{};

template <>
struct is_serializable<int32_t> : public boost::true_type{};

template <>
struct is_serializable<int64_t> : public boost::true_type{};

template <>
struct is_serializable<float> : public boost::true_type{};

template <>
struct is_serializable<double> : public boost::true_type{};

template <>
struct is_serializable<bool> : public boost::true_type{};

template <>
struct is_serializable<std::string> : public boost::true_type{};

template <>
struct is_serializable<std::vector<uint8_t > > : public boost::true_type{};

} // namespace avro

#endif
