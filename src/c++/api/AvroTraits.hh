/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
