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

template <typename Reader, typename T>
void parse(Reader &p, T& val)
{
    parse(p, val, is_serializable<T>());
}

/// Type trait should be set to is_serializable in otherwise force the compiler to complain.

template <typename Reader, typename T>
void parse(Reader &p, T& val, const boost::false_type &)
{
    BOOST_STATIC_ASSERT(sizeof(T)==0);
}

/// The remainder of the file includes default implementations for serializable types.

// @{

template <typename Reader, typename T>
void parse(Reader &p, T &val, const boost::true_type &) {
    p.readValue(val);
}

template <typename Reader>
void parse(Reader &p, std::vector<uint8_t> &val, const boost::true_type &) {
    p.readBytes(val);
}

// @}

} // namespace avro

#endif
