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

BOOST_STATIC_ASSERT( (sizeof(typeToString)/sizeof(std::string)) == (AVRO_NUM_TYPES+1) );

} // namespace strings


// this static assert exists because a 32 bit integer is used as a bit-flag for each type,
// and it would be a problem for this flag if we ever supported more than 32 types
BOOST_STATIC_ASSERT( AVRO_NUM_TYPES < 32 );

std::ostream &operator<< (std::ostream &os, Type type)
{
    if(isAvroTypeOrPseudoType(type)) {
        os << strings::typeToString[type];
    }
    else {
        os << static_cast<int>(type);
    }
    return os;
}

} // namespace avro

