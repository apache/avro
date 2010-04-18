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

    AVRO_NUM_TYPES, // marker
    
    // The following is a pseudo-type used in implementation
    
    AVRO_SYMBOLIC = AVRO_NUM_TYPES,
    AVRO_UNKNOWN  = -1

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

inline bool isAvroTypeOrPseudoType(Type t) {
    return (t >= AVRO_STRING) && (t <= AVRO_NUM_TYPES);
}


std::ostream &operator<< (std::ostream &os, avro::Type type);

/// define a type to identify Null in template functions
struct Null { };

std::ostream& operator<< (std::ostream &os, const Null &null);

} // namespace avro


#endif
