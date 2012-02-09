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

#ifndef avro_json_JsonDom_hh__
#define avro_json_JsonDom_hh__

#include <stdint.h>
#include <map>
#include <string>
#include <vector>

#include "boost/any.hpp"
#include "Config.hh"

namespace avro {

class AVRO_DECL InputStream;

namespace json {

class AVRO_DECL JsonParser;
class AVRO_DECL JsonGenerator;

enum EntityType {
    etNull,
    etBool,
    etLong,
    etDouble,
    etString,
    etArray,
    etObject
};

class AVRO_DECL Entity {
    EntityType type_;
    boost::any value_;
public:
    Entity() : type_(etNull) { }
    Entity(bool v) : type_(etBool), value_(v) { }
    Entity(int64_t v) : type_(etLong), value_(v) { }
    Entity(double v) : type_(etDouble), value_(v) { }
    Entity(const std::string& v) : type_(etString), value_(v) { }
    Entity(const std::vector<Entity> v) : type_(etArray), value_(v) { }
    Entity(const std::map<std::string, Entity> v) : type_(etObject), value_(v) {
    }


    EntityType type() const { return type_; }

    template <typename T>
    const T& value() const {
        return *boost::any_cast<T>(&value_);
    }

    template <typename T>
    T& value() {
        return *boost::any_cast<T>(&value_);
    }

    template <typename T>
    void set(const T& v) {
        *boost::any_cast<T>(&value_) = v;
    }

    std::string toString() const;
};

template <typename T>
struct type_traits {
};

template <> struct type_traits<std::string> {
    static EntityType type() { return etString; }
    static const char* name() { return "string"; }
};

template <> struct type_traits<std::vector<Entity> > {
    static EntityType type() { return etArray; }
    static const char* name() { return "array"; }
};

template <> struct type_traits<int64_t> {
    static EntityType type() { return etLong; }
    static const char* name() { return "integer"; }
};

AVRO_DECL Entity readEntity(JsonParser& p);

AVRO_DECL Entity loadEntity(InputStream& in);
AVRO_DECL Entity loadEntity(const char* text);
AVRO_DECL Entity loadEntity(const uint8_t* text, size_t len);

void writeEntity(JsonGenerator& g, const Entity& n);

}
}

#endif


