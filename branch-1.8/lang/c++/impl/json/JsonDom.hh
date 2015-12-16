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

#include <iostream>
#include <stdint.h>
#include <map>
#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>

#include "boost/any.hpp"
#include "Config.hh"

namespace avro {

class AVRO_DECL InputStream;

namespace json {
class Entity;
    
typedef bool Bool;
typedef int64_t Long;
typedef double Double;
typedef std::string String;
typedef std::vector<Entity> Array;
typedef std::map<std::string, Entity> Object;
    
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
    void ensureType(EntityType) const;
public:
    Entity() : type_(etNull) { }
    Entity(Bool v) : type_(etBool), value_(v) { }
    Entity(Long v) : type_(etLong), value_(v) { }
    Entity(Double v) : type_(etDouble), value_(v) { }
    Entity(const boost::shared_ptr<String>& v) : type_(etString), value_(v) { }
    Entity(const boost::shared_ptr<Array>& v) : type_(etArray), value_(v) { }
    Entity(const boost::shared_ptr<Object>& v) : type_(etObject), value_(v) { }
    
    EntityType type() const { return type_; }

    Bool boolValue() const {
        ensureType(etBool);
        return boost::any_cast<Bool>(value_);
    }

    Long longValue() const {
        ensureType(etLong);
        return boost::any_cast<Long>(value_);
    }
    
    Double doubleValue() const {
        ensureType(etDouble);
        return boost::any_cast<Double>(value_);
    }

    const String& stringValue() const {
        ensureType(etString);
        return **boost::any_cast<boost::shared_ptr<String> >(&value_);
    }
    
    const Array& arrayValue() const {
        ensureType(etArray);
        return **boost::any_cast<boost::shared_ptr<Array> >(&value_);
    }

    const Object& objectValue() const {
        ensureType(etObject);
        return **boost::any_cast<boost::shared_ptr<Object> >(&value_);
    }

    std::string toString() const;
};

template <typename T>
struct type_traits {
};

template <> struct type_traits<bool> {
    static EntityType type() { return etBool; }
    static const char* name() { return "bool"; }
};

template <> struct type_traits<int64_t> {
    static EntityType type() { return etLong; }
    static const char* name() { return "long"; }
};

template <> struct type_traits<double> {
    static EntityType type() { return etDouble; }
    static const char* name() { return "double"; }
};
    
template <> struct type_traits<std::string> {
    static EntityType type() { return etString; }
    static const char* name() { return "string"; }
};

template <> struct type_traits<std::vector<Entity> > {
    static EntityType type() { return etArray; }
    static const char* name() { return "array"; }
};

template <> struct type_traits<std::map<std::string, Entity> > {
    static EntityType type() { return etObject; }
    static const char* name() { return "object"; }
};

AVRO_DECL Entity readEntity(JsonParser& p);

AVRO_DECL Entity loadEntity(InputStream& in);
AVRO_DECL Entity loadEntity(const char* text);
AVRO_DECL Entity loadEntity(const uint8_t* text, size_t len);

void writeEntity(JsonGenerator& g, const Entity& n);

}
}

#endif


