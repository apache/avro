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

#include "JsonDom.hh"

#include <stdexcept>

#include <string.h>
#include <boost/make_shared.hpp>

#include "Stream.hh"
#include "JsonIO.hh"

using std::string;
using boost::format;

namespace avro {
namespace json {
static const char* typeToString(EntityType t)
{
    switch (t) {
    case etNull: return "null";
    case etBool: return "bool";
    case etLong: return "long";
    case etDouble: return "double";
    case etString: return "string";
    case etArray: return "array";
    case etObject: return "object";
    default: return "unknown";
    }
}

Entity readEntity(JsonParser& p)
{
    switch (p.peek()) {
    case JsonParser::tkNull:
        p.advance();
        return Entity();
    case JsonParser::tkBool:
        p.advance();
        return Entity(p.boolValue());
    case JsonParser::tkLong:
        p.advance();
        return Entity(p.longValue());
    case JsonParser::tkDouble:
        p.advance();
        return Entity(p.doubleValue());
    case JsonParser::tkString:
        p.advance();
        return Entity(boost::make_shared<String>(p.stringValue()));
    case JsonParser::tkArrayStart:
        {
            p.advance();
            boost::shared_ptr<Array> v = boost::make_shared<Array>();
            while (p.peek() != JsonParser::tkArrayEnd) {
                v->push_back(readEntity(p));
            }
            p.advance();
            return Entity(v);
        }
    case JsonParser::tkObjectStart:
        {
            p.advance();
            boost::shared_ptr<Object> v = boost::make_shared<Object>();
            while (p.peek() != JsonParser::tkObjectEnd) {
                p.advance();
                std::string k = p.stringValue();
                Entity n = readEntity(p);
                v->insert(std::make_pair(k, n));
            }
            p.advance();
            return Entity(v);
        }
    default:
        throw std::domain_error(JsonParser::toString(p.peek()));
    }
    
}

Entity loadEntity(const char* text)
{
    return loadEntity(reinterpret_cast<const uint8_t*>(text), ::strlen(text));
}

Entity loadEntity(InputStream& in)
{
    JsonParser p;
    p.init(in);
    return readEntity(p);
}

Entity loadEntity(const uint8_t* text, size_t len)
{
    std::auto_ptr<InputStream> in = memoryInputStream(text, len);
    return loadEntity(*in);
}

void writeEntity(JsonGenerator& g, const Entity& n)
{
    switch (n.type()) {
    case etNull:
        g.encodeNull();
        break;
    case etBool:
        g.encodeBool(n.boolValue());
        break;
    case etLong:
        g.encodeNumber(n.longValue());
        break;
    case etDouble:
        g.encodeNumber(n.doubleValue());
        break;
    case etString:
        g.encodeString(n.stringValue());
        break;
    case etArray:
        {
            g.arrayStart();
            const Array& v = n.arrayValue();
            for (Array::const_iterator it = v.begin();
                it != v.end(); ++it) {
                writeEntity(g, *it);
            }
            g.arrayEnd();
        }
        break;
    case etObject:
        {
            g.objectStart();
            const Object& v = n.objectValue();
            for (Object::const_iterator it = v.begin(); it != v.end(); ++it) {
                g.encodeString(it->first);
                writeEntity(g, it->second);
            }
            g.objectEnd();
        }
        break;
    }
}

void Entity::ensureType(EntityType type) const
{
    if (type_ != type) {
        format msg = format("Invalid type. Expected \"%1%\" actual %2%") %
            typeToString(type) % typeToString(type_);
        throw Exception(msg);
    }
}
    

std::string Entity::toString() const
{
    std::auto_ptr<OutputStream> out = memoryOutputStream();
    JsonGenerator g;
    g.init(*out);
    writeEntity(g, *this);
    g.flush();
    std::auto_ptr<InputStream> in = memoryInputStream(*out);
    const uint8_t *p = 0;
    size_t n = 0;
    size_t c = 0;
    while (in->next(&p, &n)) {
        c += n;
    }
    std::string result;
    result.resize(c);
    c = 0;
    std::auto_ptr<InputStream> in2 = memoryInputStream(*out);
    while (in2->next(&p, &n)) {
        ::memcpy(&result[c], p, n);
        c += n;
    }
    return result;
}

}
}

