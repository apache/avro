#include "JsonDom.hh"

#include <stdexcept>

#include <string.h>

#include "Stream.hh"
#include "JsonIO.hh"

namespace avro {
namespace json {

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
        return Entity(p.stringValue());
    case JsonParser::tkArrayStart:
        {
            p.advance();
            std::vector<Entity> v;
            while (p.peek() != JsonParser::tkArrayEnd) {
                v.push_back(readEntity(p));
            }
            p.advance();
            return Entity(v);
        }
    case JsonParser::tkObjectStart:
        {
            p.advance();
            std::map<std::string, Entity> v;
            while (p.peek() != JsonParser::tkObjectEnd) {
                p.advance();
                std::string k = p.stringValue();
                Entity n = readEntity(p);
                v.insert(std::make_pair(k, n));
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
        g.encodeBool(n.value<bool>());
        break;
    case etLong:
        g.encodeNumber(n.value<int64_t>());
        break;
    case etDouble:
        g.encodeNumber(n.value<double>());
        break;
    case etString:
        g.encodeString(n.value<std::string>());
        break;
    case etArray:
        {
            g.arrayStart();
            const std::vector<Entity>& v = n.value<std::vector<Entity> >();
            for (std::vector<Entity>::const_iterator it = v.begin();
                it != v.end(); ++it) {
                writeEntity(g, *it);
            }
            g.arrayEnd();
        }
        break;
    case etObject:
        {
            g.objectStart();
            const std::map<std::string, Entity>& v =
                n.value<std::map<std::string, Entity> >();
            for (std::map<std::string, Entity>::const_iterator it = v.begin();
                it != v.end(); ++it) {
                g.encodeString(it->first);
                writeEntity(g, it->second);
            }
            g.objectEnd();
        }
        break;
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

