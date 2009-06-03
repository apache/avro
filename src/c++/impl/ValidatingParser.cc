#include <boost/static_assert.hpp>

#include "ValidatingParser.hh"
#include "ValidSchema.hh"
#include "OutputStreamer.hh"

namespace avro {

ValidatingParser::ValidatingParser(const ValidSchema &schema, InputStreamer &in) :
    validator_(schema),
    parser_(in)
{ }

void
ValidatingParser::getNull()
{ 
    checkSafeToGet(AVRO_NULL);
    validator_.advance();
    parser_.getNull();
}

int32_t
ValidatingParser::getInt()
{
    checkSafeToGet(AVRO_INT);
    int32_t val = parser_.getInt();
    validator_.advance();
    return val;
}

int64_t
ValidatingParser::getLong()
{
    checkSafeToGet(AVRO_LONG);
    int64_t val = parser_.getLong();
    validator_.advance();
    return val;
}

float
ValidatingParser::getFloat()
{
    checkSafeToGet(AVRO_FLOAT);
    validator_.advance();
    return parser_.getFloat();
}

double
ValidatingParser::getDouble()
{
    checkSafeToGet(AVRO_DOUBLE);
    validator_.advance();
    return parser_.getDouble();
}

bool
ValidatingParser::getBool()
{
    checkSafeToGet(AVRO_BOOL);
    validator_.advance();
    return parser_.getBool();
}

void
ValidatingParser::getString(std::string &val)
{
    checkSafeToGet(AVRO_STRING);
    validator_.advance();
    parser_.getString(val);
}

void
ValidatingParser::getBytes(std::vector<uint8_t> &val)
{
    checkSafeToGet(AVRO_BYTES);
    validator_.advance();
    parser_.getBytes(val);
}

int64_t
ValidatingParser::getCount()
{
    checkSafeToGet(AVRO_LONG);
    int64_t val = parser_.getLong();
    validator_.advanceWithCount(val);
    return val;
}

void 
ValidatingParser::getRecord()
{
    checkSafeToGet(AVRO_RECORD);
    validator_.advance();
}

int64_t 
ValidatingParser::getUnion()
{
    checkSafeToGet(AVRO_UNION);
    validator_.advance();
    return getCount();
}

int64_t 
ValidatingParser::getEnum()
{
    checkSafeToGet(AVRO_ENUM);
    validator_.advance();
    return getCount();
}

int64_t 
ValidatingParser::getMapBlockSize()
{
    checkSafeToGet(AVRO_MAP);
    validator_.advance();
    return getCount();
}

int64_t 
ValidatingParser::getArrayBlockSize()
{
    checkSafeToGet(AVRO_ARRAY);
    validator_.advance();
    return getCount();
}

} // namepspace avro
