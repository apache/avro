#include <boost/static_assert.hpp>

#include "ValidatingSerializer.hh"
#include "ValidSchema.hh"
#include "OutputStreamer.hh"

namespace avro {

ValidatingSerializer::ValidatingSerializer(const ValidSchema &schema, OutputStreamer &out) :
    validator_(schema),
    serializer_(out)
{ }

void
ValidatingSerializer::putNull()
{ 
    checkSafeToPut(AVRO_NULL);
    serializer_.putNull();
    validator_.advance();
}

void
ValidatingSerializer::putInt(int32_t val)
{
    checkSafeToPut(AVRO_INT);
    serializer_.putInt(val);
    validator_.advance();
}

void
ValidatingSerializer::putLong(int64_t val)
{
    checkSafeToPut(AVRO_LONG);
    serializer_.putLong(val);
    validator_.advance();
}


void
ValidatingSerializer::putFloat(float val)
{
    checkSafeToPut(AVRO_FLOAT);
    serializer_.putFloat(val);
    validator_.advance();
}

void
ValidatingSerializer::putDouble(double val)
{
    checkSafeToPut(AVRO_DOUBLE);
    serializer_.putDouble(val);
    validator_.advance();
}

void
ValidatingSerializer::putBool(bool val)
{
    checkSafeToPut(AVRO_BOOL);
    serializer_.putBool(val);
    validator_.advance();
}

void
ValidatingSerializer::putString(const std::string &val)
{
    checkSafeToPut(AVRO_STRING);
    serializer_.putString(val);
    validator_.advance();
}

void
ValidatingSerializer::putBytes(const uint8_t *val, size_t size)
{
    checkSafeToPut(AVRO_BYTES);
    serializer_.putBytes(val, size);
    validator_.advance();
}

void 
ValidatingSerializer::putCount(int64_t count)
{
    checkSafeToPut(AVRO_LONG);
    serializer_.putLong(count);
    validator_.advanceWithCount(count);
}

void 
ValidatingSerializer::beginRecord()
{
    checkSafeToPut(AVRO_RECORD);
    validator_.advance();
}

void 
ValidatingSerializer::beginArrayBlock(int64_t size)
{
    checkSafeToPut(AVRO_ARRAY);
    validator_.advance();
    putCount(size);
}

void 
ValidatingSerializer::endArray()
{
    beginArrayBlock(0);
}

void 
ValidatingSerializer::beginMapBlock(int64_t size)
{
    checkSafeToPut(AVRO_MAP);
    validator_.advance();
    putCount(size);
}

void 
ValidatingSerializer::endMap()
{
    beginMapBlock(0);
}

void 
ValidatingSerializer::beginUnion(int64_t choice)
{
    checkSafeToPut(AVRO_UNION);
    validator_.advance();
    putCount(choice);
}

void 
ValidatingSerializer::beginEnum(int64_t choice)
{
    checkSafeToPut(AVRO_ENUM);
    validator_.advance();
    putCount(choice);
}


} // namepspace avro
