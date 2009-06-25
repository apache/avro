#include <boost/static_assert.hpp>

#include "ValidatingWriter.hh"
#include "ValidSchema.hh"
#include "OutputStreamer.hh"
#include "AvroTraits.hh"

namespace avro {

ValidatingWriter::ValidatingWriter(const ValidSchema &schema, OutputStreamer &out) :
    validator_(schema),
    writer_(out)
{ }

void
ValidatingWriter::putBytes(const void *val, size_t size)
{
    checkSafeToPut(AVRO_BYTES);
    writer_.putBytes(val, size);
    validator_.advance();
}

void 
ValidatingWriter::putCount(int64_t count)
{
    checkSafeToPut(AVRO_LONG);
    writer_.putValue(count);
    validator_.advanceWithCount(count);
}

void 
ValidatingWriter::beginRecord()
{
    checkSafeToPut(AVRO_RECORD);
    validator_.advance();
}

void 
ValidatingWriter::beginArrayBlock(int64_t size)
{
    checkSafeToPut(AVRO_ARRAY);
    validator_.advance();
    putCount(size);
}

void 
ValidatingWriter::endArray()
{
    beginArrayBlock(0);
}

void 
ValidatingWriter::beginMapBlock(int64_t size)
{
    checkSafeToPut(AVRO_MAP);
    validator_.advance();
    putCount(size);
}

void 
ValidatingWriter::endMap()
{
    beginMapBlock(0);
}

void 
ValidatingWriter::beginUnion(int64_t choice)
{
    checkSafeToPut(AVRO_UNION);
    validator_.advance();
    putCount(choice);
}

void 
ValidatingWriter::beginEnum(int64_t choice)
{
    checkSafeToPut(AVRO_ENUM);
    validator_.advance();
    putCount(choice);
}


} // namepspace avro
