#ifndef avro_ValidatingSerializer_hh__
#define avro_ValidatingSerializer_hh__

#include <boost/noncopyable.hpp>

#include "Serializer.hh"
#include "Validator.hh"

namespace avro {

class ValidSchema;
class OutputStreamer;

/// This class walks the parse tree as data is being serialized, and throws if
/// attempt to serialize a data type does not match the type expected in the
/// schema.

class ValidatingSerializer : private boost::noncopyable
{

  public:

    ValidatingSerializer(const ValidSchema &schema, OutputStreamer &out);

    void putNull();

    void putBool(bool val);

    void putInt(int32_t val);

    void putLong(int64_t val);

    void putFloat(float val);

    void putDouble(double val);

    void putString(const std::string &val);

    void putBytes(const uint8_t *val, size_t size);

    void putFixed(const uint8_t *val, size_t size) {
        checkSafeToPut(AVRO_FIXED);
        checkSizeExpected(size);
        serializer_.putFixed(val, size);
        validator_.advance();
    }

    void beginRecord();

    void beginArrayBlock(int64_t size);
    void endArray();

    void beginMapBlock(int64_t size);
    void endMap();

    void beginUnion(int64_t choice);

    void beginEnum(int64_t choice);

  private:

    void putCount(int64_t count);

    void checkSafeToPut(Type type) const {
        if(! validator_.typeIsExpected(type)) {
            throw Exception("Type does not match schema");
        }
    }

    void checkSizeExpected(int size) const {
        if(validator_.nextSizeExpected() != size) {
            throw Exception("Wrong size of for fixed");
        }
    }

    Validator  validator_;
    Serializer serializer_;

};

} // namespace avro

#endif
