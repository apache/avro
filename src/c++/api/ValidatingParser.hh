#ifndef avro_ValidatingParser_hh__
#define avro_ValidatingParser_hh__

#include <stdint.h>
#include <vector>
#include <boost/noncopyable.hpp>

#include "Parser.hh"
#include "Validator.hh"

namespace avro {

class ValidSchema;
class InputStreamer;

/// As an avro object is being parsed from binary data to its C++
/// representation, this parser will walk the parse tree and ensure that the
/// correct type is being asked for.  If the user attempts to parse a type that
/// does not match what the schema says, an exception will be thrown.  
///
/// The ValidatingParser object can also be used to tell what the next type is,
/// so that callers can dynamically discover the contents.  It also tells
/// the attribute names of the objects or their fields, if they exist.
///

class ValidatingParser : private boost::noncopyable
{

  public:

    explicit ValidatingParser(const ValidSchema &schema, InputStreamer &in);

    void getNull();

    bool getBool();

    int32_t getInt();

    int64_t getLong();

    float getFloat();

    double getDouble();

    void getBytes(std::vector<uint8_t> &val);

    void getFixed(uint8_t *val, size_t size) {
        checkSafeToGet(AVRO_FIXED);
        checkSizeExpected(size);
        validator_.advance();
        parser_.getFixed(val, size);
    }

    void getFixed(std::vector<uint8_t> &val, size_t size) {
        checkSafeToGet(AVRO_FIXED);
        checkSizeExpected(size);
        validator_.advance();
        parser_.getFixed(val, size);
    }

    void getString(std::string &val);

    void getRecord();

    int64_t getArrayBlockSize();

    int64_t getUnion();

    int64_t getEnum();

    int64_t getMapBlockSize();

    Type nextType() const{
        return validator_.nextTypeExpected();
    }

    bool getCurrentRecordName(std::string &name) const {
        return validator_.getCurrentRecordName(name);
    }

    bool getNextFieldName(std::string &name) const {
        return validator_.getNextFieldName(name);
    }

  private:

    int64_t getCount();

    void checkSafeToGet(Type type) const {
        if(validator_.nextTypeExpected() != type) {
            throw Exception("Type does not match");
        }
    }

    void checkSizeExpected(int size) const {
        if(validator_.nextSizeExpected() != size) {
            throw Exception("Wrong size of for fixed");
        }
    }

    Validator validator_;
    Parser parser_;
};


} // namespace avro

#endif
