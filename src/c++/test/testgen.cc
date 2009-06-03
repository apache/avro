
#include <string.h>
#include <fstream>
#include <sstream>

#include "code.hh"
#include "OutputStreamer.hh"
#include "InputStreamer.hh"
#include "Serializer.hh"
#include "ValidatingSerializer.hh"
#include "Parser.hh"
#include "ValidatingParser.hh"
#include "ValidSchema.hh"
#include "Compiler.hh"

void serialize(const avrouser::RootRecord &rec) 
{
    avro::ScreenStreamer os;
    avro::Serializer s (os);

    avro::serialize(s, rec);
}

void serializeValid(const avro::ValidSchema &valid, const avrouser::RootRecord &rec) 
{
    avro::ScreenStreamer os;
    avro::ValidatingSerializer s (valid, os);

    avro::serialize(s, rec);
}

void checkArray(const avrouser::Array_of_double &a1, const avrouser::Array_of_double &a2) 
{
    assert(a1.value.size() == a2.value.size());
    for(size_t i = 0; i < a1.value.size(); ++i) {
        assert(a1.value[i] == a2.value[i]);
    }
}

void checkMap(const avrouser::Map_of_int &map1, const avrouser::Map_of_int &map2) 
{
    assert(map1.value.size() == map2.value.size());
    avrouser::Map_of_int::MapType::const_iterator iter1 = map1.value.begin();
    avrouser::Map_of_int::MapType::const_iterator end   = map1.value.end();
    avrouser::Map_of_int::MapType::const_iterator iter2 = map2.value.begin();

    while(iter1 != end) {
        assert(iter1->first == iter2->first);
        assert(iter1->second == iter2->second);
        ++iter1;
        ++iter2;
    }
}

void checkOk(const avrouser::RootRecord &rec1, const avrouser::RootRecord &rec2)
{
    assert(rec1.mylong == rec1.mylong);
    checkMap(rec1.mymap, rec2.mymap);
    checkArray(rec1.myarray, rec2.myarray);

    assert(rec1.myenum.value == rec2.myenum.value);

    assert(rec1.myunion.choice == rec2.myunion.choice);
    // in this test I know choice was 1
    {
        assert(rec1.myunion.choice == 1);
        checkMap(rec1.myunion.getValue<avrouser::Map_of_int>(), rec2.myunion.getValue<avrouser::Map_of_int>());
    }

    assert(rec1.mybool == rec2.mybool);
    for(int i = 0; i < static_cast<int>(avrouser::md5::fixedSize); ++i) {
        assert(rec1.myfixed.value[i] == rec2.myfixed.value[i]);
    }
    assert(rec1.anotherint == rec1.anotherint);

}

void testParser(const avrouser::RootRecord &myRecord)
{
    std::ostringstream ostring;
    avro::OStreamer os(ostring);
    avro::Serializer s (os);

    avro::serialize(s, myRecord); 

    avrouser::RootRecord inRecord;
    std::istringstream istring(ostring.str());
    avro::IStreamer is(istring);
    avro::Parser p(is);
    avro::parse(p, inRecord);

    checkOk(myRecord, inRecord);
}

void testParserValid(avro::ValidSchema &valid, const avrouser::RootRecord &myRecord)
{
    std::ostringstream ostring;
    avro::OStreamer os(ostring);
    avro::ValidatingSerializer s (valid, os);

    avro::serialize(s, myRecord);

    avrouser::RootRecord inRecord;
    std::istringstream istring(ostring.str());
    avro::IStreamer is(istring);
    avro::ValidatingParser p(valid, is);
    avro::parse(p, inRecord);

    checkOk(myRecord, inRecord);
}

void runTests(const avrouser::RootRecord myRecord) 
{
    std::cout << "Serialize:\n";
    serialize(myRecord);
    std::cout << "end Serialize\n";

    avro::ValidSchema schema;
    std::ifstream in("jsonschemas/bigrecord");
    avro::compileJsonSchema(in, schema);
    std::cout << "Serialize validated:\n";
    serializeValid(schema, myRecord);
    std::cout << "end Serialize validated\n";

    testParser(myRecord);

    testParserValid(schema, myRecord);
}

int main() 
{
    uint8_t fixed[] =  {0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

    avrouser::RootRecord myRecord;
    myRecord.mylong = 212;
    myRecord.mymap.value.clear();
    myRecord.myarray.addValue(3434.9);
    myRecord.myarray.addValue(7343.9);
    myRecord.myenum.value = avrouser::ExampleEnum::one;
    avrouser::Map_of_int map;
    map.addValue("one", 1);
    map.addValue("two", 2);
    myRecord.myunion.set_Map_of_int(map);
    myRecord.mybool = true;
    memcpy(myRecord.myfixed.value, fixed, avrouser::md5::fixedSize);
    myRecord.anotherint = 4534;

    runTests(myRecord);


}
