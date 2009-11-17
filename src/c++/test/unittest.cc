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

#include <iostream>
#include <fstream>
#include <sstream>
#include <boost/test/included/unit_test_framework.hpp>

#include "Zigzag.hh"
#include "Node.hh"
#include "Schema.hh"
#include "ValidSchema.hh"
#include "OutputStreamer.hh"
#include "Serializer.hh"
#include "Parser.hh"
#include "SymbolMap.hh"
#include "Compiler.hh"
#include "SchemaResolution.hh"

#include "AvroSerialize.hh"

using namespace avro;

static const uint8_t fixeddata[16] = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};

struct TestSchema
{
    TestSchema() 
    {}

    void buildSchema()
    {
        RecordSchema record("RootRecord");

        record.addField("mylong", LongSchema());

        IntSchema intSchema;
        avro::MapSchema map = MapSchema(IntSchema());

        record.addField("mymap", map);

        ArraySchema array = ArraySchema(DoubleSchema());

        const std::string s("myarray");
        record.addField(s, array);

        EnumSchema myenum("ExampleEnum");
        myenum.addSymbol("zero");
        myenum.addSymbol("one");
        myenum.addSymbol("two");
        myenum.addSymbol("three");

        bool caught = false;
        try {
            myenum.addSymbol("three");
        }
        catch(Exception &e) {
            std::cout << "(intentional) exception: " << e.what() << '\n';
            caught = true;
        }
        BOOST_CHECK_EQUAL(caught, true);

        record.addField("myenum", myenum); 

        UnionSchema onion;
        onion.addType(NullSchema());
        onion.addType(map);
        onion.addType(FloatSchema());
       
        record.addField("myunion", onion); 

        RecordSchema nestedRecord("NestedRecord");
        nestedRecord.addField("floatInNested", FloatSchema());

        record.addField("nested", nestedRecord);

        record.addField("mybool", BoolSchema());
        FixedSchema fixed(16, "fixed16");
        record.addField("myfixed", fixed);

        caught = false;
        try {
            record.addField("mylong", LongSchema());
        }
        catch(Exception &e) {
            std::cout << "(intentional) exception: " << e.what() << '\n';
            caught = true;
        }
        BOOST_CHECK_EQUAL(caught, true);

        record.addField("mylong2", LongSchema());

        record.addField("anotherint", intSchema);

        schema_.setSchema(record);
    }

    void checkNameLookup() {
        NodePtr node = schema_.root();

        size_t index = 0;
        bool found = node->nameIndex("mylongxxx", index);
        BOOST_CHECK_EQUAL(found, false);

        found = node->nameIndex("mylong", index);
        BOOST_CHECK_EQUAL(found, true);
        BOOST_CHECK_EQUAL(index, 0U);

        found = node->nameIndex("mylong2", index);
        BOOST_CHECK_EQUAL(found, true);
        BOOST_CHECK_EQUAL(index, 8U);

        found = node->nameIndex("myenum", index);
        BOOST_CHECK_EQUAL(found, true);
        NodePtr enumNode = node->leafAt(index);

        found = enumNode->nameIndex("one", index);
        BOOST_CHECK_EQUAL(found, true);
        BOOST_CHECK_EQUAL(index, 1U);

        found = enumNode->nameIndex("three", index);
        BOOST_CHECK_EQUAL(found, true);
        BOOST_CHECK_EQUAL(index, 3U);

        found = enumNode->nameIndex("four", index);
        BOOST_CHECK_EQUAL(found, false);
    }

    template<typename Serializer>
    void printUnion(Serializer &s, int path)
    {
        s.writeUnion(path);
        if(path == 0) {
            std::cout << "Null in union\n";
            s.writeNull();
        }
        else if(path == 1) {
            std::cout << "Map in union\n";
            s.writeMapBlock(2);
            s.writeString("Foo");
            s.writeInt(16);
            s.writeString("Bar");
            s.writeInt(17);
            s.writeMapBlock(1);
            s.writeString("FooBar");
            s.writeInt(18);
            s.writeMapEnd();
        }
        else {
            std::cout << "Float in union\n";
            s.writeFloat(200.);
        }
    }

    template<typename Serializer>
    void writeEncoding(Serializer &s, int path)
    {
        std::cout << "Record\n";
        s.writeRecord();
        s.writeInt(1000);

        std::cout << "Map\n";
        s.writeMapBlock(2);
        s.writeString(std::string("Foo"));
        s.writeInt(16);
        s.writeString(std::string("Bar"));
        s.writeInt(17);
        s.writeMapEnd();

        std::cout << "Array\n";
        s.writeArrayBlock(2);
        s.writeDouble(100.0);
        s.writeDouble(1000.0);
        s.writeArrayEnd();

        std::cout << "Enum\n";
        s.writeEnum(3);

        std::cout << "Union\n";
        printUnion(s, path);

        std::cout << "Record\n";
        s.writeRecord();
        s.writeFloat(-101.101f);

        std::cout << "Bool\n";
        s.writeBool(true);

        std::cout << "Fixed16\n";
        
        s.writeFixed(fixeddata);

        std::cout << "Long\n";
        s.writeLong(7010728798977672067LL);

        std::cout << "Int\n";
        s.writeInt(-3456);
    }

    void printEncoding() {
        std::cout << "Encoding\n";
        ScreenStreamer os;
        Serializer<Writer> s(os);
        writeEncoding(s, 0);
    }

    void printValidatingEncoding(int path)
    {
        std::cout << "Validating Encoding " << path << "\n";
        ScreenStreamer os;
        Serializer<ValidatingWriter> s(schema_, os);
        writeEncoding(s, path);
    }

    void saveValidatingEncoding(int path) 
    {
        std::ofstream out("test.avro");
        OStreamer os(out);
        Serializer<ValidatingWriter> s(schema_, os);
        writeEncoding(s, path);
    }

    void printNext(Parser<Reader> &p) {
        // no-op printer
    }

    void printNext(Parser<ValidatingReader> &p)
    {
        std::cout << "Next: \"" << nextType(p);
        std::string recordName;
        std::string fieldName;
        if( currentRecordName(p, recordName) ) {
            std::cout << "\" record: \"" << recordName;
        }
        if( nextFieldName(p, fieldName) ) {
            std::cout << "\" field: \"" << fieldName;
        }
        std::cout << "\"\n";

    }

    template <typename Parser>
    void readMap(Parser &p)
    {
        int64_t size = 0;
        do { 
            printNext(p);
            size = p.readMapBlockSize();
            std::cout << "Size " << size << '\n';
            for(int64_t i=0; i < size; ++i) {
                std::string key;
                printNext(p);
                p.readString(key);
                printNext(p);
                int32_t intval = p.readInt();
                std::cout << key << ":" << intval << '\n';
            }
        } while (size != 0);
    }

    template <typename Parser>
    void readArray(Parser &p)
    {
        int64_t size = 0;
        double d = 0.0;
        do {
            printNext(p);
            size = p.readArrayBlockSize();
            std::cout << "Size " << size << '\n';
            for(int64_t i=0; i < size; ++i) {
                printNext(p);
                d = p.readDouble();
                std::cout << i << ":" << d << '\n';
            }
        } while(size != 0);
        BOOST_CHECK_EQUAL(d, 1000.0);
    }

    template <typename Parser>
    void readNestedRecord(Parser &p)
    {
        printNext(p);
        p.readRecord();
        printNext(p);
        float f = p.readFloat();
        std::cout << f << '\n';
        BOOST_CHECK_EQUAL(f, -101.101f);
    }

    template <typename Parser>
    void readFixed(Parser &p) {

        boost::array<uint8_t, 16> input;
        p.readFixed<16>(input);
        BOOST_CHECK_EQUAL(input.size(), 16U);

        for(int i=0; i< 16; ++i) {
            std::cout << static_cast<int>(input[i]) << ' ';
        }
        std::cout << '\n';
    }

    template <typename Parser>
    void readData(Parser &p)
    {
        printNext(p);
        p.readRecord();

        printNext(p);
        int64_t longval = p.readLong();
        std::cout << longval << '\n';
        BOOST_CHECK_EQUAL(longval, 1000);

        readMap(p);
        readArray(p);

        printNext(p);
        longval = p.readEnum();
        std::cout << "Enum choice " << longval << '\n';

        printNext(p);
        longval = p.readUnion();
        std::cout << "Union path " << longval << '\n';
        readMap(p);

        readNestedRecord(p);

        printNext(p);
        bool boolval = p.readBool();
        std::cout << boolval << '\n';
        BOOST_CHECK_EQUAL(boolval, true);

        printNext(p);
        readFixed(p);

        printNext(p);
        longval = p.readLong();
        std::cout << longval << '\n';
        BOOST_CHECK_EQUAL(longval, 7010728798977672067LL);

        printNext(p);
        int32_t intval = p.readInt();
        std::cout << intval << '\n';
        BOOST_CHECK_EQUAL(intval, -3456);
    }

    void readRawData() {
        std::ifstream in("test.avro");
        IStreamer ins(in);
        Parser<Reader> p(ins);
        readData(p);
    }

    void readValidatedData()
    {
        std::ifstream in("test.avro");
        IStreamer ins(in);
        Parser<ValidatingReader> p(schema_, ins);
        readData(p);
    }

    void test()
    {
        std::cout << "Before\n";
        schema_.toJson(std::cout);
        schema_.toFlatList(std::cout);
        buildSchema();
        std::cout << "After\n";
        schema_.toJson(std::cout);
        schema_.toFlatList(std::cout);

        checkNameLookup();

        printEncoding();
        printValidatingEncoding(0);
        printValidatingEncoding(1);
        printValidatingEncoding(2);

        saveValidatingEncoding(1);
        readRawData();
        readValidatedData();
    }

    ValidSchema schema_;
};

struct TestEncoding {


    void compare(int32_t val) {
        uint32_t encoded = encodeZigzag32(val);
        BOOST_CHECK_EQUAL(decodeZigzag32(encoded), val);
    }

    void compare(int64_t val) {
        uint64_t encoded = encodeZigzag64(val);
        BOOST_CHECK_EQUAL(decodeZigzag64(encoded), val);
    }

    template<typename IntType>
    void testEncoding(IntType start, IntType stop)
    {
        std::cout << "testing from " << start << " to " << stop << " inclusive\n";
        IntType val = start;
        IntType diff = stop - start + 1;

        for(IntType i = 0; i < diff; ++i) {
            compare(val++);
        }
    }

    template<typename IntType>
    void testEncoding()
    {
        testEncoding<IntType>(std::numeric_limits<IntType>::min(), std::numeric_limits<IntType>::min() + 1000);
        testEncoding<IntType>(-1000, 1000);
        testEncoding<IntType>(std::numeric_limits<IntType>::max()-1000, std::numeric_limits<IntType>::max());
    }

    void test() {
        testEncoding<int32_t>();
        testEncoding<int64_t>();
    }

};

struct TestSymbolMap
{
    TestSymbolMap()
    {}

    void test() 
    {
        std::cout << "TestSymbolMap\n";
        std::string name("myrecord");

        RecordSchema rec(name);

        NodePtr node = map_.locateSymbol(name);
        BOOST_CHECK(node == 0);

        map_.registerSymbol(rec.root());

        node = map_.locateSymbol(name);
        BOOST_CHECK(node);
        BOOST_CHECK_EQUAL(node->name(), name);
        std::cout << "Found " << name << " registered\n";
    }

    SymbolMap map_;
};

struct TestNested
{
    TestNested()
    {}

    void createSchema() 
    {
        std::cout << "TestNested\n";
        RecordSchema rec("LongList");
        rec.addField("value", LongSchema());
        UnionSchema next;
        next.addType(NullSchema());
        next.addType(rec);
        rec.addField("next", next);
        rec.addField("end", BoolSchema());

        schema_.setSchema(rec);
        schema_.toJson(std::cout);
        schema_.toFlatList(std::cout);
    }

    void serializeNoRecurse(OutputStreamer &os)
    {
        std::cout << "No recurse\n";
        Serializer<ValidatingWriter> s(schema_, os);
        s.writeRecord();
        s.writeLong(1);
        s.writeUnion(0);
        s.writeNull();
        s.writeBool(true);
    }

    void serializeRecurse(OutputStreamer &os)
    {
        std::cout << "Recurse\n";
        Serializer<ValidatingWriter> s(schema_, os);
        s.writeRecord();
        s.writeLong(1);
        s.writeUnion(1);
        {
            s.writeRecord();
            s.writeLong(2);
            s.writeUnion(1);
            {
                s.writeRecord();
                s.writeLong(3);
                s.writeUnion(0);
            }
            s.writeNull();
        }
        s.writeBool(true);
    }

    void validatingParser(InputStreamer &is) 
    {
        Parser<ValidatingReader> p(schema_, is);
        int64_t val = 0;
        int64_t path = 0;
    
        do {
            p.readRecord();
            val = p.readLong();
            std::cout << "longval = " << val << '\n';
            path = p.readUnion();
        } while(path == 1);

        p.readNull();
        bool b = p.readBool();
        std::cout << "bval = " << b << '\n';
    }

    void testToScreen() {
        ScreenStreamer os;
        serializeNoRecurse(os);
        serializeRecurse(os);
    }

    void testParseNoRecurse() {
        std::ostringstream ostring;
        OStreamer os(ostring);
        serializeNoRecurse(os);
        std::cout << "ParseNoRecurse\n";

        std::istringstream istring(ostring.str());
        IStreamer is(istring);
        validatingParser(is);
    }

    void testParseRecurse() {
        std::ostringstream ostring;
        OStreamer os(ostring);
        serializeRecurse(os);
        std::cout << "ParseRecurse\n";

        std::istringstream istring(ostring.str());
        IStreamer is(istring);
        validatingParser(is);
    }


    void test() {
        createSchema();
        testToScreen();

        testParseNoRecurse();
        testParseRecurse();

    }

    ValidSchema schema_;
};

struct TestGenerated
{
    TestGenerated()
    {}

    void test() 
    {
        std::cout << "TestGenerated\n";

        int32_t val = 100;
        float   f   = 200.0;

        ScreenStreamer os;
        Writer writer(os);

        serialize(writer, val);
        serialize(writer, Null());
        serialize(writer, f);
        
    }
};

struct TestBadStuff
{
    void testBadFile() 
    {
        std::cout << "TestBadFile\n";

        avro::ValidSchema schema;
        std::ifstream in("agjoewejefkjs");
        std::string error;
        bool result = avro::compileJsonSchema(in, schema, error);
        BOOST_CHECK_EQUAL(result, false);
        std::cout << "(intentional) error: " << error << '\n';
    }

    void testBadSchema()
    {
        std::cout << "TestBadSchema\n";

        std::string str ("{ \"type\" : \"wrong\" }");
        std::istringstream in(str);

        avro::ValidSchema schema;
        std::string error;
        bool result = avro::compileJsonSchema(in, schema, error);
        BOOST_CHECK_EQUAL(result, false);
        std::cout << "(intentional) error: " << error << '\n';
    }

    void test() 
    {
        std::cout << "TestBadStuff\n";
        testBadFile();
        testBadSchema();
    }
};


template<typename T>
void addTestCase(boost::unit_test::test_suite &test) 
{
    boost::shared_ptr<T> newtest( new T );
    test.add( BOOST_CLASS_TEST_CASE( &T::test, newtest ));
}

struct TestResolution
{
    TestResolution() :
        int_(IntSchema()), 
        long_(LongSchema()),
        bool_(BoolSchema()), 
        float_(FloatSchema()), 
        double_(DoubleSchema()),

        mapOfInt_(MapSchema(IntSchema())),
        mapOfDouble_(MapSchema(DoubleSchema())),

        arrayOfLong_(ArraySchema(LongSchema())),
        arrayOfFloat_(ArraySchema(FloatSchema()))
    {
        {
            EnumSchema one("one");
            one.addSymbol("X");
            enumOne_.setSchema(one);

            EnumSchema two("two");
            two.addSymbol("Y");
            enumTwo_.setSchema(two);
        }
    
        {
            UnionSchema one;
            one.addType(IntSchema());
            one.addType(FloatSchema());
            unionOne_.setSchema(one);

            UnionSchema two;
            two.addType(IntSchema());
            two.addType(DoubleSchema());
            unionTwo_.setSchema(two);
        }
    }

    SchemaResolution resolve(const ValidSchema &writer, const ValidSchema &reader)
    {
        return writer.root()->resolve(*reader.root());
    }

    void test() 
    {
        std::cout << "TestResolution\n";

        BOOST_CHECK_EQUAL(resolve(long_, long_), RESOLVE_MATCH); 
        BOOST_CHECK_EQUAL(resolve(long_, bool_), RESOLVE_NO_MATCH); 
        BOOST_CHECK_EQUAL(resolve(bool_, long_), RESOLVE_NO_MATCH); 

        BOOST_CHECK_EQUAL(resolve(int_, long_), RESOLVE_PROMOTABLE_TO_LONG); 
        BOOST_CHECK_EQUAL(resolve(long_, int_), RESOLVE_NO_MATCH); 

        BOOST_CHECK_EQUAL(resolve(int_, float_), RESOLVE_PROMOTABLE_TO_FLOAT); 
        BOOST_CHECK_EQUAL(resolve(float_, int_), RESOLVE_NO_MATCH); 

        BOOST_CHECK_EQUAL(resolve(int_, double_), RESOLVE_PROMOTABLE_TO_DOUBLE); 
        BOOST_CHECK_EQUAL(resolve(double_, int_), RESOLVE_NO_MATCH); 

        BOOST_CHECK_EQUAL(resolve(long_, float_), RESOLVE_PROMOTABLE_TO_FLOAT); 
        BOOST_CHECK_EQUAL(resolve(float_, long_), RESOLVE_NO_MATCH); 

        BOOST_CHECK_EQUAL(resolve(long_, double_), RESOLVE_PROMOTABLE_TO_DOUBLE); 
        BOOST_CHECK_EQUAL(resolve(double_, long_), RESOLVE_NO_MATCH); 

        BOOST_CHECK_EQUAL(resolve(float_, double_), RESOLVE_PROMOTABLE_TO_DOUBLE); 
        BOOST_CHECK_EQUAL(resolve(double_, float_), RESOLVE_NO_MATCH); 

        BOOST_CHECK_EQUAL(resolve(int_, mapOfInt_), RESOLVE_NO_MATCH);
        BOOST_CHECK_EQUAL(resolve(mapOfInt_, int_), RESOLVE_NO_MATCH);

        BOOST_CHECK_EQUAL(resolve(mapOfInt_, mapOfInt_), RESOLVE_MATCH);
        BOOST_CHECK_EQUAL(resolve(mapOfDouble_, mapOfInt_), RESOLVE_NO_MATCH);
        BOOST_CHECK_EQUAL(resolve(mapOfInt_, mapOfDouble_), RESOLVE_PROMOTABLE_TO_DOUBLE);

        BOOST_CHECK_EQUAL(resolve(long_, arrayOfLong_), RESOLVE_NO_MATCH);
        BOOST_CHECK_EQUAL(resolve(arrayOfLong_, long_), RESOLVE_NO_MATCH);

        BOOST_CHECK_EQUAL(resolve(arrayOfLong_, arrayOfLong_), RESOLVE_MATCH);
        BOOST_CHECK_EQUAL(resolve(arrayOfFloat_, arrayOfLong_), RESOLVE_NO_MATCH);
        BOOST_CHECK_EQUAL(resolve(arrayOfLong_, arrayOfFloat_), RESOLVE_PROMOTABLE_TO_FLOAT);

        BOOST_CHECK_EQUAL(resolve(enumOne_, enumOne_), RESOLVE_MATCH);
        BOOST_CHECK_EQUAL(resolve(enumOne_, enumTwo_), RESOLVE_NO_MATCH);

        BOOST_CHECK_EQUAL(resolve(float_, unionOne_), RESOLVE_MATCH);
        BOOST_CHECK_EQUAL(resolve(double_, unionOne_), RESOLVE_NO_MATCH);
        BOOST_CHECK_EQUAL(resolve(float_, unionTwo_), RESOLVE_PROMOTABLE_TO_DOUBLE);

        BOOST_CHECK_EQUAL(resolve(unionOne_, float_), RESOLVE_MATCH);
        BOOST_CHECK_EQUAL(resolve(unionOne_, double_), RESOLVE_PROMOTABLE_TO_DOUBLE);
        BOOST_CHECK_EQUAL(resolve(unionTwo_, float_), RESOLVE_PROMOTABLE_TO_FLOAT);
        BOOST_CHECK_EQUAL(resolve(unionOne_, unionTwo_), RESOLVE_MATCH);
    }

  private:

    ValidSchema int_;
    ValidSchema long_;
    ValidSchema bool_;
    ValidSchema float_;
    ValidSchema double_;

    ValidSchema mapOfInt_;
    ValidSchema mapOfDouble_;

    ValidSchema arrayOfLong_;
    ValidSchema arrayOfFloat_;

    ValidSchema enumOne_;
    ValidSchema enumTwo_;

    ValidSchema unionOne_;
    ValidSchema unionTwo_;
};


boost::unit_test::test_suite*
init_unit_test_suite( int argc, char* argv[] ) 
{
    using namespace boost::unit_test;

    test_suite* test= BOOST_TEST_SUITE( "Avro C++ unit test suite" );

    addTestCase<TestEncoding>(*test);
    addTestCase<TestSchema>(*test);
    addTestCase<TestSymbolMap>(*test);
    addTestCase<TestNested>(*test);
    addTestCase<TestGenerated>(*test);
    addTestCase<TestBadStuff>(*test);
    addTestCase<TestResolution>(*test);

    return test;
}

