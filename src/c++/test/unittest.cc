#include <iostream>
#include <fstream>
#include <cassert>
#include <sstream>

#include "Zigzag.hh"
#include "Node.hh"
#include "Schema.hh"
#include "ValidSchema.hh"
#include "OutputStreamer.hh"
#include "Serializer.hh"
#include "Parser.hh"
#include "ValidatingSerializer.hh"
#include "ValidatingParser.hh"
#include "SymbolMap.hh"

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
        record.addField("myenum", myenum); 

        UnionSchema onion;
        onion.addType(NullSchema());
        onion.addType(map);
        onion.addType(FloatSchema());
       
        record.addField("myunion", onion); 

        record.addField("mybool", BoolSchema());
        FixedSchema fixed(16, "fixed16");
        record.addField("myfixed", fixed);
        record.addField("anotherint", intSchema);

        schema_.setSchema(record);
    }

    template<typename Serializer>
    void printUnion(Serializer &s, int path)
    {
        s.beginUnion(path);
        if(path == 0) {
            std::cout << "Null in union\n";
            s.putNull();
        }
        else if(path == 1) {
            std::cout << "Map in union\n";
            s.beginMapBlock(2);
            s.putString("Foo");
            s.putInt(16);
            s.putString("Bar");
            s.putInt(17);
            s.beginMapBlock(1);
            s.putString("FooBar");
            s.putInt(18);
            s.endMap();
        }
        else {
            std::cout << "Float in union\n";
            s.putFloat(200.);
        }
    }

    template<typename Serial>
    void writeEncoding(Serial &s, int path)
    {

        std::cout << "Record\n";
        s.beginRecord();
        s.putInt(212);

        std::cout << "Map\n";
        s.beginMapBlock(2);
        s.putString(std::string("Foo"));
        s.putInt(16);
        s.putString(std::string("Bar"));
        s.putInt(17);
        s.endMap();

        std::cout << "Array\n";
        s.beginArrayBlock(2);
        s.putDouble(100.0);
        s.putDouble(1000.0);
        s.endArray();

        std::cout << "Enum\n";
        s.beginEnum(3);

        std::cout << "Union\n";
        printUnion(s, path);

        std::cout << "Bool\n";
        s.putBool(true);

        std::cout << "Fixed16\n";
        
        s.putFixed(fixeddata, 16);

        std::cout << "Int\n";
        s.putInt(-1);
    }

    void printEncoding() {
        std::cout << "Encoding\n";
        ScreenStreamer os;
        Serializer s(os);
        writeEncoding(s, 0);
    }

    void printValidatingEncoding(int path)
    {
        std::cout << "Validating Encoding " << path << "\n";
        ScreenStreamer os;
        ValidatingSerializer s(schema_, os);
        writeEncoding(s, path);
    }

    void saveValidatingEncoding(int path) 
    {
        std::ofstream out("test.avro");
        OStreamer os(out);
        ValidatingSerializer s(schema_, os);
        writeEncoding(s, path);
    }

    void printNext(Parser &p) {
    }

    void printNext(ValidatingParser &p)
    {
        std::cout << "Next: \"" << p.nextType();
        std::string recordName;
        std::string fieldName;
        if( p.getCurrentRecordName(recordName) ) {
            std::cout << "\" record: \"" << recordName;
        }
        if( p.getNextFieldName(fieldName) ) {
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
            size = p.getMapBlockSize();
            std::cout << "Size " << size << '\n';
            for(int32_t i=0; i < size; ++i) {
                std::string key;
                printNext(p);
                p.getString(key);
                printNext(p);
                int32_t intval = p.getInt();
                std::cout << key << ":" << intval << '\n';
            }
        } while (size != 0);
    }

    template <typename Parser>
    void readArray(Parser &p)
    {
        int64_t size = 0;
        do {
            printNext(p);
            size = p.getArrayBlockSize();
            std::cout << "Size " << size << '\n';
            for(int32_t i=0; i < size; ++i) {
                printNext(p);
                double d = p.getDouble();
                std::cout << i << ":" << d << '\n';
            }
        } while(size != 0);
    }

    template <typename Parser>
    void readFixed(Parser &p) {

        std::vector<uint8_t> input;
        p.getFixed(input, 16);

        for(int i=0; i< 16; ++i) {
            std::cout << static_cast<int>(input[i]) << ' ';
        }
        std::cout << '\n';
    }

    template <typename Parser>
    void readData(Parser &p)
    {
        printNext(p);
        p.getRecord();

        printNext(p);
        int64_t longval = p.getLong();
        std::cout << longval << '\n';

        readMap(p);
        readArray(p);

        printNext(p);
        longval = p.getEnum();
        std::cout << "Enum choice " << longval << '\n';

        printNext(p);
        longval = p.getUnion();
        std::cout << "Union path " << longval << '\n';
        readMap(p);

        printNext(p);
        bool boolval = p.getBool();
        std::cout << boolval << '\n';

        printNext(p);
        readFixed(p);

        printNext(p);
        int32_t intval = p.getInt();
        std::cout << intval << '\n';
    }

    void readRawData() {
        std::ifstream in("test.avro");
        IStreamer ins(in);
        Parser p(ins);
        readData(p);
    }

    void readValidatedData()
    {
        std::ifstream in("test.avro");
        IStreamer ins(in);
        ValidatingParser p(schema_, ins);
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
        if (decodeZigzag32(encoded) != val) {
            std::cout << val << '\n';
            std::cout << encoded << '\n';
            std::cout << decodeZigzag32(encoded) << '\n';
            assert(0);
        }
    }

    void compare(int64_t val) {
        uint64_t encoded = encodeZigzag64(val);
        if (decodeZigzag64(encoded) != val) {
            std::cout << val << '\n';
            std::cout << encoded << '\n';
            std::cout << decodeZigzag64(encoded) << '\n';
            assert(0);
        }
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
        assert(node == 0);

        map_.registerSymbol(rec.root());

        node = map_.locateSymbol(name);
        assert(node);
        assert(node->name() == name);
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
        ValidatingSerializer s(schema_, os);
        s.beginRecord();
        s.putLong(1);
        s.beginUnion(0);
        s.putNull();
        s.putBool(true);
    }

    void serializeRecurse(OutputStreamer &os)
    {
        std::cout << "Recurse\n";
        ValidatingSerializer s(schema_, os);
        s.beginRecord();
        s.putLong(1);
        s.beginUnion(1);
        {
            s.beginRecord();
            s.putLong(2);
            s.beginUnion(1);
            {
                s.beginRecord();
                s.putLong(3);
                s.beginUnion(0);
            }
            s.putNull();
        }
        s.putBool(true);
    }

    void validatingParser(InputStreamer &is) 
    {
        ValidatingParser p(schema_, is);
        int64_t val = 0;
        int64_t path = 0;
    
        do {
            p.getRecord();
            val = p.getLong();
            std::cout << "longval = " << val << '\n';
            path = p.getUnion();
        } while(path == 1);

        p.getNull();
        bool b = p.getBool();
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
        Serializer s(os);

        serialize(s, val);
        serialize(s, Null());
        serialize(s, f);
        

    }
};


int main()
{
    try {
        TestEncoding test1;
        test1.test();

        TestSchema test2;
        test2.test();

        TestSymbolMap test3;
        test3.test();

        TestNested test4;
        test4.test();

        TestGenerated test5;
        test5.test();
    }
    catch (std::exception &e) {
        std::cout << "Failed unit test due to exception: " << e.what() << std::endl;
    }

    return 0;
}
