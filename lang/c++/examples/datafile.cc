#include <fstream>

#include "cpx.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"
#include "avro/ValidSchema.hh"
#include "avro/Compiler.hh"
#include "avro/DataFile.hh"


avro::ValidSchema loadSchema(const char* filename)
{
    std::ifstream ifs(filename);
    avro::ValidSchema result;
    avro::compileJsonSchema(ifs, result);
    return result;
}

int
main()
{
    avro::ValidSchema cpxSchema = loadSchema("cpx.json");

    {
        avro::DataFileWriter<c::cpx> dfw("test.bin", cpxSchema);
        c::cpx c1;
        for (int i = 0; i < 100; i++) {
            c1.re = i * 100;
            c1.im = i + 100;
            dfw.write(c1);
        }
        dfw.close();
    }

    {
        avro::DataFileReader<c::cpx> dfr("test.bin", cpxSchema);
        c::cpx c2;
        while (dfr.read(c2)) {
            std::cout << '(' << c2.re << ", " << c2.im << ')' << std::endl;
        }
    }
    return 0;
}

