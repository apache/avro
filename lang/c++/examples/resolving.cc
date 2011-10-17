#include <fstream>

#include "cpx.hh"
#include "imaginary.hh"

#include "avro/Compiler.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"
#include "avro/Specific.hh"
#include "avro/Generic.hh"



avro::ValidSchema load(const char* filename)
{
    std::ifstream ifs(filename);
    avro::ValidSchema result;
    avro::compileJsonSchema(ifs, result);
    return result;
}

int
main()
{
    avro::ValidSchema cpxSchema = load("cpx.json");
    avro::ValidSchema imaginarySchema = load("imaginary.json");

    std::auto_ptr<avro::OutputStream> out = avro::memoryOutputStream();
    avro::EncoderPtr e = avro::binaryEncoder();
    e->init(*out);
    c::cpx c1;
    c1.re = 100.23;
    c1.im = 105.77;
    avro::encode(*e, c1);

    std::auto_ptr<avro::InputStream> in = avro::memoryInputStream(*out);
    avro::DecoderPtr d = avro::resolvingDecoder(cpxSchema, imaginarySchema,
        avro::binaryDecoder());
    d->init(*in);

    i::cpx c2;
    avro::decode(*d, c2);
    std::cout << "Imaginary: " << c2.im << std::endl;

}
