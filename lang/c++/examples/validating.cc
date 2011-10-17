#include <fstream>
#include <complex>

#include "avro/Compiler.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"
#include "avro/Specific.hh"

namespace avro {
template<typename T>
struct codec_traits<std::complex<T> > {
    static void encode(Encoder& e, const std::complex<T>& c) {
        avro::encode(e, std::real(c));
        avro::encode(e, std::imag(c));
    }

    static void decode(Decoder& d, std::complex<T>& c) {
        T re, im;
        avro::decode(d, re);
        avro::decode(d, im);
        c = std::complex<T>(re, im);
    }
};

}
int
main()
{
    std::ifstream ifs("cpx.json");

    avro::ValidSchema cpxSchema;
    avro::compileJsonSchema(ifs, cpxSchema);

    std::auto_ptr<avro::OutputStream> out = avro::memoryOutputStream();
    avro::EncoderPtr e = avro::validatingEncoder(cpxSchema,
        avro::binaryEncoder());
    e->init(*out);
    std::complex<double> c1(1.0, 2.0);
    avro::encode(*e, c1);

    std::auto_ptr<avro::InputStream> in = avro::memoryInputStream(*out);
    avro::DecoderPtr d = avro::validatingDecoder(cpxSchema,
        avro::binaryDecoder());
    d->init(*in);

    std::complex<double> c2;
    avro::decode(*d, c2);
    std::cout << '(' << std::real(c2) << ", " << std::imag(c2) << ')' << std::endl;
    return 0;
}
