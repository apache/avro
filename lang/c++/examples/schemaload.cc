#include <fstream>

#include "avro/ValidSchema.hh"
#include "avro/Compiler.hh"


int
main()
{
    std::ifstream in("cpx.json");

    avro::ValidSchema cpxSchema;
    avro::compileJsonSchema(in, cpxSchema);
}
