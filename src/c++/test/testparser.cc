#include <stdio.h>
#include <stdlib.h>

#include "Compiler.hh"
#include "ValidSchema.hh"

int main()
{

    try {
        avro::ValidSchema schema;
        avro::compileJsonSchema(std::cin, schema);

        schema.toJson(std::cout);
    }
    catch (std::exception &e) {
        std::cout << "Failed to parse or compile schema: " << e.what() << std::endl;
    }

    return 0;
}
