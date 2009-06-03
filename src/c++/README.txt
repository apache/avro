Avro C++ README.txt

The C++ port is thus far incomplete.  Currently, it contains:

 - Serializer/Parser- objects for writing/reading raw binary.

 - xxxSchema- objects for composing schemas.

 - ValidSchema- a schema object that has been converted to a parse tree
   (with some sanity checks). 

 - ValidSchema.toJson() writes the schema as a json object.

 - ValidatingSerializer/ValidatingParser- check that reads/writes
   match the expected schema type (more expensive than the raw
   serializer/parser but they detect errors, and allow dynamic
   discovery of parsed data/attributes).

 - Compiler (compileJsonSchema())- converts a Json string schema to a
   ValidSchema.

 - Code Generation (experimental) - given a schema it generates C++
   objects of the same data types, and the code to serialize and parse
   it.

What's missing: Defaults are not yet supported. Resolving schema
conflicts is not yet supported. And the file and rpc containers are
not yet implemented. Documentation, sparse.

To compile requires boost headers, and the boost regex library.
Additionally, to generate the avro spec compiler requires flex and bison.

As of yet, there is no automatic configuration, so to get started:

 edit (or add) the System.XXX.mk file, where XXX is the result of `uname` on
your machine, to include the paths for:

BOOSTINC (location of boost headers) 
BOOSTREGEX (location of libboost-regex_*.a for your boost)
LEX (path to flex)
YACC (path to bison)

Then type gmake, and it should build everything and run some unit tests.
