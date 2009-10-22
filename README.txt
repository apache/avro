Avro README

Avro is a data serialization system.

Please build and read the documentation for more details.

REQUIREMENTS

The following packages must be installed before Avro can be built:

 - Java: JDK 1.6
 - Python: 2.5 or greater
 - C: gcc, automake, libtool, libapr1-dev, libaprutil1-dev, doxygen
 - C++: g++, flex, bison, libboost-dev
 - Apache Ant 1.7
 - Apache Forrest 0.8 (for documentation, requires Java 1.5)

BUILDING

One the requirements are installed, Ant can be used as follows:

 'ant javadoc' builds Java API documentation to build/doc/api
 'ant doc' builds Avro specification document in build/doc/spec.html
 'ant test' runs tests in src/test
 'ant jar' creates a jar in build/avro-X.X.jar
 'ant tar' makes a "release" with docs, jar, src, etc. in build/avro-X.X.tar.gz
 'ant clean' removes all generated artifacts

USING ECLIPSE

To use Eclipse, use the "ant eclipse" target to trigger the generation
of an Eclipse project.  This project is automatically created by 
the ant-eclipse tool (see http://ant-eclipse.sourceforge.net/).
Note that to build completely, you still have to use "ant", because
the Eclipse project depends on some generated code.

You should be able to run all the java tests in Eclipse, except
for the InteropTest.  There is a launcher in .eclipse_launchers,
that should work if your checkout directory is "avro".
