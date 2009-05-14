Avro README

Avro is a data serialization system.

Please build and read the documentation for more details.

WARNING: Avro is a work-in-progress, not yet a finished product.  Its
current intended audience is developers interested in collaborating to
make it more useful.  Do *not* yet use Avro for persistent data in
production systems, as the data format may be incompatibly changed.

REQUIREMENTS

The following packages must be installed before Avro can be built:

 - Java 1.6
 - Python 2.5 or greater
 - gcc, automake, libtool, libapr1-dev, libaprutil1-dev
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
