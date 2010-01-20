Avro README

Avro is a data serialization system.

Please build and read the documentation for more details.

REQUIREMENTS

The following packages must be installed before Avro can be built:

 - Java: JDK 1.6
 - Python: 2.5 or greater
 - C: gcc, autoconf, automake, libtool, asciidoc 
 - C++: g++, flex, bison, libboost-dev
 - Ruby: ruby, gem, rake, echoe, jajl-ruby
 - Apache Ant 1.7
 - Apache Forrest 0.8 (for documentation, requires Java 1.5)

BUILDING

One the requirements are installed, build.sh can used as follows:

 './build.sh test' runs tests for all languages
 './build.sh dist' creates all release distribution files in dist/
 './build.sh clean' removes all generated artifacts
