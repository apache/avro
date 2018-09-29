# Apache Avro Build Instructions

## Requirements

The following packages must be installed before Avro can be built:

 - Java: JDK 1.8, Maven 2 or better, protobuf-compile
 - PHP: php5, phpunit, php5-gmp
 - Python: 2.5 or greater, python-setuptools for dist target
 - C: gcc, cmake, asciidoc, source-highlight
 - C++: cmake 2.8.4 or greater, g++, flex, bison, libboost-dev
 - C#: mono-devel mono-gmcs nunit
 - JavaScript: nodejs, npm
 - Ruby: ruby 1.86 or greater, ruby-dev, gem, rake, echoe, yajl-ruby
 - Perl: perl 5.8.1 or greater, gmake, Module::Install,
   Module::Install::ReadmeFromPod, Module::Install::Repository,
   Math::BigInt, JSON::XS, Try::Tiny, Regexp::Common, Encode,
   IO::String, Object::Tiny, Compress::ZLib, Test::More,
   Test::Exception, Test::Pod
 - Apache Ant 1.7
 - Apache Forrest 0.9 (for documentation)
 - md5sum, sha1sum, used by top-level dist target

To simplify this, you can run a Docker container with all the above
dependencies installed by installing Docker and run:

```bash
./build.sh docker
docker@539f6535c9db:~/avro$ cd lang/java/
docker@539f6535c9db:~/avro/lang/java$ ./build.sh test
[INFO] Scanning for projects...
```

When this completes you will be in a shell running in the
container. Building the image the first time may take a while (20
minutes or more) since dependencies must be downloaded and
installed. However subsequent invocations are much faster as the
cached image is used.

The working directory in the container is mounted from your host. This
allows you to access the files in your Avro development tree from the
Docker container.

## Building

Once the requirements are installed (or from the Docker container),
build.sh can be used as follows:

```
./build.sh test # runs tests for all languages
./build.sh dist # creates all release distribution files in dist/
./build.sh clean # removes all generated artifacts
```

## Testing

Testing is done with the same Docker container as mentioned in the building
step. The difference is that it will do clean run of the full test suite:

```bash
./build.sh docker-test
```
