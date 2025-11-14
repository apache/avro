#!/usr/bin/env bash
set -ex
avroVersion=${1?"Usage: $0 AVRO_VERSION ARGUMENTS"};
shift
mkdir -p downloads
wget --timestamping --directory-prefix=downloads/ http://archive.apache.org/dist/avro/avro-${avroVersion}/java/avro-tools-${avroVersion}.jar
java -jar downloads/avro-tools-${avroVersion}.jar $*
