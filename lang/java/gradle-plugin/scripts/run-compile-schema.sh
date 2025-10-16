#!/usr/bin/env bash
set -ex
avroVersion=${1?"Usage: $0 AVRO_VERSION SCHEMA_FILE"};
schemaFile=${2?"Usage: $0 AVRO_VERSION SCHEMA_FILE"};
mkdir -p input/
mkdir -p output/
./run-avro-cli.sh $1 compile schema input/$2 output/
