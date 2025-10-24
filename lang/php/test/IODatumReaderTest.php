<?php
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache\Avro\Tests;

use Apache\Avro\Datum\AvroIOBinaryDecoder;
use Apache\Avro\Datum\AvroIOBinaryEncoder;
use Apache\Avro\Datum\AvroIODatumReader;
use Apache\Avro\Datum\AvroIODatumWriter;
use Apache\Avro\Datum\Type\AvroDuration;
use Apache\Avro\IO\AvroStringIO;
use Apache\Avro\Schema\AvroSchema;
use PHPUnit\Framework\TestCase;

class IODatumReaderTest extends TestCase
{
    public function testSchemaMatching(): void
    {
        $writers_schema = <<<JSON
        {
          "type": "map",
          "values": "bytes"
        }
        JSON;
        $readers_schema = $writers_schema;
        $this->assertTrue(AvroIODatumReader::schemasMatch(
                AvroSchema::parse($writers_schema),
            AvroSchema::parse($readers_schema)));
    }

    public function test_aliased(): void
    {
        $writers_schema = AvroSchema::parse(
            <<<JSON
            {
              "type": "record",
              "name": "Rec1",
              "fields": [
                {
                  "name": "field1",
                  "type": "int"
                }
              ]
            }
            JSON
        );
    $readers_schema = AvroSchema::parse(<<<SCHEMA
      {"type":"record", "name":"Rec2", "aliases":["Rec1"], "fields":[
        {"name":"field2", "aliases":["field1"], "type":"int"}
      ]}
    SCHEMA);

        $io = new AvroStringIO();
        $writer = new AvroIODatumWriter();
        $writer->writeData($writers_schema, ['field1' => 1], new AvroIOBinaryEncoder($io));

        $bin = $io->string();
        $reader = new AvroIODatumReader();
        $record = $reader->readRecord(
            $writers_schema,
            $readers_schema,
            new AvroIOBinaryDecoder(new AvroStringIO($bin))
        );

        $this->assertEquals(['field2' => 1], $record);
    }

    public function testRecordNullField(): void
    {
        $schema_json = <<<JSON
            {
              "name":"member",
              "type":"record",
              "fields":[
                {"name":"one", "type":"int"},
                {"name":"two", "type":["null", "string"]}
              ]
            }
            JSON;

        $schema = AvroSchema::parse($schema_json);
        $datum = array("one" => 1);

        $io = new AvroStringIO();
        $writer = new AvroIODatumWriter($schema);
        $encoder = new AvroIOBinaryEncoder($io);
        $writer->write($datum, $encoder);
        $bin = $io->string();

        $this->assertSame('0200', bin2hex($bin));
    }

    public function testRecordFieldWithDefault(): void
    {
        $schema = AvroSchema::parse(
            <<<JSON
            {
              "name": "RecordWithDefaultValue",
              "type": "record",
              "fields": [
                {
                  "name": "field1",
                  "type": "string",
                  "default": "default"
                }
              ]
            }
            JSON
        );

        $io = new AvroStringIO();
        $writer = new AvroIODatumWriter();
        $writer->writeData($schema, ['field1' => "foobar"], new AvroIOBinaryEncoder($io));

        $bin = $io->string();
        $reader = new AvroIODatumReader();
        $record = $reader->readRecord(
            $schema,
            $schema,
            new AvroIOBinaryDecoder(new AvroStringIO($bin))
        );

        $this->assertEquals(['field1' => "foobar"], $record);
    }

    public function testRecordWithLogicalTypes(): void
    {
        $schema = AvroSchema::parse(
            <<<JSON
            {
              "name": "RecordWithLogicalTypes",
              "type": "record",
              "fields": [
                {
                  "name": "decimal_field",
                  "type": "bytes",
                  "logicalType": "decimal",
                  "precision": 4,
                  "scale": 2
                },
                {
                  "name": "uuid_field",
                  "type": "string",
                  "logicalType": "uuid"
                },
                {
                  "name": "date_field",
                  "type": {
                    "type": "int",
                    "logicalType": "date"
                  }
                },
                {
                  "name": "time_millis_field",
                  "type": {
                    "type": "int",
                    "logicalType": "time-millis"
                  }
                },
                {
                  "name": "time_micros_field",
                  "type": {
                    "type": "long",
                    "logicalType": "time-micros"
                  }
                },
                {
                  "name": "timestamp_millis_field",
                  "type": {
                    "type": "long",
                    "logicalType": "timestamp-millis"
                  }
                },
                {
                  "name": "timestamp_micros_field",
                  "type": {
                    "type": "long",
                    "logicalType": "timestamp-micros"
                  }
                },
                {
                  "name": "local_timestamp_millis_field",
                  "type": {
                    "type": "long",
                    "logicalType": "local-timestamp-millis"
                 }
                },
                {
                  "name": "local_timestamp_micros_field",
                  "type": {
                    "type": "long",
                    "logicalType": "local-timestamp-micros"
                 }
                },
                {
                  "name": "duration_field",
                  "type": {
                    "name": "duration_field",
                    "type": "fixed",
                    "size": 12,
                    "logicalType": "duration"
                  }
                },
                {
                  "name": "decimal_fixed_field",
                  "type": {
                    "name": "decimal_fixed_field",
                    "type": "fixed",
                    "logicalType": "decimal",
                    "size": 3,
                    "precision": 4,
                    "scale": 2
                  }
                }
              ]
            }
            JSON
        );

        $io = new AvroStringIO();
        $writer = new AvroIODatumWriter();
        $writer->writeData(
            $schema,
            [
                'decimal_field' => '10.91',
                'uuid_field' => '9fb9ea49-2f7e-4df3-b02b-96d881e27a6b',
                'date_field' => 20251023,
                'time_millis_field' => 86400000,
                'time_micros_field' => 86400000000,
                'timestamp_millis_field' => 1761224729109,
                'timestamp_micros_field' => 1761224729109000,
                'local_timestamp_millis_field' => 1751224729109,
                'local_timestamp_micros_field' => 1751224729109000,
                'duration_field' => new AvroDuration(5, 3600, 1234),
                'decimal_fixed_field' => '10.91',
            ],
            new AvroIOBinaryEncoder($io)
        );

        $bin = $io->string();
        $reader = new AvroIODatumReader();
        $record = $reader->readRecord(
            $schema,
            $schema,
            new AvroIOBinaryDecoder(new AvroStringIO($bin))
        );

        $this->assertEquals(
            [
                'decimal_field' => '10.91',
                'uuid_field' => '9fb9ea49-2f7e-4df3-b02b-96d881e27a6b',
                'date_field' => 20251023,
                'time_millis_field' => 86400000,
                'time_micros_field' => 86400000000,
                'timestamp_millis_field' => 1761224729109,
                'timestamp_micros_field' => 1761224729109000,
                'local_timestamp_millis_field' => 1751224729109,
                'local_timestamp_micros_field' => 1751224729109000,
                'duration_field' => new AvroDuration(5, 3600, 1234),
                'decimal_fixed_field' => '10.91',
            ],
            $record
        );
    }
}
