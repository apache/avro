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

use Apache\Avro\AvroDebug;
use Apache\Avro\Datum\AvroIOBinaryDecoder;
use Apache\Avro\Datum\AvroIOBinaryEncoder;
use Apache\Avro\Datum\AvroIODatumReader;
use Apache\Avro\Datum\AvroIODatumWriter;
use Apache\Avro\IO\AvroStringIO;
use Apache\Avro\Schema\AvroSchema;
use PHPUnit\Framework\TestCase;

/**
 * @covers AvroIOBinaryDecoder
 * @covers AvroIOBinaryEncoder
 * @covers AvroIODatumReader
 * @covers AvroIODatumWriter
 */
class DatumIOTest extends TestCase
{
    /**
     * @dataProvider data_provider
     */
    public function test_datum_round_trip($schema_json, $datum, $binary): void
    {
        $schema = AvroSchema::parse($schema_json);
        $written = new AvroStringIO();
        $encoder = new AvroIOBinaryEncoder($written);
        $writer = new AvroIODatumWriter($schema);

        $writer->write($datum, $encoder);
        $output = (string) $written;
        $this->assertEquals($binary, $output,
            sprintf("expected: %s\n  actual: %s",
                AvroDebug::asciiString($binary, 'hex'),
                AvroDebug::asciiString($output, 'hex')));

        $read = new AvroStringIO($binary);
        $decoder = new AvroIOBinaryDecoder($read);
        $reader = new AvroIODatumReader($schema);
        $read_datum = $reader->read($decoder);
        $this->assertEquals($datum, $read_datum);
    }

    public static function data_provider(): array
    {
        return [
            ['"null"', null, ''],

            ['"boolean"', true, "\001"],
            ['"boolean"', false, "\000"],

            ['"int"', (int) -2147483648, "\xFF\xFF\xFF\xFF\x0F"],
            ['"int"', -1, "\001"],
            ['"int"', 0, "\000"],
            ['"int"', 1, "\002"],
            ['"int"', 2147483647, "\xFE\xFF\xFF\xFF\x0F"],

            ['"long"', (int) -9223372036854775808, "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x01"],
            ['"long"', -(1<<62), "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x7F"],
            ['"long"', -(1<<61), "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x3F"],
            ['"long"', -4294967295, "\xFD\xFF\xFF\xFF\x1F"],
            ['"long"', -1<<24, "\xFF\xFF\xFF\x0F"],
            ['"long"', -1<<16, "\xFF\xFF\x07"],
            ['"long"', -255, "\xFD\x03"],
            ['"long"', -128, "\xFF\x01"],
            ['"long"', -127, "\xFD\x01"],
            ['"long"', -10, "\x13"],
            ['"long"', -3, "\005"],
            ['"long"', -2, "\003"],
            ['"long"', -1, "\001"],
            ['"long"',  0, "\000"],
            ['"long"',  1, "\002"],
            ['"long"',  2, "\004"],
            ['"long"',  3, "\006"],
            ['"long"', 10, "\x14"],
            ['"long"', 127, "\xFE\x01"],
            ['"long"', 128, "\x80\x02"],
            ['"long"', 255, "\xFE\x03"],
            ['"long"', 1<<16, "\x80\x80\x08"],
            ['"long"', 1<<24, "\x80\x80\x80\x10"],
            ['"long"', 4294967295, "\xFE\xFF\xFF\xFF\x1F"],
            ['"long"', 1<<61, "\x80\x80\x80\x80\x80\x80\x80\x80\x40"],
            ['"long"', 1<<62, "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x01"],
            ['"long"', 9223372036854775807, "\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x01"],

            ['"float"', (float) -10.0, "\000\000 \301"],
            ['"float"', (float) -1.0, "\000\000\200\277"],
            ['"float"', (float) 0.0, "\000\000\000\000"],
            ['"float"', (float) 2.0, "\000\000\000@"],
            ['"float"', (float) 9.0, "\000\000\020A"],

            ['"double"', (double) -10.0, "\000\000\000\000\000\000$\300"],
            ['"double"', (double) -1.0, "\000\000\000\000\000\000\360\277"],
            ['"double"', (double) 0.0, "\000\000\000\000\000\000\000\000"],
            ['"double"', (double) 2.0, "\000\000\000\000\000\000\000@"],
            ['"double"', (double) 9.0, "\000\000\000\000\000\000\"@"],

            ['"string"', 'foo', "\x06foo"],
            ['"bytes"', "\x01\x02\x03", "\x06\x01\x02\x03"],

            [
                '{"type":"array","items":"int"}',
                [1, 2, 3],
                "\x06\x02\x04\x06\x00"
            ],
            [
                '{"type":"map","values":"int"}',
                ['foo' => 1, 'bar' => 2, 'baz' => 3],
                "\x06\x06foo\x02\x06bar\x04\x06baz\x06\x00"
            ],
            ['["null", "int"]', 1, "\x02\x02"],
            [
                '{"name":"fix","type":"fixed","size":3}',
                "\xAA\xBB\xCC",
                "\xAA\xBB\xCC"
            ],
            [
                '{"name":"enm","type":"enum","symbols":["A","B","C"]}',
                'B',
                "\x02"
            ],
            [
                '{"name":"rec","type":"record","fields":[{"name":"a","type":"int"},{"name":"b","type":"boolean"}]}',
                ['a' => 1, 'b' => false],
                "\x02\x00"
            ]
        ];
    }

    public static function default_provider(): array
    {
        return [
            [
                '"null"',
                'null',
                null,
            ],
            [
                '"boolean"',
                'true',
                true,
            ],
            [
                '"int"',
                '1',
                1,
            ],
            [
                '"long"',
                '2000',
                2000,
            ],
            [
                '"float"',
                '1.1',
                1.1,
            ],
            [
                '"double"',
                '200.2',
                200.2,
            ],
            [
                '"string"',
                '"quux"',
                'quux',
            ],
            [
                '"bytes"',
                '"\u00FF"',
                "\xC3\xBF"],
            [
                '{"type":"array","items":"int"}',
                '[5,4,3,2]',
                [5, 4, 3, 2]
            ],
            [
                '{"type":"map","values":"int"}',
                '{"a":9}',
                ['a' => 9],
            ],
            [
                '["int","string"]',
                '8',
                8,
            ],
            [
                '{"name":"x","type":"enum","symbols":["A","V"]}',
                '"A"',
                'A',
            ],
            [
                '{"name":"x","type":"fixed","size":4}',
                '"\u00ff"',
                "\xC3\xBF",
            ],
            [
                '{"name":"x","type":"record","fields":[{"name":"label","type":"int"}]}',
                '{"label":7}',
                ['label' => 7],
            ],
            'logical type' => [
                '{"type":"string","logicalType":"uuid"}',
                '"550e8400-e29b-41d4-a716-446655"',
                '550e8400-e29b-41d4-a716-446655',

            ],
            'logical type in a record' => [
                '{"name":"x","type":"record","fields":[{"name":"price","type":"bytes","logicalType":"decimal","precision":4,"scale":2}]}',
                '{"price": "\u0000\u0000"}',
                ['price' => "0"],

            ],
        ];
    }

    /**
     * @dataProvider default_provider
     */
    public function test_field_default_value(
        $field_schema_json,
        $default_json,
        $default_value
    ): void {
        $writers_schema_json = '{"name":"foo","type":"record","fields":[]}';
        $writers_schema = AvroSchema::parse($writers_schema_json);

        $readers_schema_json = sprintf(
            '{"name":"foo","type":"record","fields":[{"name":"f","type":%s,"default":%s}]}',
            $field_schema_json, $default_json);
        $readers_schema = AvroSchema::parse($readers_schema_json);

        $reader = new AvroIODatumReader($writers_schema, $readers_schema);
        $record = $reader->read(new AvroIOBinaryDecoder(new AvroStringIO()));
        if (array_key_exists('f', $record)) {
            $this->assertEquals($default_value, $record['f']);
        } else {
            $this->assertTrue(false, sprintf('expected field record[f]: %s',
                print_r($record, true)));
        }
    }
}
