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
use Apache\Avro\AvroException;
use Apache\Avro\Datum\AvroIOBinaryDecoder;
use Apache\Avro\Datum\AvroIOBinaryEncoder;
use Apache\Avro\Datum\AvroIODatumReader;
use Apache\Avro\Datum\AvroIODatumWriter;
use Apache\Avro\Datum\Type\AvroDuration;
use Apache\Avro\IO\AvroStringIO;
use Apache\Avro\Schema\AvroSchema;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * @covers \AvroIOBinaryDecoder
 * @covers \AvroIOBinaryEncoder
 * @covers \AvroIODatumReader
 * @covers \AvroIODatumWriter
 */
class DatumIOTest extends TestCase
{
    #[DataProvider('data_provider')]
    public function test_datum_round_trip(string $schema_json, mixed $datum, string $binary): void
    {
        $this->assertIsValidDatumForSchema($schema_json, $datum, $binary);
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
            ['"long"', -(1 << 62), "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x7F"],
            ['"long"', -(1 << 61), "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x3F"],
            ['"long"', -4294967295, "\xFD\xFF\xFF\xFF\x1F"],
            ['"long"', -1 << 24, "\xFF\xFF\xFF\x0F"],
            ['"long"', -1 << 16, "\xFF\xFF\x07"],
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
            ['"long"', 1 << 16, "\x80\x80\x08"],
            ['"long"', 1 << 24, "\x80\x80\x80\x10"],
            ['"long"', 4294967295, "\xFE\xFF\xFF\xFF\x1F"],
            ['"long"', 1 << 61, "\x80\x80\x80\x80\x80\x80\x80\x80\x40"],
            ['"long"', 1 << 62, "\x80\x80\x80\x80\x80\x80\x80\x80\x80\x01"],
            ['"long"', 9223372036854775807, "\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x01"],

            ['"float"', (float) -10.0, "\000\000 \301"],
            ['"float"', (float) -1.0, "\000\000\200\277"],
            ['"float"', (float) 0.0, "\000\000\000\000"],
            ['"float"', (float) 2.0, "\000\000\000@"],
            ['"float"', (float) 9.0, "\000\000\020A"],

            ['"double"', (float) -10.0, "\000\000\000\000\000\000$\300"],
            ['"double"', (float) -1.0, "\000\000\000\000\000\000\360\277"],
            ['"double"', (float) 0.0, "\000\000\000\000\000\000\000\000"],
            ['"double"', (float) 2.0, "\000\000\000\000\000\000\000@"],
            ['"double"', (float) 9.0, "\000\000\000\000\000\000\"@"],

            ['"string"', 'foo', "\x06foo"],
            ['"bytes"', "\x01\x02\x03", "\x06\x01\x02\x03"],

            [
                '{"type":"array","items":"int"}',
                [1, 2, 3],
                "\x06\x02\x04\x06\x00",
            ],
            [
                '{"type":"map","values":"int"}',
                ['foo' => 1, 'bar' => 2, 'baz' => 3],
                "\x06\x06foo\x02\x06bar\x04\x06baz\x06\x00",
            ],
            ['["null", "int"]', 1, "\x02\x02"],
            [
                '{"name":"fix","type":"fixed","size":3}',
                "\xAA\xBB\xCC",
                "\xAA\xBB\xCC",
            ],
            [
                '{"name":"enm","type":"enum","symbols":["A","B","C"]}',
                'B',
                "\x02",
            ],
            [
                '{"name":"rec","type":"record","fields":[{"name":"a","type":"int"},{"name":"b","type":"boolean"}]}',
                ['a' => 1, 'b' => false],
                "\x02\x00",
            ],
        ];
    }

    public static function validBytesDecimalLogicalType(): array
    {
        return [
            'positive' => [
                '10.95',
                4,
                2,
                "\x04\x04\x47",
            ],
            'negative' => [
                '-10.95',
                4,
                2,
                "\x04\xFB\xB9",
            ],
            'small positive' => [
                '0.05',
                3,
                2,
                "\x02\x05",
            ],
            'zero value' => [
                '0',
                4,
                2,
                "\x02\x00",
            ],
            'unscaled positive' => [
                '12345',
                6,
                0,
                "\x04\x30\x39",
            ],
            'trimming edge case 127' => [
                '127',
                3,
                0,
                "\x02\x7F",
            ],
            'trimming edge case 128' => [
                '128',
                3,
                0,
                "\x04\x00\x80",
            ],
            'negative trimming -1' => [
                '-1',
                3,
                0,
                "\x02\xFF",
            ],
            'negative trimming -129' => [
                '-129',
                3,
                0,
                "\x04\xFF\x7F",
            ],
            'high precision positive number' => [
                '549755813887',
                12,
                0,
                "\x0A\x7F\xFF\xFF\xFF\xFF",
            ],
            'high precision positive number with scale' => [
                '54975581.3887',
                12,
                4,
                "\x0A\x7F\xFF\xFF\xFF\xFF",
            ],
            'high precision negative number' => [
                '-549755813888',
                12,
                0,
                "\x0A\x80\x00\x00\x00\x00",
            ],
            'high precision negative number with scale' => [
                '-54975581.3888',
                12,
                4,
                "\x0A\x80\x00\x00\x00\x00",
            ],
        ];
    }

    #[DataProvider('validBytesDecimalLogicalType')]
    public function test_valid_bytes_decimal_logical_type(string $datum, int $precision, int $scale, string $expected): void
    {
        $bytesSchemaJson = <<<JSON
            {
              "name": "number",
              "type": "bytes",
              "logicalType": "decimal",
              "precision": {$precision},
              "scale": {$scale}
            }
            JSON;

        $this->assertIsValidDatumForSchema($bytesSchemaJson, $datum, $expected);

        $fixedSchemaJson = <<<JSON
            {
              "name": "number",
              "type": "fixed",
              "size": 8,
              "logicalType": "decimal",
              "precision": {$precision},
              "scale": {$scale}
            }
            JSON;

        $this->assertIsValidDatumForSchema($fixedSchemaJson, $datum, $expected);
    }

    public function test_invalid_bytes_logical_type_out_of_range(): void
    {
        $schemaJson = <<<JSON
            {
              "name": "number",
              "type": "bytes",
              "logicalType": "decimal",
              "precision": 4,
              "scale": 0
            }
            JSON;

        $schema = AvroSchema::parse($schemaJson);
        $written = new AvroStringIO();
        $encoder = new AvroIOBinaryEncoder($written);
        $writer = new AvroIODatumWriter($schema);

        $this->expectException(AvroException::class);
        $this->expectExceptionMessage("Decimal value '10000' is out of range for precision=4, scale=0");
        $writer->write("10000", $encoder);
    }

    public static function validDurationLogicalTypes(): array
    {
        return [
            [new AvroDuration(1, 2, 3)],
            [new AvroDuration(-1, -2, -3)],
            [new AvroDuration(AvroSchema::INT_MAX_VALUE, AvroSchema::INT_MAX_VALUE, AvroSchema::INT_MIN_VALUE)],
        ];
    }

    #[DataProvider('validDurationLogicalTypes')]
    public function test_duration_logical_type(AvroDuration $avroDuration): void
    {
        $bytesSchemaJson = <<<JSON
            {
              "name": "number",
              "type": "fixed",
              "size": 12,
              "logicalType": "duration"
            }
            JSON;

        $this->assertIsValidDatumForSchema(
            $bytesSchemaJson,
            $avroDuration,
            $avroDuration
        );
    }

    public static function durationLogicalTypeOutOfBounds(): array
    {
        return [
            [AvroSchema::INT_MIN_VALUE - 1, 0, 0],
            [0, AvroSchema::INT_MIN_VALUE - 1, 0],
            [0, 0, AvroSchema::INT_MIN_VALUE - 1],
            [AvroSchema::INT_MAX_VALUE + 1, 0, 0],
            [0, AvroSchema::INT_MAX_VALUE + 1, 0],
            [0, 0, AvroSchema::INT_MAX_VALUE + 1],
        ];
    }

    #[DataProvider('durationLogicalTypeOutOfBounds')]
    public function test_duration_logical_type_out_of_bounds(int $months, int $days, int $milliseconds): void
    {
        $this->expectException(AvroException::class);
        new AvroDuration($months, $days, $milliseconds);
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
                [5, 4, 3, 2],
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

    #[DataProvider('default_provider')]
    public function test_field_default_value(
        string $field_schema_json,
        string $default_json,
        mixed $default_value
    ): void {
        $writers_schema_json = '{"name":"foo","type":"record","fields":[]}';
        $writers_schema = AvroSchema::parse($writers_schema_json);

        $readers_schema_json = sprintf(
            '{"name":"foo","type":"record","fields":[{"name":"f","type":%s,"default":%s}]}',
            $field_schema_json,
            $default_json
        );
        $readers_schema = AvroSchema::parse($readers_schema_json);

        $reader = new AvroIODatumReader($writers_schema, $readers_schema);
        $record = $reader->read(new AvroIOBinaryDecoder(new AvroStringIO()));
        if (array_key_exists('f', $record)) {
            $this->assertEquals($default_value, $record['f']);
        } else {
            $this->assertTrue(false, sprintf(
                'expected field record[f]: %s',
                print_r($record, true)
            ));
        }
    }

    /**
     * @param string $datum
     * @param string $expected
     * @throws \Apache\Avro\IO\AvroIOException
     */
    private function assertIsValidDatumForSchema(string $schemaJson, $datum, $expected): void
    {
        $schema = AvroSchema::parse($schemaJson);
        $written = new AvroStringIO();
        $encoder = new AvroIOBinaryEncoder($written);
        $writer = new AvroIODatumWriter($schema);

        $writer->write($datum, $encoder);
        $output = (string) $written;
        $this->assertEquals(
            $expected,
            $output,
            sprintf(
                "expected: %s\n  actual: %s",
                AvroDebug::asciiString($expected, 'hex'),
                AvroDebug::asciiString($output, 'hex')
            )
        );

        $read = new AvroStringIO((string) $expected);
        $decoder = new AvroIOBinaryDecoder($read);
        $reader = new AvroIODatumReader($schema);
        $read_datum = $reader->read($decoder);
        $this->assertEquals($datum, $read_datum);
    }
}
