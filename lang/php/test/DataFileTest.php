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

declare(strict_types=1);

namespace Apache\Avro\Tests;

use Apache\Avro\DataFile\AvroDataIO;
use PHPUnit\Framework\TestCase;

class DataFileTest extends TestCase
{
    public const REMOVE_DATA_FILES = true;
    private array $dataFiles = [];

    protected function setUp(): void
    {
        if (!file_exists(TEST_TEMP_DIR)) {
            mkdir(TEST_TEMP_DIR);
        }
        $this->remove_data_files();
        $this->dataFiles = [];
    }

    protected function tearDown(): void
    {
        $this->remove_data_files();
    }

    public function test_write_read_nothing_round_trip(): void
    {
        foreach (AvroDataIO::validCodecs() as $codec) {
            if (
                (AvroDataIO::SNAPPY_CODEC === $codec && !extension_loaded('snappy'))
                || (AvroDataIO::ZSTANDARD_CODEC === $codec && !extension_loaded('zstd'))
            ) {
                continue;
            }
            $data_file = $this->add_data_file(sprintf('data-wr-nothing-null-%s.avr', $codec));
            $writers_schema = '"null"';
            $dw = AvroDataIO::openFile($data_file, 'w', $writers_schema, $codec);
            $dw->close();

            $dr = AvroDataIO::openFile($data_file);
            $data = $dr->data();
            $dr->close();
            $this->assertEmpty($data);
        }
    }

    public static function current_timestamp(): string
    {
        return (new \DateTime())->format('Y-m-d H:i:s');
    }

    public function test_write_read_null_round_trip(): void
    {
        foreach (AvroDataIO::validCodecs() as $codec) {
            if (
                (AvroDataIO::SNAPPY_CODEC === $codec && !extension_loaded('snappy'))
                || (AvroDataIO::ZSTANDARD_CODEC === $codec && !extension_loaded('zstd'))
            ) {
                continue;
            }
            $data_file = $this->add_data_file(sprintf('data-wr-null-%s.avr', $codec));
            $writers_schema = '"null"';
            $data = null;
            $dw = AvroDataIO::openFile($data_file, 'w', $writers_schema, $codec);
            $dw->append($data);
            $dw->close();

            $dr = AvroDataIO::openFile($data_file);
            $read_data = $dr->data();
            $datum = reset($read_data);
            $dr->close();
            $this->assertEquals($data, $datum);
        }
    }

    public function test_write_read_string_round_trip(): void
    {
        foreach (AvroDataIO::validCodecs() as $codec) {
            if (
                (AvroDataIO::SNAPPY_CODEC === $codec && !extension_loaded('snappy'))
                || (AvroDataIO::ZSTANDARD_CODEC === $codec && !extension_loaded('zstd'))
            ) {
                continue;
            }
            $data_file = $this->add_data_file(sprintf('data-wr-str-%s.avr', $codec));
            $writers_schema = '"string"';
            $data = 'foo';
            $dw = AvroDataIO::openFile($data_file, 'w', $writers_schema, $codec);
            $dw->append($data);
            $dw->close();

            $dr = AvroDataIO::openFile($data_file);
            $read_data = $dr->data();
            $datum = array_shift($read_data);
            $dr->close();
            $this->assertEquals($data, $datum);
        }
    }

    public function test_write_read_round_trip(): void
    {
        foreach (AvroDataIO::validCodecs() as $codec) {
            if (
                (AvroDataIO::SNAPPY_CODEC === $codec && !extension_loaded('snappy'))
                || (AvroDataIO::ZSTANDARD_CODEC === $codec && !extension_loaded('zstd'))
            ) {
                continue;
            }
            $data_file = $this->add_data_file(sprintf('data-wr-int-%s.avr', $codec));
            $writers_schema = '"int"';
            $data = 1;

            $dw = AvroDataIO::openFile($data_file, 'w', $writers_schema, $codec);
            $dw->append(1);
            $dw->close();

            $dr = AvroDataIO::openFile($data_file);
            $read_data = $dr->data();
            $datum = array_shift($read_data);
            $dr->close();
            $this->assertEquals($data, $datum);
        }
    }

    public function test_write_read_true_round_trip(): void
    {
        foreach (AvroDataIO::validCodecs() as $codec) {
            if (
                (AvroDataIO::SNAPPY_CODEC === $codec && !extension_loaded('snappy'))
                || (AvroDataIO::ZSTANDARD_CODEC === $codec && !extension_loaded('zstd'))
            ) {
                continue;
            }
            $data_file = $this->add_data_file(sprintf('data-wr-true-%s.avr', $codec));
            $writers_schema = '"boolean"';
            $datum = true;
            $dw = AvroDataIO::openFile($data_file, 'w', $writers_schema, $codec);
            $dw->append($datum);
            $dw->close();

            $dr = AvroDataIO::openFile($data_file);
            $read_data = $dr->data();
            $read_datum = array_shift($read_data);
            $dr->close();
            $this->assertEquals($datum, $read_datum);
        }
    }

    public function test_write_read_false_round_trip(): void
    {
        foreach (AvroDataIO::validCodecs() as $codec) {
            if (
                (AvroDataIO::SNAPPY_CODEC === $codec && !extension_loaded('snappy'))
                || (AvroDataIO::ZSTANDARD_CODEC === $codec && !extension_loaded('zstd'))
            ) {
                continue;
            }
            $data_file = $this->add_data_file(sprintf('data-wr-false-%s.avr', $codec));
            $writers_schema = '"boolean"';
            $datum = false;
            $dw = AvroDataIO::openFile($data_file, 'w', $writers_schema, $codec);
            $dw->append($datum);
            $dw->close();

            $dr = AvroDataIO::openFile($data_file);
            $read_data = $dr->data();
            $read_datum = array_shift($read_data);
            $dr->close();
            $this->assertEquals($datum, $read_datum);
        }
    }

    public function test_write_read_int_array_round_trip(): void
    {
        foreach (AvroDataIO::validCodecs() as $codec) {
            if (
                (AvroDataIO::SNAPPY_CODEC === $codec && !extension_loaded('snappy'))
                || (AvroDataIO::ZSTANDARD_CODEC === $codec && !extension_loaded('zstd'))
            ) {
                continue;
            }
            $data_file = $this->add_data_file(sprintf('data-wr-int-ary-%s.avr', $codec));
            $writers_schema = '"int"';
            $data = [10, 20, 30, 40, 50, 60, 70, 567, 89012345];
            $dw = AvroDataIO::openFile($data_file, 'w', $writers_schema, $codec);
            foreach ($data as $datum) {
                $dw->append($datum);
            }
            $dw->close();

            $dr = AvroDataIO::openFile($data_file);
            $read_data = $dr->data();
            $dr->close();
            $this->assertEquals(
                $data,
                $read_data,
                sprintf(
                    "in: %s\nout: %s",
                    json_encode($data),
                    json_encode($read_data)
                )
            );
        }
    }

    public function test_differing_schemas_with_primitives(): void
    {
        foreach (AvroDataIO::validCodecs() as $codec) {
            if (
                (AvroDataIO::SNAPPY_CODEC === $codec && !extension_loaded('snappy'))
                || (AvroDataIO::ZSTANDARD_CODEC === $codec && !extension_loaded('zstd'))
            ) {
                continue;
            }
            $data_file = $this->add_data_file(sprintf('data-prim-%s.avr', $codec));

            $writer_schema = <<<JSON
                { "type": "record",
                  "name": "User",
                  "fields" : [
                      {"name": "username", "type": "string"},
                      {"name": "age", "type": "int"},
                      {"name": "verified", "type": "boolean", "default": "false"}
                      ]
                      }
                JSON;
            $data = [
                ['username' => 'john', 'age' => 25, 'verified' => true],
                ['username' => 'ryan', 'age' => 23, 'verified' => false],
            ];
            $dw = AvroDataIO::openFile($data_file, 'w', $writer_schema, $codec);
            foreach ($data as $datum) {
                $dw->append($datum);
            }
            $dw->close();
            $reader_schema = <<<JSON
                { "type": "record",
                  "name": "User",
                  "fields" : [
                {"name": "username", "type": "string"}
                ]}
                JSON;
            $dr = AvroDataIO::openFile($data_file, 'r', $reader_schema);
            foreach ($dr->data() as $index => $record) {
                $this->assertEquals($data[$index]['username'], $record['username']);
            }
        }
    }

    public function test_differing_schemas_with_complex_objects(): void
    {
        foreach (AvroDataIO::validCodecs() as $codec) {
            if (
                (AvroDataIO::SNAPPY_CODEC === $codec && !extension_loaded('snappy'))
                || (AvroDataIO::ZSTANDARD_CODEC === $codec && !extension_loaded('zstd'))
            ) {
                continue;
            }
            $data_file = $this->add_data_file(sprintf('data-complex-%s.avr', $codec));

            $writers_schema = <<<JSON
                {
                  "type": "record",
                  "name": "something",
                  "fields": [
                    {
                      "name": "something_fixed",
                      "type": {
                        "name": "inner_fixed",
                        "type": "fixed",
                        "size": 3
                      }
                    },
                    {
                      "name": "something_enum",
                      "type": {
                        "name": "inner_enum",
                        "type": "enum",
                        "symbols": [
                          "hello",
                          "goodbye"
                        ]
                      }
                    },
                    {
                      "name": "something_array",
                      "type": {
                        "type": "array",
                        "items": "int"
                      }
                    },
                    {
                      "name": "something_map",
                      "type": {
                        "type": "map",
                        "values": "int"
                      }
                    },
                    {
                      "name": "something_record",
                      "type": {
                        "name": "inner_record",
                        "type": "record",
                        "fields": [
                          {
                            "name": "inner",
                            "type": "int"
                          }
                        ]
                      }
                    },
                    {
                      "name": "username",
                      "type": "string"
                    }
                  ]
                }
                JSON;

            $data = [
                [
                    "username" => "john",
                    "something_fixed" => "foo",
                    "something_enum" => "hello",
                    "something_array" => [1, 2, 3],
                    "something_map" => ["a" => 1, "b" => 2],
                    "something_record" => ["inner" => 2],
                    "something_error" => ["code" => 403],
                ],
                [
                    "username" => "ryan",
                    "something_fixed" => "bar",
                    "something_enum" => "goodbye",
                    "something_array" => [1, 2, 3],
                    "something_map" => ["a" => 2, "b" => 6],
                    "something_record" => ["inner" => 1],
                    "something_error" => ["code" => 401],
                ],
            ];
            $dw = AvroDataIO::openFile($data_file, 'w', $writers_schema, $codec);
            foreach ($data as $datum) {
                $dw->append($datum);
            }
            $dw->close();

            foreach ([
                'fixed',
                'enum',
                'record',
                'error',
                'array',
                'map',
                'union',
            ] as $s) {
                $readers_schema = json_decode($writers_schema, true);
                $dr = AvroDataIO::openFile($data_file, 'r', json_encode($readers_schema));
                foreach ($dr->data() as $idx => $obj) {
                    foreach ($readers_schema['fields'] as $field) {
                        $field_name = $field['name'];
                        $this->assertEquals($data[$idx][$field_name], $obj[$field_name]);
                    }
                }
                $dr->close();
            }
        }
    }

    protected function add_data_file(string $data_file): string
    {
        $data_file = "$data_file.".self::current_timestamp();
        $full = implode(DIRECTORY_SEPARATOR, [TEST_TEMP_DIR, $data_file]);
        $this->dataFiles[] = $full;

        return $full;
    }

    protected function remove_data_files(): void
    {
        if (self::REMOVE_DATA_FILES && $this->dataFiles) {
            foreach ($this->dataFiles as $data_file) {
                self::remove_data_file($data_file);
            }
        }
    }

    protected static function remove_data_file($data_file): void
    {
        if (file_exists($data_file)) {
            unlink($data_file);
        }
    }
}
