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

namespace Apache\Avro\Tests\Datum;

use Apache\Avro\Datum\AvroIOBinaryDecoder;
use Apache\Avro\Datum\AvroIOBinaryEncoder;
use Apache\Avro\Datum\AvroIODatumReader;
use Apache\Avro\Datum\AvroIODatumWriter;
use Apache\Avro\Datum\AvroSpecificDatumWriter;
use Apache\Avro\IO\AvroStringIO;
use Apache\Avro\Schema\AvroSchema;
use Apache\Avro\Tests\Fixtures\Generated\Address;
use Apache\Avro\Tests\Fixtures\Generated\Car;
use Apache\Avro\Tests\Fixtures\Generated\FuelType;
use Apache\Avro\Tests\Fixtures\Generated\Member;
use Apache\Avro\Tests\Fixtures\Generated\Metadata;
use Apache\Avro\Tests\Fixtures\Generated\Order;
use Apache\Avro\Tests\Fixtures\Generated\Priority;
use Apache\Avro\Tests\Fixtures\Generated\Profile;
use Apache\Avro\Tests\Fixtures\Generated\Task;
use Apache\Avro\Tests\Fixtures\Generated\Team;
use Apache\Avro\Tests\Fixtures\Generated\User;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

/**
 * Tests for AvroSpecificDatumWriter: serializing generated class instances
 * to Avro binary and verifying round-trip via AvroIODatumReader.
 */
class AvroSpecificDatumWriterTest extends TestCase
{
    #[Test]
    public function simple_record_with_primitives(): void
    {
        $schema = AvroSchema::parse(<<<JSON
            {
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "active", "type": "boolean"},
                    {"name": "score", "type": "float"}
                ]
            }
            JSON);

        $user = new User('Alice', 30, true, 9.5);
        $result = $this->roundTrip($schema, $user);

        self::assertSame('Alice', $result['name']);
        self::assertSame(30, $result['age']);
        self::assertTrue($result['active']);
        self::assertEqualsWithDelta(9.5, $result['score'], 0.001);
    }

    #[Test]
    public function record_with_enum_field(): void
    {
        $schema = AvroSchema::parse(<<<JSON
            {
                "type": "record",
                "name": "Car",
                "fields": [
                    {"name": "brand", "type": "string"},
                    {
                        "name": "fuel",
                        "type": {
                            "type": "enum",
                            "name": "FuelType",
                            "symbols": ["gasoline", "diesel", "electric"]
                        }
                    }
                ]
            }
            JSON);

        $car = new Car('Tesla', FuelType::ELECTRIC);
        $result = $this->roundTrip($schema, $car);

        self::assertSame('Tesla', $result['brand']);
        self::assertSame('electric', $result['fuel']);
    }

    #[Test]
    public function record_with_nested_record(): void
    {
        $schema = AvroSchema::parse(<<<JSON
            {
                "type": "record",
                "name": "Order",
                "fields": [
                    {"name": "id", "type": "int"},
                    {
                        "name": "address",
                        "type": {
                            "type": "record",
                            "name": "Address",
                            "fields": [
                                {"name": "street", "type": "string"},
                                {"name": "city", "type": "string"}
                            ]
                        }
                    }
                ]
            }
            JSON);

        $order = new Order(42, new Address('123 Main St', 'Springfield'));
        $result = $this->roundTrip($schema, $order);

        self::assertSame(42, $result['id']);
        self::assertSame('123 Main St', $result['address']['street']);
        self::assertSame('Springfield', $result['address']['city']);
    }

    #[Test]
    public function record_with_nullable_field_present(): void
    {
        $schema = AvroSchema::parse(<<<JSON
            {
                "type": "record",
                "name": "Profile",
                "fields": [
                    {"name": "username", "type": "string"},
                    {"name": "bio", "type": ["null", "string"], "default": null}
                ]
            }
            JSON);

        $profile = new Profile('bob', 'Hello world');
        $result = $this->roundTrip($schema, $profile);

        self::assertSame('bob', $result['username']);
        self::assertSame('Hello world', $result['bio']);
    }

    #[Test]
    public function record_with_nullable_field_null(): void
    {
        $schema = AvroSchema::parse(<<<JSON
            {
                "type": "record",
                "name": "Profile",
                "fields": [
                    {"name": "username", "type": "string"},
                    {"name": "bio", "type": ["null", "string"], "default": null}
                ]
            }
            JSON);

        $profile = new Profile('bob');
        $result = $this->roundTrip($schema, $profile);

        self::assertSame('bob', $result['username']);
        self::assertNull($result['bio']);
    }

    #[Test]
    public function record_with_nullable_enum_present(): void
    {
        $schema = AvroSchema::parse(<<<JSON
            {
                "type": "record",
                "name": "Task",
                "fields": [
                    {"name": "title", "type": "string"},
                    {
                        "name": "priority",
                        "type": [
                            "null",
                            {
                                "type": "enum",
                                "name": "Priority",
                                "symbols": ["low", "medium", "high"]
                            }
                        ],
                        "default": null
                    }
                ]
            }
            JSON);

        $task = new Task('Fix bug', Priority::HIGH);
        $result = $this->roundTrip($schema, $task);

        self::assertSame('Fix bug', $result['title']);
        self::assertSame('high', $result['priority']);
    }

    #[Test]
    public function record_with_nullable_enum_null(): void
    {
        $schema = AvroSchema::parse(<<<JSON
            {
                "type": "record",
                "name": "Task",
                "fields": [
                    {"name": "title", "type": "string"},
                    {
                        "name": "priority",
                        "type": [
                            "null",
                            {
                                "type": "enum",
                                "name": "Priority",
                                "symbols": ["low", "medium", "high"]
                            }
                        ],
                        "default": null
                    }
                ]
            }
            JSON);

        $task = new Task('No priority');
        $result = $this->roundTrip($schema, $task);

        self::assertSame('No priority', $result['title']);
        self::assertNull($result['priority']);
    }

    #[Test]
    public function record_with_array_of_records(): void
    {
        $schema = AvroSchema::parse(<<<JSON
            {
                "type": "record",
                "name": "Team",
                "fields": [
                    {"name": "name", "type": "string"},
                    {
                        "name": "members",
                        "type": {
                            "type": "array",
                            "items": {
                                "type": "record",
                                "name": "Member",
                                "fields": [
                                    {"name": "name", "type": "string"},
                                    {"name": "role", "type": "string"}
                                ]
                            }
                        }
                    }
                ]
            }
            JSON);

        $team = new Team('Engineering', [
            new Member('Alice', 'Lead'),
            new Member('Bob', 'Developer'),
        ]);
        $result = $this->roundTrip($schema, $team);

        self::assertSame('Engineering', $result['name']);
        self::assertCount(2, $result['members']);
        self::assertSame('Alice', $result['members'][0]['name']);
        self::assertSame('Lead', $result['members'][0]['role']);
        self::assertSame('Bob', $result['members'][1]['name']);
        self::assertSame('Developer', $result['members'][1]['role']);
    }

    #[Test]
    public function record_with_empty_array(): void
    {
        $schema = AvroSchema::parse(<<<JSON
            {
                "type": "record",
                "name": "Team",
                "fields": [
                    {"name": "name", "type": "string"},
                    {
                        "name": "members",
                        "type": {
                            "type": "array",
                            "items": {
                                "type": "record",
                                "name": "Member",
                                "fields": [
                                    {"name": "name", "type": "string"},
                                    {"name": "role", "type": "string"}
                                ]
                            }
                        }
                    }
                ]
            }
            JSON);

        $team = new Team('Empty Team', []);
        $result = $this->roundTrip($schema, $team);

        self::assertSame('Empty Team', $result['name']);
        self::assertSame([], $result['members']);
    }

    #[Test]
    public function record_with_map_field(): void
    {
        $schema = AvroSchema::parse(<<<JSON
            {
                "type": "record",
                "name": "Metadata",
                "fields": [
                    {"name": "properties", "type": {"type": "map", "values": "string"}}
                ]
            }
            JSON);

        $metadata = new Metadata(['env' => 'production', 'version' => '1.2.3']);
        $result = $this->roundTrip($schema, $metadata);

        self::assertSame('production', $result['properties']['env']);
        self::assertSame('1.2.3', $result['properties']['version']);
    }

    #[Test]
    public function record_with_empty_map(): void
    {
        $schema = AvroSchema::parse(<<<JSON
            {
                "type": "record",
                "name": "Metadata",
                "fields": [
                    {"name": "properties", "type": {"type": "map", "values": "string"}}
                ]
            }
            JSON);

        $metadata = new Metadata([]);
        $result = $this->roundTrip($schema, $metadata);

        self::assertSame([], $result['properties']);
    }

    #[Test]
    public function produces_same_bytes_as_generic_writer(): void
    {
        $schema = AvroSchema::parse(<<<JSON
            {
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "active", "type": "boolean"},
                    {"name": "score", "type": "float"}
                ]
            }
            JSON);

        // Specific writer: from object
        $user = new User('Alice', 30, true, 9.5);
        $specificWriter = new AvroSpecificDatumWriter($schema);
        $specificIo = new AvroStringIO();
        $specificEncoder = new AvroIOBinaryEncoder($specificIo);
        $specificWriter->write($user, $specificEncoder);

        // Generic writer: from associative array
        $genericWriter = new AvroIODatumWriter($schema);
        $genericIo = new AvroStringIO();
        $genericEncoder = new AvroIOBinaryEncoder($genericIo);
        $genericWriter->write(
            ['name' => 'Alice', 'age' => 30, 'active' => true, 'score' => 9.5],
            $genericEncoder
        );

        self::assertSame($genericIo->string(), $specificIo->string());
    }

    #[Test]
    public function produces_same_bytes_for_enum_as_generic_writer(): void
    {
        $schema = AvroSchema::parse(<<<JSON
            {
                "type": "record",
                "name": "Car",
                "fields": [
                    {"name": "brand", "type": "string"},
                    {
                        "name": "fuel",
                        "type": {
                            "type": "enum",
                            "name": "FuelType",
                            "symbols": ["gasoline", "diesel", "electric"]
                        }
                    }
                ]
            }
            JSON);

        // Specific
        $car = new Car('BMW', FuelType::DIESEL);
        $specificWriter = new AvroSpecificDatumWriter($schema);
        $specificIo = new AvroStringIO();
        $specificWriter->write($car, new AvroIOBinaryEncoder($specificIo));

        // Generic
        $genericWriter = new AvroIODatumWriter($schema);
        $genericIo = new AvroStringIO();
        $genericWriter->write(
            ['brand' => 'BMW', 'fuel' => 'diesel'],
            new AvroIOBinaryEncoder($genericIo)
        );

        self::assertSame($genericIo->string(), $specificIo->string());
    }

    #[Test]
    public function produces_same_bytes_for_nested_record_as_generic_writer(): void
    {
        $schema = AvroSchema::parse(<<<JSON
            {
                "type": "record",
                "name": "Order",
                "fields": [
                    {"name": "id", "type": "int"},
                    {
                        "name": "address",
                        "type": {
                            "type": "record",
                            "name": "Address",
                            "fields": [
                                {"name": "street", "type": "string"},
                                {"name": "city", "type": "string"}
                            ]
                        }
                    }
                ]
            }
            JSON);

        // Specific
        $order = new Order(99, new Address('Oak Ave', 'Riverside'));
        $specificWriter = new AvroSpecificDatumWriter($schema);
        $specificIo = new AvroStringIO();
        $specificWriter->write($order, new AvroIOBinaryEncoder($specificIo));

        // Generic
        $genericWriter = new AvroIODatumWriter($schema);
        $genericIo = new AvroStringIO();
        $genericWriter->write(
            ['id' => 99, 'address' => ['street' => 'Oak Ave', 'city' => 'Riverside']],
            new AvroIOBinaryEncoder($genericIo)
        );

        self::assertSame($genericIo->string(), $specificIo->string());
    }

    #[Test]
    public function produces_same_bytes_for_nullable_union_as_generic_writer(): void
    {
        $schema = AvroSchema::parse(<<<JSON
            {
                "type": "record",
                "name": "Profile",
                "fields": [
                    {"name": "username", "type": "string"},
                    {"name": "bio", "type": ["null", "string"], "default": null}
                ]
            }
            JSON);

        // With value
        $profile = new Profile('alice', 'Bio text');
        $specificWriter = new AvroSpecificDatumWriter($schema);
        $specificIo = new AvroStringIO();
        $specificWriter->write($profile, new AvroIOBinaryEncoder($specificIo));

        $genericWriter = new AvroIODatumWriter($schema);
        $genericIo = new AvroStringIO();
        $genericWriter->write(
            ['username' => 'alice', 'bio' => 'Bio text'],
            new AvroIOBinaryEncoder($genericIo)
        );

        self::assertSame($genericIo->string(), $specificIo->string());

        // With null
        $profileNull = new Profile('bob');
        $specificIo2 = new AvroStringIO();
        $specificWriter->write($profileNull, new AvroIOBinaryEncoder($specificIo2));

        $genericIo2 = new AvroStringIO();
        $genericWriter->write(
            ['username' => 'bob', 'bio' => null],
            new AvroIOBinaryEncoder($genericIo2)
        );

        self::assertSame($genericIo2->string(), $specificIo2->string());
    }

    /**
     * Helper: serialize with AvroSpecificDatumWriter, then deserialize
     * with AvroIODatumReader to get back an associative array.
     */
    private function roundTrip(AvroSchema $schema, object $datum): mixed
    {
        // Serialize
        $writer = new AvroSpecificDatumWriter($schema);
        $io = new AvroStringIO();
        $encoder = new AvroIOBinaryEncoder($io);
        $writer->write($datum, $encoder);

        // Deserialize
        $io->seek(0);
        $reader = new AvroIODatumReader($schema);
        $decoder = new AvroIOBinaryDecoder($io);

        return $reader->read($decoder);
    }
}
