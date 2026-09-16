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

namespace Apache\Avro\Tests\Generator;

use Apache\Avro\Generator\AvroCodeGenerator;
use Apache\Avro\Schema\AvroSchema;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

class AvroCodeGeneratorTest extends TestCase
{
    #[Test]
    public function nested_schema_generation(): void
    {
        $schema = <<<JSON
            {
               "type":"record",
               "name":"Lisp",
               "fields":[
                  {
                     "name":"value",
                     "type":[
                        "null",
                        "string",
                        {
                           "type":"record",
                           "name":"Cons",
                           "fields":[
                              {
                                 "name":"car",
                                 "type":"Lisp"
                              },
                              {
                                 "name":"cdr",
                                 "type":"Lisp"
                              }
                           ]
                        }
                     ]
                  }
               ]
            }
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'MyApp\\Avro\\Generated');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(2, $files);

        self::assertArrayHasKey('/generated/Lisp.php', $files);
        self::assertArrayHasKey('/generated/Cons.php', $files);

        $expectedLisp = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace MyApp\Avro\Generated;
            
            final class Lisp implements \JsonSerializable
            {
                private null|string|\MyApp\Avro\Generated\Cons \$value;
                public function __construct(null|string|\MyApp\Avro\Generated\Cons \$value)
                {
                    \$this->value = \$value;
                }
                public function value(): null|string|\MyApp\Avro\Generated\Cons
                {
                    return \$this->value;
                }
                public function jsonSerialize(): mixed
                {
                    return ['value' => \$this->value];
                }
            }

            PHP;

        self::assertEquals($expectedLisp, $files['/generated/Lisp.php']);

        $expectedLisp = <<<PHP
            <?php

            declare(strict_types=1);
            
            namespace MyApp\Avro\Generated;
            
            final class Cons implements \JsonSerializable
            {
                private \MyApp\Avro\Generated\Lisp \$car;
                private \MyApp\Avro\Generated\Lisp \$cdr;
                public function __construct(\MyApp\Avro\Generated\Lisp \$car, \MyApp\Avro\Generated\Lisp \$cdr)
                {
                    \$this->car = \$car;
                    \$this->cdr = \$cdr;
                }
                public function car(): \MyApp\Avro\Generated\Lisp
                {
                    return \$this->car;
                }
                public function cdr(): \MyApp\Avro\Generated\Lisp
                {
                    return \$this->cdr;
                }
                public function jsonSerialize(): mixed
                {
                    return ['car' => \$this->car, 'cdr' => \$this->cdr];
                }
            }

            PHP;
        self::assertEquals($expectedLisp, $files['/generated/Cons.php']);
    }

    #[Test]
    public function simple_record_with_primitive_types(): void
    {
        $schema = <<<JSON
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
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\Model');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(1, $files);
        self::assertArrayHasKey('/generated/User.php', $files);

        $expected = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Model;
            
            final class User implements \JsonSerializable
            {
                private string \$name;
                private int \$age;
                private bool \$active;
                private float \$score;
                public function __construct(string \$name, int \$age, bool \$active, float \$score)
                {
                    \$this->name = \$name;
                    \$this->age = \$age;
                    \$this->active = \$active;
                    \$this->score = \$score;
                }
                public function name(): string
                {
                    return \$this->name;
                }
                public function age(): int
                {
                    return \$this->age;
                }
                public function active(): bool
                {
                    return \$this->active;
                }
                public function score(): float
                {
                    return \$this->score;
                }
                public function jsonSerialize(): mixed
                {
                    return ['name' => \$this->name, 'age' => \$this->age, 'active' => \$this->active, 'score' => \$this->score];
                }
            }

            PHP;

        self::assertEquals($expected, $files['/generated/User.php']);
    }

    #[Test]
    public function namespaced_schema_uses_namespace_directories_and_prefix_namespace(): void
    {
        $schema = <<<JSON
            {
                "type": "record",
                "namespace": "com.example",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "int"}
                ]
            }
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\Model');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(1, $files);
        self::assertArrayHasKey('/generated/Com/Example/User.php', $files);
        self::assertStringContainsString('namespace App\\Model\\Com\\Example;', $files['/generated/Com/Example/User.php']);
    }

    #[Test]
    public function duplicate_record_names_with_different_namespaces_generate_distinct_classes(): void
    {
        $schema = <<<JSON
            {
                "type": "record",
                "name": "Organization",
                "fields": [
                    {
                        "name": "sector1User",
                        "type": {
                            "type": "record",
                            "name": "User",
                            "namespace": "org.Acme.Sector1",
                            "fields": [
                                {"name": "id", "type": "int"}
                            ]
                        }
                    },
                    {
                        "name": "sector2User",
                        "type": {
                            "type": "record",
                            "name": "User",
                            "namespace": "org.Acme.Sector2",
                            "fields": [
                                {"name": "id", "type": "int"}
                            ]
                        }
                    }
                ]
            }
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\Model');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(3, $files);
        self::assertArrayHasKey('/generated/Organization.php', $files);
        self::assertArrayHasKey('/generated/Org/Acme/Sector1/User.php', $files);
        self::assertArrayHasKey('/generated/Org/Acme/Sector2/User.php', $files);

        $expectedOrganization = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Model;
            
            final class Organization implements \JsonSerializable
            {
                private \App\Model\Org\Acme\Sector1\User \$sector1User;
                private \App\Model\Org\Acme\Sector2\User \$sector2User;
                public function __construct(\App\Model\Org\Acme\Sector1\User \$sector1User, \App\Model\Org\Acme\Sector2\User \$sector2User)
                {
                    \$this->sector1User = \$sector1User;
                    \$this->sector2User = \$sector2User;
                }
                public function sector1User(): \App\Model\Org\Acme\Sector1\User
                {
                    return \$this->sector1User;
                }
                public function sector2User(): \App\Model\Org\Acme\Sector2\User
                {
                    return \$this->sector2User;
                }
                public function jsonSerialize(): mixed
                {
                    return ['sector1User' => \$this->sector1User, 'sector2User' => \$this->sector2User];
                }
            }

            PHP;

        self::assertEquals($expectedOrganization, $files['/generated/Organization.php']);
        self::assertStringContainsString('namespace App\\Model\\Org\\Acme\\Sector1;', $files['/generated/Org/Acme/Sector1/User.php']);
        self::assertStringContainsString('namespace App\\Model\\Org\\Acme\\Sector2;', $files['/generated/Org/Acme/Sector2/User.php']);
    }

    #[Test]
    public function enum_schema_generation(): void
    {
        $schema = <<<JSON
            {
                "type": "enum",
                "name": "Color",
                "symbols": ["red", "green", "blue"]
            }
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\Enums');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(1, $files);
        self::assertArrayHasKey('/generated/Color.php', $files);

        $expected = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Enums;
            
            enum Color : string
            {
                case RED = 'red';
                case GREEN = 'green';
                case BLUE = 'blue';
            }

            PHP;

        self::assertEquals($expected, $files['/generated/Color.php']);
    }

    #[Test]
    public function record_with_default_values(): void
    {
        $schema = <<<JSON
            {
                "type": "record",
                "name": "Config",
                "fields": [
                    {"name": "retries", "type": "int", "default": 3},
                    {"name": "label", "type": "string", "default": "default"},
                    {"name": "enabled", "type": "boolean", "default": true}
                ]
            }
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\Config');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(1, $files);
        self::assertArrayHasKey('/generated/Config.php', $files);

        $expected = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Config;
            
            final class Config implements \JsonSerializable
            {
                private int \$retries = 3;
                private string \$label = 'default';
                private bool \$enabled = true;
                public function __construct(int \$retries = 3, string \$label = 'default', bool \$enabled = true)
                {
                    \$this->retries = \$retries;
                    \$this->label = \$label;
                    \$this->enabled = \$enabled;
                }
                public function retries(): int
                {
                    return \$this->retries;
                }
                public function label(): string
                {
                    return \$this->label;
                }
                public function enabled(): bool
                {
                    return \$this->enabled;
                }
                public function jsonSerialize(): mixed
                {
                    return ['retries' => \$this->retries, 'label' => \$this->label, 'enabled' => \$this->enabled];
                }
            }

            PHP;

        self::assertEquals($expected, $files['/generated/Config.php']);
    }

    #[Test]
    public function record_with_array_field(): void
    {
        $schema = <<<JSON
            {
                "type": "record",
                "name": "Playlist",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "tags", "type": {"type": "array", "items": "string"}}
                ]
            }
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\Music');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(1, $files);
        self::assertArrayHasKey('/generated/Playlist.php', $files);

        $expected = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Music;
            
            final class Playlist implements \JsonSerializable
            {
                private string \$name;
                /** @var list<string> */
                private array \$tags;
                /**
                 * @param list<string> \$tags
                 */
                public function __construct(string \$name, array \$tags)
                {
                    \$this->name = \$name;
                    \$this->tags = \$tags;
                }
                public function name(): string
                {
                    return \$this->name;
                }
                /** @return list<string> */
                public function tags(): array
                {
                    return \$this->tags;
                }
                public function jsonSerialize(): mixed
                {
                    return ['name' => \$this->name, 'tags' => \$this->tags];
                }
            }

            PHP;

        self::assertEquals($expected, $files['/generated/Playlist.php']);
    }

    #[Test]
    public function record_with_map_field(): void
    {
        $schema = <<<JSON
            {
                "type": "record",
                "name": "Metadata",
                "fields": [
                    {"name": "properties", "type": {"type": "map", "values": "string"}}
                ]
            }
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\Data');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(1, $files);
        self::assertArrayHasKey('/generated/Metadata.php', $files);

        $expected = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Data;
            
            final class Metadata implements \JsonSerializable
            {
                /** @var array<string, string> */
                private array \$properties;
                /**
                 * @param array<string, string> \$properties
                 */
                public function __construct(array \$properties)
                {
                    \$this->properties = \$properties;
                }
                /** @return array<string, string> */
                public function properties(): array
                {
                    return \$this->properties;
                }
                public function jsonSerialize(): mixed
                {
                    return ['properties' => \$this->properties];
                }
            }

            PHP;

        self::assertEquals($expected, $files['/generated/Metadata.php']);
    }

    #[Test]
    public function record_with_enum_field(): void
    {
        $schema = <<<JSON
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
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\Vehicles');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(2, $files);
        self::assertArrayHasKey('/generated/Car.php', $files);
        self::assertArrayHasKey('/generated/FuelType.php', $files);

        $expectedCar = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Vehicles;
            
            final class Car implements \JsonSerializable
            {
                private string \$brand;
                private \App\Vehicles\FuelType \$fuel;
                public function __construct(string \$brand, \App\Vehicles\FuelType \$fuel)
                {
                    \$this->brand = \$brand;
                    \$this->fuel = \$fuel;
                }
                public function brand(): string
                {
                    return \$this->brand;
                }
                public function fuel(): \App\Vehicles\FuelType
                {
                    return \$this->fuel;
                }
                public function jsonSerialize(): mixed
                {
                    return ['brand' => \$this->brand, 'fuel' => \$this->fuel->value];
                }
            }

            PHP;

        self::assertEquals($expectedCar, $files['/generated/Car.php']);

        $expectedEnum = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Vehicles;
            
            enum FuelType : string
            {
                case GASOLINE = 'gasoline';
                case DIESEL = 'diesel';
                case ELECTRIC = 'electric';
            }

            PHP;

        self::assertEquals($expectedEnum, $files['/generated/FuelType.php']);
    }

    #[Test]
    public function record_with_nullable_field(): void
    {
        $schema = <<<JSON
            {
                "type": "record",
                "name": "Profile",
                "fields": [
                    {"name": "username", "type": "string"},
                    {"name": "bio", "type": ["null", "string"], "default": null}
                ]
            }
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\Social');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(1, $files);
        self::assertArrayHasKey('/generated/Profile.php', $files);

        $expected = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Social;
            
            final class Profile implements \JsonSerializable
            {
                private string \$username;
                private null|string \$bio = null;
                public function __construct(string \$username, null|string \$bio = null)
                {
                    \$this->username = \$username;
                    \$this->bio = \$bio;
                }
                public function username(): string
                {
                    return \$this->username;
                }
                public function bio(): null|string
                {
                    return \$this->bio;
                }
                public function jsonSerialize(): mixed
                {
                    return ['username' => \$this->username, 'bio' => \$this->bio];
                }
            }

            PHP;

        self::assertEquals($expected, $files['/generated/Profile.php']);
    }

    #[Test]
    public function record_with_all_primitive_types(): void
    {
        $schema = <<<JSON
            {
                "type": "record",
                "name": "AllTypes",
                "fields": [
                    {"name": "nullField", "type": "null"},
                    {"name": "boolField", "type": "boolean"},
                    {"name": "intField", "type": "int"},
                    {"name": "longField", "type": "long"},
                    {"name": "floatField", "type": "float"},
                    {"name": "doubleField", "type": "double"},
                    {"name": "stringField", "type": "string"},
                    {"name": "bytesField", "type": "bytes"}
                ]
            }
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\Types');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(1, $files);
        self::assertArrayHasKey('/generated/AllTypes.php', $files);

        $expected = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Types;
            
            final class AllTypes implements \JsonSerializable
            {
                private null \$nullField;
                private bool \$boolField;
                private int \$intField;
                private int \$longField;
                private float \$floatField;
                private float \$doubleField;
                private string \$stringField;
                private string \$bytesField;
                public function __construct(null \$nullField, bool \$boolField, int \$intField, int \$longField, float \$floatField, float \$doubleField, string \$stringField, string \$bytesField)
                {
                    \$this->nullField = \$nullField;
                    \$this->boolField = \$boolField;
                    \$this->intField = \$intField;
                    \$this->longField = \$longField;
                    \$this->floatField = \$floatField;
                    \$this->doubleField = \$doubleField;
                    \$this->stringField = \$stringField;
                    \$this->bytesField = \$bytesField;
                }
                public function nullField(): null
                {
                    return \$this->nullField;
                }
                public function boolField(): bool
                {
                    return \$this->boolField;
                }
                public function intField(): int
                {
                    return \$this->intField;
                }
                public function longField(): int
                {
                    return \$this->longField;
                }
                public function floatField(): float
                {
                    return \$this->floatField;
                }
                public function doubleField(): float
                {
                    return \$this->doubleField;
                }
                public function stringField(): string
                {
                    return \$this->stringField;
                }
                public function bytesField(): string
                {
                    return \$this->bytesField;
                }
                public function jsonSerialize(): mixed
                {
                    return ['nullField' => \$this->nullField, 'boolField' => \$this->boolField, 'intField' => \$this->intField, 'longField' => \$this->longField, 'floatField' => \$this->floatField, 'doubleField' => \$this->doubleField, 'stringField' => \$this->stringField, 'bytesField' => \$this->bytesField];
                }
            }

            PHP;

        self::assertEquals($expected, $files['/generated/AllTypes.php']);
    }

    #[Test]
    public function record_with_nested_array_of_records(): void
    {
        $schema = <<<JSON
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
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\Org');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(2, $files);
        self::assertArrayHasKey('/generated/Team.php', $files);
        self::assertArrayHasKey('/generated/Member.php', $files);

        $expectedTeam = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Org;
            
            final class Team implements \JsonSerializable
            {
                private string \$name;
                /** @var list<\App\Org\Member> */
                private array \$members;
                /**
                 * @param list<\App\Org\Member> \$members
                 */
                public function __construct(string \$name, array \$members)
                {
                    \$this->name = \$name;
                    \$this->members = \$members;
                }
                public function name(): string
                {
                    return \$this->name;
                }
                /** @return list<\App\Org\Member> */
                public function members(): array
                {
                    return \$this->members;
                }
                public function jsonSerialize(): mixed
                {
                    return ['name' => \$this->name, 'members' => \$this->members];
                }
            }

            PHP;

        self::assertEquals($expectedTeam, $files['/generated/Team.php']);

        $expectedMember = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Org;
            
            final class Member implements \JsonSerializable
            {
                private string \$name;
                private string \$role;
                public function __construct(string \$name, string \$role)
                {
                    \$this->name = \$name;
                    \$this->role = \$role;
                }
                public function name(): string
                {
                    return \$this->name;
                }
                public function role(): string
                {
                    return \$this->role;
                }
                public function jsonSerialize(): mixed
                {
                    return ['name' => \$this->name, 'role' => \$this->role];
                }
            }

            PHP;

        self::assertEquals($expectedMember, $files['/generated/Member.php']);
    }

    #[Test]
    public function record_with_multiple_union_types(): void
    {
        $schema = <<<JSON
            {
                "type": "record",
                "name": "Event",
                "fields": [
                    {"name": "payload", "type": ["null", "string", "int", "boolean"]}
                ]
            }
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\Events');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(1, $files);
        self::assertArrayHasKey('/generated/Event.php', $files);

        $expected = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Events;
            
            final class Event implements \JsonSerializable
            {
                private null|string|int|bool \$payload;
                public function __construct(null|string|int|bool \$payload)
                {
                    \$this->payload = \$payload;
                }
                public function payload(): null|string|int|bool
                {
                    return \$this->payload;
                }
                public function jsonSerialize(): mixed
                {
                    return ['payload' => \$this->payload];
                }
            }

            PHP;

        self::assertEquals($expected, $files['/generated/Event.php']);
    }

    #[Test]
    public function record_with_nested_record_field(): void
    {
        $schema = <<<JSON
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
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\Shop');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(2, $files);
        self::assertArrayHasKey('/generated/Order.php', $files);
        self::assertArrayHasKey('/generated/Address.php', $files);

        $expectedOrder = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Shop;
            
            final class Order implements \JsonSerializable
            {
                private int \$id;
                private \App\Shop\Address \$address;
                public function __construct(int \$id, \App\Shop\Address \$address)
                {
                    \$this->id = \$id;
                    \$this->address = \$address;
                }
                public function id(): int
                {
                    return \$this->id;
                }
                public function address(): \App\Shop\Address
                {
                    return \$this->address;
                }
                public function jsonSerialize(): mixed
                {
                    return ['id' => \$this->id, 'address' => \$this->address];
                }
            }

            PHP;

        self::assertEquals($expectedOrder, $files['/generated/Order.php']);

        $expectedAddress = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Shop;
            
            final class Address implements \JsonSerializable
            {
                private string \$street;
                private string \$city;
                public function __construct(string \$street, string \$city)
                {
                    \$this->street = \$street;
                    \$this->city = \$city;
                }
                public function street(): string
                {
                    return \$this->street;
                }
                public function city(): string
                {
                    return \$this->city;
                }
                public function jsonSerialize(): mixed
                {
                    return ['street' => \$this->street, 'city' => \$this->city];
                }
            }

            PHP;

        self::assertEquals($expectedAddress, $files['/generated/Address.php']);
    }

    #[Test]
    public function enum_with_single_symbol(): void
    {
        $schema = <<<JSON
            {
                "type": "enum",
                "name": "Singleton",
                "symbols": ["only"]
            }
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\Enums');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(1, $files);
        self::assertArrayHasKey('/generated/Singleton.php', $files);

        $expected = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Enums;
            
            enum Singleton : string
            {
                case ONLY = 'only';
            }

            PHP;

        self::assertEquals($expected, $files['/generated/Singleton.php']);
    }

    #[Test]
    public function record_with_nullable_record_field(): void
    {
        $schema = <<<JSON
            {
                "type": "record",
                "name": "Employee",
                "fields": [
                    {"name": "name", "type": "string"},
                    {
                        "name": "manager",
                        "type": [
                            "null",
                            {
                                "type": "record",
                                "name": "Manager",
                                "fields": [
                                    {"name": "name", "type": "string"},
                                    {"name": "department", "type": "string"}
                                ]
                            }
                        ],
                        "default": null
                    }
                ]
            }
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\HR');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(2, $files);
        self::assertArrayHasKey('/generated/Employee.php', $files);
        self::assertArrayHasKey('/generated/Manager.php', $files);

        $expectedEmployee = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\HR;
            
            final class Employee implements \JsonSerializable
            {
                private string \$name;
                private null|\App\HR\Manager \$manager = null;
                public function __construct(string \$name, null|\App\HR\Manager \$manager = null)
                {
                    \$this->name = \$name;
                    \$this->manager = \$manager;
                }
                public function name(): string
                {
                    return \$this->name;
                }
                public function manager(): null|\App\HR\Manager
                {
                    return \$this->manager;
                }
                public function jsonSerialize(): mixed
                {
                    return ['name' => \$this->name, 'manager' => \$this->manager];
                }
            }

            PHP;

        self::assertEquals($expectedEmployee, $files['/generated/Employee.php']);

        $expectedManager = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\HR;
            
            final class Manager implements \JsonSerializable
            {
                private string \$name;
                private string \$department;
                public function __construct(string \$name, string \$department)
                {
                    \$this->name = \$name;
                    \$this->department = \$department;
                }
                public function name(): string
                {
                    return \$this->name;
                }
                public function department(): string
                {
                    return \$this->department;
                }
                public function jsonSerialize(): mixed
                {
                    return ['name' => \$this->name, 'department' => \$this->department];
                }
            }

            PHP;

        self::assertEquals($expectedManager, $files['/generated/Manager.php']);
    }

    #[Test]
    public function record_with_map_of_records(): void
    {
        $schema = <<<JSON
            {
                "type": "record",
                "name": "Library",
                "fields": [
                    {"name": "name", "type": "string"},
                    {
                        "name": "books",
                        "type": {
                            "type": "map",
                            "values": {
                                "type": "record",
                                "name": "Book",
                                "fields": [
                                    {"name": "title", "type": "string"},
                                    {"name": "pages", "type": "int"}
                                ]
                            }
                        }
                    }
                ]
            }
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\Library');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(2, $files);
        self::assertArrayHasKey('/generated/Library.php', $files);
        self::assertArrayHasKey('/generated/Book.php', $files);

        $expectedLibrary = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Library;
            
            final class Library implements \JsonSerializable
            {
                private string \$name;
                /** @var array<string, \App\Library\Book> */
                private array \$books;
                /**
                 * @param array<string, \App\Library\Book> \$books
                 */
                public function __construct(string \$name, array \$books)
                {
                    \$this->name = \$name;
                    \$this->books = \$books;
                }
                public function name(): string
                {
                    return \$this->name;
                }
                /** @return array<string, \App\Library\Book> */
                public function books(): array
                {
                    return \$this->books;
                }
                public function jsonSerialize(): mixed
                {
                    return ['name' => \$this->name, 'books' => \$this->books];
                }
            }

            PHP;

        self::assertEquals($expectedLibrary, $files['/generated/Library.php']);

        $expectedBook = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Library;
            
            final class Book implements \JsonSerializable
            {
                private string \$title;
                private int \$pages;
                public function __construct(string \$title, int \$pages)
                {
                    \$this->title = \$title;
                    \$this->pages = \$pages;
                }
                public function title(): string
                {
                    return \$this->title;
                }
                public function pages(): int
                {
                    return \$this->pages;
                }
                public function jsonSerialize(): mixed
                {
                    return ['title' => \$this->title, 'pages' => \$this->pages];
                }
            }

            PHP;

        self::assertEquals($expectedBook, $files['/generated/Book.php']);
    }

    #[Test]
    public function record_with_record_reuse_by_name(): void
    {
        $schema = <<<JSON
            {
                "type": "record",
                "name": "Invoice",
                "fields": [
                    {"name": "id", "type": "int"},
                    {
                        "name": "billingAddress",
                        "type": {
                            "type": "record",
                            "name": "PostalAddress",
                            "fields": [
                                {"name": "street", "type": "string"},
                                {"name": "zip", "type": "string"}
                            ]
                        }
                    },
                    {
                        "name": "shippingAddress",
                        "type": "PostalAddress"
                    }
                ]
            }
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\Billing');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(2, $files);
        self::assertArrayHasKey('/generated/Invoice.php', $files);
        self::assertArrayHasKey('/generated/PostalAddress.php', $files);

        $expectedInvoice = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Billing;
            
            final class Invoice implements \JsonSerializable
            {
                private int \$id;
                private \App\Billing\PostalAddress \$billingAddress;
                private \App\Billing\PostalAddress \$shippingAddress;
                public function __construct(int \$id, \App\Billing\PostalAddress \$billingAddress, \App\Billing\PostalAddress \$shippingAddress)
                {
                    \$this->id = \$id;
                    \$this->billingAddress = \$billingAddress;
                    \$this->shippingAddress = \$shippingAddress;
                }
                public function id(): int
                {
                    return \$this->id;
                }
                public function billingAddress(): \App\Billing\PostalAddress
                {
                    return \$this->billingAddress;
                }
                public function shippingAddress(): \App\Billing\PostalAddress
                {
                    return \$this->shippingAddress;
                }
                public function jsonSerialize(): mixed
                {
                    return ['id' => \$this->id, 'billingAddress' => \$this->billingAddress, 'shippingAddress' => \$this->shippingAddress];
                }
            }

            PHP;

        self::assertEquals($expectedInvoice, $files['/generated/Invoice.php']);
    }

    #[Test]
    public function record_with_array_default_value(): void
    {
        $schema = <<<JSON
            {
                "type": "record",
                "name": "Settings",
                "fields": [
                    {"name": "tags", "type": {"type": "array", "items": "string"}, "default": []}
                ]
            }
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\Config');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(1, $files);
        self::assertArrayHasKey('/generated/Settings.php', $files);

        $expected = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Config;
            
            final class Settings implements \JsonSerializable
            {
                /** @var list<string> */
                private array \$tags = [];
                /**
                 * @param list<string> \$tags
                 */
                public function __construct(array \$tags = [])
                {
                    \$this->tags = \$tags;
                }
                /** @return list<string> */
                public function tags(): array
                {
                    return \$this->tags;
                }
                public function jsonSerialize(): mixed
                {
                    return ['tags' => \$this->tags];
                }
            }

            PHP;

        self::assertEquals($expected, $files['/generated/Settings.php']);
    }

    #[Test]
    public function record_with_mixed_default_and_required_fields(): void
    {
        $schema = <<<JSON
            {
                "type": "record",
                "name": "Item",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "quantity", "type": "int", "default": 1},
                    {"name": "description", "type": "string", "default": "N/A"}
                ]
            }
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\Inventory');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(1, $files);
        self::assertArrayHasKey('/generated/Item.php', $files);

        $expected = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Inventory;
            
            final class Item implements \JsonSerializable
            {
                private string \$name;
                private int \$quantity = 1;
                private string \$description = 'N/A';
                public function __construct(string \$name, int \$quantity = 1, string \$description = 'N/A')
                {
                    \$this->name = \$name;
                    \$this->quantity = \$quantity;
                    \$this->description = \$description;
                }
                public function name(): string
                {
                    return \$this->name;
                }
                public function quantity(): int
                {
                    return \$this->quantity;
                }
                public function description(): string
                {
                    return \$this->description;
                }
                public function jsonSerialize(): mixed
                {
                    return ['name' => \$this->name, 'quantity' => \$this->quantity, 'description' => \$this->description];
                }
            }

            PHP;

        self::assertEquals($expected, $files['/generated/Item.php']);
    }

    #[Test]
    public function record_with_nullable_enum_field(): void
    {
        $schema = <<<JSON
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
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\Tasks');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(2, $files);
        self::assertArrayHasKey('/generated/Task.php', $files);
        self::assertArrayHasKey('/generated/Priority.php', $files);

        $expectedTask = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Tasks;
            
            final class Task implements \JsonSerializable
            {
                private string \$title;
                private null|\App\Tasks\Priority \$priority = null;
                public function __construct(string \$title, null|\App\Tasks\Priority \$priority = null)
                {
                    \$this->title = \$title;
                    \$this->priority = \$priority;
                }
                public function title(): string
                {
                    return \$this->title;
                }
                public function priority(): null|\App\Tasks\Priority
                {
                    return \$this->priority;
                }
                public function jsonSerialize(): mixed
                {
                    return ['title' => \$this->title, 'priority' => \$this->priority?->value];
                }
            }

            PHP;

        self::assertEquals($expectedTask, $files['/generated/Task.php']);

        $expectedPriority = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Tasks;
            
            enum Priority : string
            {
                case LOW = 'low';
                case MEDIUM = 'medium';
                case HIGH = 'high';
            }

            PHP;

        self::assertEquals($expectedPriority, $files['/generated/Priority.php']);
    }

    #[Test]
    public function record_with_nullable_array_field(): void
    {
        $schema = <<<JSON
            {
                "type": "record",
                "name": "Report",
                "fields": [
                    {"name": "title", "type": "string"},
                    {
                        "name": "scores",
                        "type": ["null", {"type": "array", "items": "int"}],
                        "default": null
                    }
                ]
            }
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\Reports');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(1, $files);
        self::assertArrayHasKey('/generated/Report.php', $files);

        $expected = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Reports;
            
            final class Report implements \JsonSerializable
            {
                private string \$title;
                /** @var null|list<int> */
                private null|array \$scores = null;
                /**
                 * @param null|list<int> \$scores
                 */
                public function __construct(string \$title, null|array \$scores = null)
                {
                    \$this->title = \$title;
                    \$this->scores = \$scores;
                }
                public function title(): string
                {
                    return \$this->title;
                }
                /** @return null|list<int> */
                public function scores(): null|array
                {
                    return \$this->scores;
                }
                public function jsonSerialize(): mixed
                {
                    return ['title' => \$this->title, 'scores' => \$this->scores];
                }
            }

            PHP;

        self::assertEquals($expected, $files['/generated/Report.php']);
    }

    #[Test]
    public function record_with_nullable_map_field(): void
    {
        $schema = <<<JSON
            {
                "type": "record",
                "name": "Dashboard",
                "fields": [
                    {
                        "name": "widgets",
                        "type": ["null", {"type": "map", "values": "string"}],
                        "default": null
                    }
                ]
            }
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\UI');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(1, $files);
        self::assertArrayHasKey('/generated/Dashboard.php', $files);

        $expected = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\UI;
            
            final class Dashboard implements \JsonSerializable
            {
                /** @var null|array<string, string> */
                private null|array \$widgets = null;
                /**
                 * @param null|array<string, string> \$widgets
                 */
                public function __construct(null|array \$widgets = null)
                {
                    \$this->widgets = \$widgets;
                }
                /** @return null|array<string, string> */
                public function widgets(): null|array
                {
                    return \$this->widgets;
                }
                public function jsonSerialize(): mixed
                {
                    return ['widgets' => \$this->widgets];
                }
            }

            PHP;

        self::assertEquals($expected, $files['/generated/Dashboard.php']);
    }

    #[Test]
    public function record_with_nested_array_of_arrays(): void
    {
        $schema = <<<JSON
            {
                "type": "record",
                "name": "Matrix",
                "fields": [
                    {
                        "name": "rows",
                        "type": {
                            "type": "array",
                            "items": {
                                "type": "array",
                                "items": "int"
                            }
                        }
                    }
                ]
            }
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\Math');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(1, $files);
        self::assertArrayHasKey('/generated/Matrix.php', $files);

        $expected = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Math;
            
            final class Matrix implements \JsonSerializable
            {
                /** @var list<list<int>> */
                private array \$rows;
                /**
                 * @param list<list<int>> \$rows
                 */
                public function __construct(array \$rows)
                {
                    \$this->rows = \$rows;
                }
                /** @return list<list<int>> */
                public function rows(): array
                {
                    return \$this->rows;
                }
                public function jsonSerialize(): mixed
                {
                    return ['rows' => \$this->rows];
                }
            }

            PHP;

        self::assertEquals($expected, $files['/generated/Matrix.php']);
    }

    #[Test]
    public function record_with_map_of_arrays(): void
    {
        $schema = <<<JSON
            {
                "type": "record",
                "name": "Index",
                "fields": [
                    {
                        "name": "entries",
                        "type": {
                            "type": "map",
                            "values": {
                                "type": "array",
                                "items": "string"
                            }
                        }
                    }
                ]
            }
            JSON;

        $avroSchema = AvroSchema::parse($schema);
        $generator = new AvroCodeGenerator('/generated', 'App\\Search');
        $generator->translate($avroSchema);
        $files = $generator->generate();

        self::assertCount(1, $files);
        self::assertArrayHasKey('/generated/Index.php', $files);

        $expected = <<<PHP
            <?php
            
            declare(strict_types=1);
            
            namespace App\Search;
            
            final class Index implements \JsonSerializable
            {
                /** @var array<string, list<string>> */
                private array \$entries;
                /**
                 * @param array<string, list<string>> \$entries
                 */
                public function __construct(array \$entries)
                {
                    \$this->entries = \$entries;
                }
                /** @return array<string, list<string>> */
                public function entries(): array
                {
                    return \$this->entries;
                }
                public function jsonSerialize(): mixed
                {
                    return ['entries' => \$this->entries];
                }
            }

            PHP;

        self::assertEquals($expected, $files['/generated/Index.php']);
    }
}
