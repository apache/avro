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

use Apache\Avro\AvroException;
use Apache\Avro\Schema\AvroSchema;
use Apache\Avro\Schema\AvroSchemaParseException;
use PHPUnit\Framework\TestCase;

class SchemaExample
{
    public $schema_string;
    public $is_valid;
    public $name;
    public $comment;
    public $normalized_schema_string;

    public function __construct(
        $schema_string,
        $is_valid,
        $normalized_schema_string = null,
        $name = null,
        $comment = null
    ) {
        $this->schema_string = $schema_string;
        $this->is_valid = $is_valid;
        $this->name = $name ? $name : $schema_string;
        $this->normalized_schema_string = $normalized_schema_string
            ? $normalized_schema_string : json_encode(json_decode($schema_string, true));
        $this->comment = $comment;
    }
}

class SchemaTest extends TestCase
{
    private static $examples = [];
    private static $valid_examples = [];

    public function test_json_decode(): void
    {
        $this->assertEquals(null, json_decode('null', true));
        $this->assertEquals(32, json_decode('32', true));
        $this->assertEquals('32', json_decode('"32"', true));
        $this->assertEquals(["foo" => 27], (array) json_decode('{"foo": 27}'));
        $this->assertIsArray(json_decode('{"foo": 27}', true));
        $this->assertEquals(["foo" => 27], json_decode('{"foo": 27}', true));
        $this->assertEquals(["bar", "baz", "blurfl"],
            json_decode('["bar", "baz", "blurfl"]', true));
        $this->assertIsNotArray(json_decode('null', true));
        $this->assertEquals(["type" => 'null'], json_decode('{"type": "null"}', true));

        // PHP now only accept lowercase true, and rejects TRUE etc.
        // https://php.net/manual/en/migration56.incompatible.php#migration56.incompatible.json-decode
        $this->assertEquals(true, json_decode('true', true), 'true');

        $this->assertEquals('boolean', json_decode('"boolean"'));
    }

    public function schema_examples_provider(): array
    {
        self::make_examples();
        $ary = [];
        foreach (self::$examples as $example) {
            $ary[] = [$example];
        }
        return $ary;
    }

    protected static function make_examples(): void
    {
        $primitive_examples = array_merge([
            new SchemaExample('"True"', false),
            new SchemaExample('{"no_type": "test"}', false),
            new SchemaExample('{"type": "panther"}', false)
        ],
            self::make_primitive_examples());

        $array_examples = [
            new SchemaExample('{"type": "array", "items": "long"}', true),
            new SchemaExample('
    {"type": "array",
     "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}
    ', true)
        ];

        $map_examples = [
            new SchemaExample('{"type": "map", "values": "long"}', true),
            new SchemaExample('
    {"type": "map",
     "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}
    ', true)
        ];

        $union_examples = [
            new SchemaExample('["string", "null", "long"]', true),
            new SchemaExample('["null", "null"]', false),
            new SchemaExample('["long", "long"]', false),
            new SchemaExample('
    [{"type": "array", "items": "long"}
     {"type": "array", "items": "string"}]
    ', false),
            new SchemaExample('["long",
                          {"type": "long"},
                          "int"]', false),
            new SchemaExample('["long",
                          {"type": "array", "items": "long"},
                          {"type": "map", "values": "long"},
                          "int"]', true),
            new SchemaExample('["long",
                          ["string", "null"],
                          "int"]', false),
            new SchemaExample('["long",
                          ["string", "null"],
                          "int"]', false),
            new SchemaExample('["null", "boolean", "int", "long", "float", "double",
                          "string", "bytes",
                          {"type": "array", "items":"int"},
                          {"type": "map", "values":"int"},
                          {"name": "bar", "type":"record",
                           "fields":[{"name":"label", "type":"string"}]},
                          {"name": "foo", "type":"fixed",
                           "size":16},
                          {"name": "baz", "type":"enum", "symbols":["A", "B", "C"]}
                         ]', true,
                '["null","boolean","int","long","float","double","string","bytes",{"type":"array","items":"int"},{"type":"map","values":"int"},{"type":"record","name":"bar","fields":[{"name":"label","type":"string"}]},{"type":"fixed","name":"foo","size":16},{"type":"enum","name":"baz","symbols":["A","B","C"]}]'),
            new SchemaExample('
    [{"name":"subtract", "namespace":"com.example",
      "type":"record",
      "fields":[{"name":"minuend", "type":"int"},
                {"name":"subtrahend", "type":"int"}]},
      {"name": "divide", "namespace":"com.example",
      "type":"record",
      "fields":[{"name":"quotient", "type":"int"},
                {"name":"dividend", "type":"int"}]},
      {"type": "array", "items": "string"}]
    ', true,
                '[{"type":"record","name":"subtract","namespace":"com.example","fields":[{"name":"minuend","type":"int"},{"name":"subtrahend","type":"int"}]},{"type":"record","name":"divide","namespace":"com.example","fields":[{"name":"quotient","type":"int"},{"name":"dividend","type":"int"}]},{"type":"array","items":"string"}]'),
        ];

        $fixed_examples = [
            new SchemaExample('{"type": "fixed", "name": "Test", "size": 1}', true),
            new SchemaExample('
    {"type": "fixed",
     "name": "MyFixed",
     "namespace": "org.apache.hadoop.avro",
     "size": 1}
    ', true),
            new SchemaExample('
    {"type": "fixed",
     "name": "Missing size"}
    ', false),
            new SchemaExample('
    {"type": "fixed",
     "size": 314}
    ', false),
            new SchemaExample('{"type":"fixed","name":"ex","doc":"this should be ignored","size": 314}',
                true,
                '{"type":"fixed","name":"ex","size":314}'),
            new SchemaExample('{"name": "bar",
                          "namespace": "com.example",
                          "type": "fixed",
                          "size": 32 }', true,
                '{"type":"fixed","name":"bar","namespace":"com.example","size":32}'),
            new SchemaExample('{"name": "com.example.bar",
                          "type": "fixed",
                          "size": 32 }', true,
                '{"type":"fixed","name":"bar","namespace":"com.example","size":32}')
        ];

        $fixed_examples [] = new SchemaExample(
            '{"type":"fixed","name":"_x.bar","size":4}', true,
            '{"type":"fixed","name":"bar","namespace":"_x","size":4}');
        $fixed_examples [] = new SchemaExample(
            '{"type":"fixed","name":"baz._x","size":4}', true,
            '{"type":"fixed","name":"_x","namespace":"baz","size":4}');
        $fixed_examples [] = new SchemaExample(
            '{"type":"fixed","name":"baz.3x","size":4}', false);
        $fixed_examples[] = new SchemaExample(
            '{"type":"fixed", "name":"Fixed2", "aliases":["Fixed1"], "size": 2}', true);

        $enum_examples = array(
            new SchemaExample('{"type": "enum", "name": "Test", "symbols": ["A", "B"]}', true),
            new SchemaExample('
    {"type": "enum",
     "name": "Status",
     "symbols": "Normal Caution Critical"}
    ', false),
            new SchemaExample('
    {"type": "enum",
     "name": [ 0, 1, 1, 2, 3, 5, 8 ],
     "symbols": ["Golden", "Mean"]}
    ', false),
            new SchemaExample('
    {"type": "enum",
     "symbols" : ["I", "will", "fail", "no", "name"]}
    ', false),
            new SchemaExample('
    {"type": "enum",
     "name": "Test"
     "symbols" : ["AA", "AA"]}
    ', false),
            new SchemaExample('{"type":"enum","name":"Test","symbols":["AA", 16]}',
                false),
            new SchemaExample('
    {"type": "enum",
     "name": "blood_types",
     "doc": "AB is freaky.",
     "symbols" : ["A", "AB", "B", "O"]}
    ', true),
            new SchemaExample('
    {"type": "enum",
     "name": "blood-types",
     "doc": 16,
     "symbols" : ["A", "AB", "B", "O"]}
    ', false)
        );


        $record_examples = array();
        $record_examples [] = new SchemaExample('
    {"type": "record",
     "name": "Test",
     "fields": [{"name": "f",
                 "type": "long"}]}
    ', true);
        $record_examples [] = new SchemaExample('
    {"type": "error",
     "name": "Test",
     "fields": [{"name": "f",
                 "type": "long"}]}
    ', true);
        $record_examples [] = new SchemaExample('
    {"type": "record",
     "name": "Node",
     "fields": [{"name": "label", "type": "string"},
                {"name": "children",
                 "type": {"type": "array", "items": "Node"}}]}
    ', true);
        $record_examples [] = new SchemaExample('
    {"type": "record",
     "name": "ListLink",
     "fields": [{"name": "car", "type": "int"},
                {"name": "cdr", "type": "ListLink"}]}
    ', true);
        $record_examples [] = new SchemaExample('
    {"type": "record",
     "name": "Lisp",
     "fields": [{"name": "value",
                 "type": ["null", "string"]}]}
    ', true);
        $record_examples [] = new SchemaExample('
    {"type": "record",
     "name": "Lisp",
     "fields": [{"name": "value",
                 "type": ["null", "string",
                          {"type": "record",
                           "name": "Cons",
                           "fields": [{"name": "car", "type": "string"},
                                      {"name": "cdr", "type": "string"}]}]}]}
    ', true);
        $record_examples [] = new SchemaExample('
    {"type": "record",
     "name": "Lisp",
     "fields": [{"name": "value",
                 "type": ["null", "string",
                          {"type": "record",
                           "name": "Cons",
                           "fields": [{"name": "car", "type": "Lisp"},
                                      {"name": "cdr", "type": "Lisp"}]}]}]}
    ', true);
        $record_examples [] = new SchemaExample('
    {"type": "record",
     "name": "HandshakeRequest",
     "namespace": "org.apache.avro.ipc",
     "fields": [{"name": "clientHash",
                 "type": {"type": "fixed", "name": "MD5", "size": 16}},
                {"name": "meta",
                 "type": ["null", {"type": "map", "values": "bytes"}]}]}
    ', true);
        $record_examples [] = new SchemaExample('
    {"type": "record",
     "name": "HandshakeRequest",
     "namespace": "org.apache.avro.ipc",
     "fields": [{"name": "clientHash",
                 "type": {"type": "fixed", "name": "MD5", "size": 16}},
                {"name": "clientProtocol", "type": ["null", "string"]},
                {"name": "serverHash", "type": "MD5"},
                {"name": "meta",
                 "type": ["null", {"type": "map", "values": "bytes"}]}]}
    ', true);
        $record_examples [] = new SchemaExample('
    {"type": "record",
     "name": "HandshakeResponse",
     "namespace": "org.apache.avro.ipc",
     "fields": [{"name": "match",
                 "type": {"type": "enum",
                          "name": "HandshakeMatch",
                          "symbols": ["BOTH", "CLIENT", "NONE"]}},
                {"name": "serverProtocol", "type": ["null", "string"]},
                {"name": "serverHash",
                 "type": ["null",
                          {"name": "MD5", "size": 16, "type": "fixed"}]},
                {"name": "meta",
                 "type": ["null", {"type": "map", "values": "bytes"}]}]}
    ', true,
            '{"type":"record","name":"HandshakeResponse","namespace":"org.apache.avro.ipc","fields":[{"name":"match","type":{"type":"enum","name":"HandshakeMatch","symbols":["BOTH","CLIENT","NONE"]}},{"name":"serverProtocol","type":["null","string"]},{"name":"serverHash","type":["null",{"type":"fixed","name":"MD5","size":16}]},{"name":"meta","type":["null",{"type":"map","values":"bytes"}]}]}'
        );
        $record_examples [] = new SchemaExample('{"type": "record",
 "namespace": "org.apache.avro",
 "name": "Interop",
 "fields": [{"type": {"fields": [{"type": {"items": "org.apache.avro.Node",
                                           "type": "array"},
                                  "name": "children"}],
                      "type": "record",
                      "name": "Node"},
             "name": "recordField"}]}
', true,
            '{"type":"record","name":"Interop","namespace":"org.apache.avro","fields":[{"name":"recordField","type":{"type":"record","name":"Node","fields":[{"name":"children","type":{"type":"array","items":"Node"}}]}}]}');
        $record_examples [] = new SchemaExample('{"type": "record",
 "namespace": "org.apache.avro",
 "name": "Interop",
 "fields": [{"type": {"symbols": ["A", "B", "C"], "type": "enum", "name": "Kind"},
             "name": "enumField"},
            {"type": {"fields": [{"type": "string", "name": "label"},
                                 {"type": {"items": "org.apache.avro.Node", "type": "array"},
                                  "name": "children"}],
                      "type": "record",
                      "name": "Node"},
             "name": "recordField"}]}', true,
            '{"type":"record","name":"Interop","namespace":"org.apache.avro","fields":[{"name":"enumField","type":{"type":"enum","name":"Kind","symbols":["A","B","C"]}},{"name":"recordField","type":{"type":"record","name":"Node","fields":[{"name":"label","type":"string"},{"name":"children","type":{"type":"array","items":"Node"}}]}}]}');

        $record_examples [] = new SchemaExample('
    {"type": "record",
     "name": "Interop",
     "namespace": "org.apache.avro",
     "fields": [{"name": "intField", "type": "int"},
                {"name": "longField", "type": "long"},
                {"name": "stringField", "type": "string"},
                {"name": "boolField", "type": "boolean"},
                {"name": "floatField", "type": "float"},
                {"name": "doubleField", "type": "double"},
                {"name": "bytesField", "type": "bytes"},
                {"name": "nullField", "type": "null"},
                {"name": "arrayField",
                 "type": {"type": "array", "items": "double"}},
                {"name": "mapField",
                 "type": {"type": "map",
                          "values": {"name": "Foo",
                                     "type": "record",
                                     "fields": [{"name": "label",
                                                 "type": "string"}]}}},
                {"name": "unionField",
                 "type": ["boolean",
                          "double",
                          {"type": "array", "items": "bytes"}]},
                {"name": "enumField",
                 "type": {"type": "enum",
                          "name": "Kind",
                          "symbols": ["A", "B", "C"]}},
                {"name": "fixedField",
                 "type": {"type": "fixed", "name": "MD5", "size": 16}},
                {"name": "recordField",
                 "type": {"type": "record",
                          "name": "Node",
                          "fields": [{"name": "label", "type": "string"},
                                     {"name": "children",
                                      "type": {"type": "array",
                                               "items": "Node"}}]}}]}
    ', true,
            '{"type":"record","name":"Interop","namespace":"org.apache.avro","fields":[{"name":"intField","type":"int"},{"name":"longField","type":"long"},{"name":"stringField","type":"string"},{"name":"boolField","type":"boolean"},{"name":"floatField","type":"float"},{"name":"doubleField","type":"double"},{"name":"bytesField","type":"bytes"},{"name":"nullField","type":"null"},{"name":"arrayField","type":{"type":"array","items":"double"}},{"name":"mapField","type":{"type":"map","values":{"type":"record","name":"Foo","fields":[{"name":"label","type":"string"}]}}},{"name":"unionField","type":["boolean","double",{"type":"array","items":"bytes"}]},{"name":"enumField","type":{"type":"enum","name":"Kind","symbols":["A","B","C"]}},{"name":"fixedField","type":{"type":"fixed","name":"MD5","size":16}},{"name":"recordField","type":{"type":"record","name":"Node","fields":[{"name":"label","type":"string"},{"name":"children","type":{"type":"array","items":"Node"}}]}}]}');
        $record_examples [] = new SchemaExample('{"type": "record", "namespace": "org.apache.avro", "name": "Interop", "fields": [{"type": "int", "name": "intField"}, {"type": "long", "name": "longField"}, {"type": "string", "name": "stringField"}, {"type": "boolean", "name": "boolField"}, {"type": "float", "name": "floatField"}, {"type": "double", "name": "doubleField"}, {"type": "bytes", "name": "bytesField"}, {"type": "null", "name": "nullField"}, {"type": {"items": "double", "type": "array"}, "name": "arrayField"}, {"type": {"type": "map", "values": {"fields": [{"type": "string", "name": "label"}], "type": "record", "name": "Foo"}}, "name": "mapField"}, {"type": ["boolean", "double", {"items": "bytes", "type": "array"}], "name": "unionField"}, {"type": {"symbols": ["A", "B", "C"], "type": "enum", "name": "Kind"}, "name": "enumField"}, {"type": {"type": "fixed", "name": "MD5", "size": 16}, "name": "fixedField"}, {"type": {"fields": [{"type": "string", "name": "label"}, {"type": {"items": "org.apache.avro.Node", "type": "array"}, "name": "children"}], "type": "record", "name": "Node"}, "name": "recordField"}]}
', true,
            '{"type":"record","name":"Interop","namespace":"org.apache.avro","fields":[{"name":"intField","type":"int"},{"name":"longField","type":"long"},{"name":"stringField","type":"string"},{"name":"boolField","type":"boolean"},{"name":"floatField","type":"float"},{"name":"doubleField","type":"double"},{"name":"bytesField","type":"bytes"},{"name":"nullField","type":"null"},{"name":"arrayField","type":{"type":"array","items":"double"}},{"name":"mapField","type":{"type":"map","values":{"type":"record","name":"Foo","fields":[{"name":"label","type":"string"}]}}},{"name":"unionField","type":["boolean","double",{"type":"array","items":"bytes"}]},{"name":"enumField","type":{"type":"enum","name":"Kind","symbols":["A","B","C"]}},{"name":"fixedField","type":{"type":"fixed","name":"MD5","size":16}},{"name":"recordField","type":{"type":"record","name":"Node","fields":[{"name":"label","type":"string"},{"name":"children","type":{"type":"array","items":"Node"}}]}}]}');
        $record_examples [] = new SchemaExample('
    {"type": "record",
     "name": "ipAddr",
     "fields": [{"name": "addr",
                 "type": [{"name": "IPv6", "type": "fixed", "size": 16},
                          {"name": "IPv4", "type": "fixed", "size": 4}]}]}
    ', true,
            '{"type":"record","name":"ipAddr","fields":[{"name":"addr","type":[{"type":"fixed","name":"IPv6","size":16},{"type":"fixed","name":"IPv4","size":4}]}]}');
        $record_examples [] = new SchemaExample('
    {"type": "record",
     "name": "Address",
     "fields": [{"type": "string"},
                {"type": "string", "name": "City"}]}
    ', false);
        $record_examples [] = new SchemaExample('
    {"type": "record",
     "name": "Event",
     "fields": [{"name": "Sponsor"},
                {"name": "City", "type": "string"}]}
    ', false);
        $record_examples [] = new SchemaExample('
    {"type": "record",
     "fields": "His vision, from the constantly passing bars,"
     "name", "Rainer"}
    ', false);
        $record_examples [] = new SchemaExample('
    {"name": ["Tom", "Jerry"],
     "type": "record",
     "fields": [{"name": "name", "type": "string"}]}
    ', false);
        $record_examples [] = new SchemaExample('
    {"type":"record","name":"foo","doc":"doc string",
     "fields":[{"name":"bar", "type":"int", "order":"ascending", "default":1}]}
',
            true,
            '{"type":"record","name":"foo","doc":"doc string","fields":[{"name":"bar","type":"int","default":1,"order":"ascending"}]}');
        $record_examples [] = new SchemaExample('
    {"type":"record", "name":"foo", "doc":"doc string",
     "fields":[{"name":"bar", "type":"int", "order":"bad"}]}
', false);
        $record_examples[] = new SchemaExample(
            '{"type":"record", "name":"Record2", "aliases":["Record1"]}', false);

        self::$examples = array_merge($primitive_examples,
            $fixed_examples,
            $enum_examples,
            $array_examples,
            $map_examples,
            $union_examples,
            $record_examples);
        self::$valid_examples = array();
        foreach (self::$examples as $example) {
            if ($example->is_valid) {
                self::$valid_examples [] = $example;
            }
        }
    }

    protected static function make_primitive_examples()
    {
        $examples = [];
        foreach ([
                     'null',
                     'boolean',
                     'int',
                     'long',
                     'float',
                     'double',
                     'bytes',
                     'string'
                 ]
                 as $type) {
            $examples [] = new SchemaExample(sprintf('"%s"', $type), true);
            $examples [] = new SchemaExample(sprintf('{"type": "%s"}', $type), true, sprintf('"%s"', $type));
        }
        return $examples;
    }

    /**
     * @dataProvider schema_examples_provider
     */
    function test_parse($example): void
    {
        $schema_string = $example->schema_string;
        try {
            $normalized_schema_string = $example->normalized_schema_string;
            $schema = AvroSchema::parse($schema_string);
            $this->assertTrue($example->is_valid,
                sprintf("schema_string: %s\n",
                    $schema_string));
            $this->assertEquals($normalized_schema_string, (string) $schema);
        } catch (AvroSchemaParseException $e) {
            $this->assertFalse($example->is_valid,
                sprintf("schema_string: %s\n%s",
                    $schema_string,
                    $e->getMessage()));
        }
    }

    public function testToAvroIncludesAliases(): void
    {
        $hash = <<<SCHEMA
                {
                    "type": "record",
                    "name": "test_record",
                    "aliases": ["alt_record"],
                    "fields": [
                        { "name": "f", "type": { "type": "fixed", "size": 2, "name": "test_fixed", "aliases": ["alt_fixed"] } },
                        { "name": "e", "type": { "type": "enum", "symbols": ["A", "B"], "name": "test_enum", "aliases": ["alt_enum"] } }
                    ]
                }
                SCHEMA;
        $schema = AvroSchema::parse($hash);
        $this->assertEquals($schema->toAvro(), json_decode($hash, true));
    }

    public function testValidateFieldAliases()
    {
        $this->expectException(AvroSchemaParseException::class);
        $this->expectExceptionMessage('Invalid aliases value. Must be an array of strings.');
        AvroSchema::parse(<<<SCHEMA
{
    "type": "record",
    "name": "fruits",
    "fields": [
        {
            "name": "banana",
            "type": "string",
            "aliases": "banane"
        }
    ]
}
SCHEMA);
    }

    public function testValidateRecordAliases()
    {
        $this->expectException(AvroSchemaParseException::class);
        $this->expectExceptionMessage('Invalid aliases value. Must be an array of strings.');
        AvroSchema::parse(<<<SCHEMA
{
    "type": "record",
    "name": "fruits",
    "aliases": ["foods", 2],
    "fields": []
}
SCHEMA);
    }

    public function testValidateFixedAliases()
    {
        $this->expectException(AvroSchemaParseException::class);
        $this->expectExceptionMessage('Invalid aliases value. Must be an array of strings.');
        AvroSchema::parse(<<<SCHEMA
{
    "type": "fixed",
    "name": "uuid",
    "size": 36,
    "aliases": "unique_id"
}
SCHEMA);
    }

    public function testValidateEnumAliases()
    {
        $this->expectException(AvroSchemaParseException::class);
        $this->expectExceptionMessage('Invalid aliases value. Must be an array of strings.');
        AvroSchema::parse(<<<SCHEMA
{
    "type": "enum",
    "name": "vowels",
    "aliases": [1, 2],
    "symbols": ["A", "E", "I", "O", "U"]
}
SCHEMA);
    }

    public function testValidateSameAliasMultipleFields()
    {
        $this->expectException(AvroSchemaParseException::class);
        $this->expectExceptionMessage('Alias already in use');
        AvroSchema::parse(<<<SCHEMA
{
    "type": "record",
    "name": "fruits",
    "fields": [
        {"name": "banana", "type": "string", "aliases": [ "yellow" ]},
        {"name": "lemo", "type": "string", "aliases": [ "yellow" ]}
    ]
}
SCHEMA);
    }

    public function testValidateRepeatedAliases()
    {
        $this->expectNotToPerformAssertions();
        AvroSchema::parse(<<<SCHEMA
            {
                "type": "record",
                "name": "fruits",
                "fields": [
                    {
                        "name": "banana",
                        "type": "string",
                        "aliases": [
                            "yellow",
                            "yellow"
                        ]
                    }
                ]
            }
            SCHEMA
        );
    }

    public function testLogicalTypesInRecord(): void
    {
        $avro = <<<AVRO
                {
                "type": "record",
                "name": "fruits",
                "fields": [
                    {
                        "name": "UUID",
                        "type": {
                            "type": "string",
                            "logicalType": "uuid"
                        }
                    },
                    {
                        "name": "decimal",
                        "type": {
                            "type": "bytes",
                            "logicalType": "decimal",
                            "precision": 6,
                            "scale": 2
                        }
                    },
                    {
                        "name": "date",
                        "type": {
                            "type": "int",
                            "logicalType": "date"
                        }
                    },
                    {
                        "name": "timeMillis",
                        "type": {
                            "type": "int",
                            "logicalType": "time-millis"
                        }
                    },
                    {
                        "name": "timeMicros",
                        "type": {
                            "type": "long",
                            "logicalType": "time-micros"
                        }
                    },
                    {
                        "name": "timestampMillis",
                        "type": {
                            "type": "long",
                            "logicalType": "timestamp-millis"
                        }
                    },
                    {
                        "name": "timestampMicros",
                        "type": {
                            "type": "long",
                            "logicalType": "timestamp-micros"
                        }
                    },
                    {
                        "name": "localTimestampMillis",
                        "type": {
                            "type": "long",
                            "logicalType": "local-timestamp-millis"
                        }
                    },
                    {
                        "name": "localTimestampMicros",
                        "type": {
                            "type": "long",
                            "logicalType": "local-timestamp-micros"
                        }
                    },
                    {
                        "name": "duration",
                        "type": {
                            "name": "inner_fixed",
                            "type": "fixed",
                            "size": 12,
                            "logicalType": "duration"
                        }
                    }
                ]
            }
            AVRO;


        $schema = AvroSchema::parse($avro);

        self::assertEquals($schema->toAvro(), json_decode($avro, true));
    }

    public static function invalidDecimalLogicalTypeDataProvider(): array
    {
        return [
            'bytes - invalid precision' => [
                '{"type": "bytes", "logicalType": "decimal", "precision": -1, "scale": 2}',
                new AvroException("Precision '-1' is invalid. It must be a positive integer."),
            ],
            'bytes - invalid value for precision (float)' => [
                '{"type": "bytes", "logicalType": "decimal", "precision": 11.23, "scale": 2}',
                new AvroException("Invalid value '11.23' for 'precision' attribute of decimal logical type."),
            ],
            'bytes - invalid value for precision (string)' => [
                '{"type": "bytes", "logicalType": "decimal", "precision": "banana", "scale": 2}',
                new AvroException("Invalid value 'banana' for 'precision' attribute of decimal logical type."),
            ],
            'bytes - invalid scale' => [
                '{"type": "bytes", "logicalType": "decimal", "precision": 2, "scale": -1}',
                new AvroException("Scale '-1' is invalid. It must be a non-negative integer."),
            ],
            'bytes - invalid scale for precision' => [
                '{"type": "bytes", "logicalType": "decimal", "precision": 2, "scale": 2}',
                new AvroException("Scale must be a lower than precision (scale='2', precision='2')."),
            ],
            'bytes - invalid value for scale (float)' => [
                '{"type": "bytes", "logicalType": "decimal", "precision": 2, "scale": 9.12}',
                new AvroException("Invalid value '9.12' for 'scale' attribute of decimal logical type."),
            ],
            'bytes - invalid value for scale (string)' => [
                '{"type": "bytes", "logicalType": "decimal", "precision": 2, "scale": "two"}',
                new AvroException("Invalid value 'two' for 'scale' attribute of decimal logical type."),
            ],
            'fixed - invalid precision' => [
                '{"name": "fixed_decimal", "type": "fixed", "logicalType": "decimal", "size": 2, "precision": -1, "scale": 2}',
                new AvroException("Precision '-1' is invalid. It must be a positive integer."),
            ],
            'fixed - invalid value for precision with specified size' => [
                '{"name": "fixed_decimal", "type": "fixed", "logicalType": "decimal", "size": 2, "precision": 6, "scale": 2}',
                new AvroException("Invalid precision for specified fixed size (size='2', precision='6')."),
            ],
        ];
    }

    /**
     * @dataProvider invalidDecimalLogicalTypeDataProvider
     */
    public function testDecimalLogicalTypeWithInvalidParameters(string $schema, AvroException $exception): void
    {
        $this->expectException(get_class($exception));
        $this->expectExceptionMessage($exception->getMessage());
        AvroSchema::parse($schema);
    }
}
