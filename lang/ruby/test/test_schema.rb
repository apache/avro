# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'test_help'

class TestSchema < Test::Unit::TestCase
  def test_default_namespace
    schema = Avro::Schema.parse <<-SCHEMA
      {"type": "record", "name": "OuterRecord", "fields": [
        {"name": "field1", "type": {
          "type": "record", "name": "InnerRecord", "fields": []
        }},
        {"name": "field2", "type": "InnerRecord"}
      ]}
    SCHEMA

    assert_equal 'OuterRecord', schema.name
    assert_equal 'OuterRecord', schema.fullname
    assert_nil schema.namespace

    schema.fields.each do |field|
      assert_equal 'InnerRecord', field.type.name
      assert_equal 'InnerRecord', field.type.fullname
      assert_nil field.type.namespace
    end
  end

  def test_inherited_namespace
    schema = Avro::Schema.parse <<-SCHEMA
      {"type": "record", "name": "OuterRecord", "namespace": "my.name.space",
       "fields": [
          {"name": "definition", "type": {
            "type": "record", "name": "InnerRecord", "fields": []
          }},
          {"name": "relativeReference", "type": "InnerRecord"},
          {"name": "absoluteReference", "type": "my.name.space.InnerRecord"}
      ]}
    SCHEMA

    assert_equal 'OuterRecord', schema.name
    assert_equal 'my.name.space.OuterRecord', schema.fullname
    assert_equal 'my.name.space', schema.namespace
    schema.fields.each do |field|
      assert_equal 'InnerRecord', field.type.name
      assert_equal 'my.name.space.InnerRecord', field.type.fullname
      assert_equal 'my.name.space', field.type.namespace
    end
  end

  def test_inherited_namespace_from_dotted_name
    schema = Avro::Schema.parse <<-SCHEMA
      {"type": "record", "name": "my.name.space.OuterRecord", "fields": [
        {"name": "definition", "type": {
          "type": "enum", "name": "InnerEnum", "symbols": ["HELLO", "WORLD"]
        }},
        {"name": "relativeReference", "type": "InnerEnum"},
        {"name": "absoluteReference", "type": "my.name.space.InnerEnum"}
      ]}
    SCHEMA

    assert_equal 'OuterRecord', schema.name
    assert_equal 'my.name.space.OuterRecord', schema.fullname
    assert_equal 'my.name.space', schema.namespace
    schema.fields.each do |field|
      assert_equal 'InnerEnum', field.type.name
      assert_equal 'my.name.space.InnerEnum', field.type.fullname
      assert_equal 'my.name.space', field.type.namespace
    end
  end

  def test_nested_namespaces
    schema = Avro::Schema.parse <<-SCHEMA
      {"type": "record", "name": "outer.OuterRecord", "fields": [
        {"name": "middle", "type": {
          "type": "record", "name": "middle.MiddleRecord", "fields": [
            {"name": "inner", "type": {
              "type": "record", "name": "InnerRecord", "fields": [
                {"name": "recursive", "type": "MiddleRecord"}
              ]
            }}
          ]
        }}
      ]}
    SCHEMA

    assert_equal 'OuterRecord', schema.name
    assert_equal 'outer.OuterRecord', schema.fullname
    assert_equal 'outer', schema.namespace
    middle = schema.fields.first.type
    assert_equal 'MiddleRecord', middle.name
    assert_equal 'middle.MiddleRecord', middle.fullname
    assert_equal 'middle', middle.namespace
    inner = middle.fields.first.type
    assert_equal 'InnerRecord', inner.name
    assert_equal 'middle.InnerRecord', inner.fullname
    assert_equal 'middle', inner.namespace
    assert_equal middle, inner.fields.first.type
  end

  def test_to_avro_includes_namespaces
    schema = Avro::Schema.parse <<-SCHEMA
      {"type": "record", "name": "my.name.space.OuterRecord", "fields": [
        {"name": "definition", "type": {
          "type": "fixed", "name": "InnerFixed", "size": 16
        }},
        {"name": "reference", "type": "InnerFixed"}
      ]}
    SCHEMA

    assert_equal({
      'type' => 'record', 'name' => 'OuterRecord', 'namespace' => 'my.name.space',
      'fields' => [
        {'name' => 'definition', 'type' => {
          'type' => 'fixed', 'name' => 'InnerFixed', 'namespace' => 'my.name.space',
          'size' => 16
        }},
        {'name' => 'reference', 'type' => 'my.name.space.InnerFixed'}
      ]
    }, schema.to_avro)
  end

  def test_to_avro_includes_logical_type
    schema = Avro::Schema.parse <<-SCHEMA
      {"type": "record", "name": "has_logical", "fields": [
        {"name": "dt", "type": {"type": "int", "logicalType": "date"}}]
      }
    SCHEMA

    assert_equal schema.to_avro, {
      'type' => 'record', 'name' => 'has_logical',
      'fields' => [
        {'name' => 'dt', 'type' => {'type' => 'int', 'logicalType' => 'date'}}
      ]
    }
  end

  def test_unknown_named_type
    error = assert_raise Avro::UnknownSchemaError do
      Avro::Schema.parse <<-SCHEMA
        {"type": "record", "name": "my.name.space.Record", "fields": [
          {"name": "reference", "type": "MissingType"}
        ]}
      SCHEMA
    end

    assert_equal '"MissingType" is not a schema we know about.', error.message
  end

  def test_to_avro_handles_falsey_defaults
    schema = Avro::Schema.parse <<-SCHEMA
      {"type": "record", "name": "Record", "namespace": "my.name.space",
        "fields": [
          {"name": "is_usable", "type": "boolean", "default": false}
        ]
      }
    SCHEMA

    assert_equal schema.to_avro, {
      'type' => 'record', 'name' => 'Record', 'namespace' => 'my.name.space',
      'fields' => [
        {'name' => 'is_usable', 'type' => 'boolean', 'default' => false}
      ]
    }
  end

  def test_record_field_doc_attribute
    field_schema_json = Avro::Schema.parse <<-SCHEMA
      {
        "type": "record",
        "name": "Record",
        "namespace": "my.name.space",
        "fields": [
          {
            "name": "name",
            "type": "boolean",
            "doc": "documentation"
          }
        ]
      }
    SCHEMA

    field_schema_hash =
      {
        'type' => 'record',
        'name' => 'Record',
        'namespace' => 'my.name.space',
        'fields' => [
          {
            'name' => 'name',
            'type' => 'boolean',
            'doc' => 'documentation'
          }
        ]
      }

    assert_equal field_schema_hash, field_schema_json.to_avro
  end

  def test_record_doc_attribute
    record_schema_json = Avro::Schema.parse <<-SCHEMA
      {
        "type": "record",
        "name": "Record",
        "namespace": "my.name.space",
        "doc": "documentation",
        "fields": [
          {
            "name": "name",
            "type": "boolean"
          }
        ]
      }
    SCHEMA

    record_schema_hash =
      {
        'type' => 'record',
        'name' => 'Record',
        'namespace' => 'my.name.space',
        'doc' => 'documentation',
        'fields' => [
          {
            'name' => 'name',
            'type' => 'boolean'
          }
        ]
      }

    assert_equal record_schema_hash, record_schema_json.to_avro
  end

  def test_enum_doc_attribute
    enum_schema_json = Avro::Schema.parse <<-SCHEMA
      {
        "type": "enum",
        "name": "Enum",
        "namespace": "my.name.space",
        "doc": "documentation",
        "symbols" : [
          "SPADES",
          "HEARTS",
          "DIAMONDS",
          "CLUBS"
        ]
      }
    SCHEMA

    enum_schema_hash =
      {
        'type' => 'enum',
        'name' => 'Enum',
        'namespace' => 'my.name.space',
        'doc' => 'documentation',
        'symbols' => [
          'SPADES',
          'HEARTS',
          'DIAMONDS',
          'CLUBS'
        ]
      }
    assert_equal enum_schema_hash, enum_schema_json.to_avro
  end

def test_empty_record
    schema = Avro::Schema.parse('{"type":"record", "name":"Empty"}')
    assert_empty(schema.fields)
  end

  def test_empty_union
    schema = Avro::Schema.parse('[]')
    assert_equal(schema.to_s, '[]')
  end

  def test_read
    schema = Avro::Schema.parse('"string"')
    writer_schema = Avro::Schema.parse('"int"')
    assert_false(schema.read?(writer_schema))
    assert_true(schema.read?(schema))
  end

  def test_be_read
    schema = Avro::Schema.parse('"string"')
    writer_schema = Avro::Schema.parse('"int"')
    assert_false(schema.be_read?(writer_schema))
    assert_true(schema.be_read?(schema))
  end

  def test_mutual_read
    schema = Avro::Schema.parse('"string"')
    writer_schema = Avro::Schema.parse('"int"')
    default1 = Avro::Schema.parse('{"type":"record", "name":"Default", "fields":[{"name":"i", "type":"int", "default": 1}]}')
    default2 = Avro::Schema.parse('{"type":"record", "name":"Default", "fields":[{"name:":"s", "type":"string", "default": ""}]}')
    assert_false(schema.mutual_read?(writer_schema))
    assert_true(schema.mutual_read?(schema))
    assert_true(default1.mutual_read?(default2))
  end
end
