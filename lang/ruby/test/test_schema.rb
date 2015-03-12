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

    assert_equal schema.name, 'OuterRecord'
    assert_equal schema.fullname, 'OuterRecord'
    assert_nil schema.namespace

    schema.fields.each do |field|
      assert_equal field.type.name, 'InnerRecord'
      assert_equal field.type.fullname, 'InnerRecord'
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

    assert_equal schema.name, 'OuterRecord'
    assert_equal schema.fullname, 'my.name.space.OuterRecord'
    assert_equal schema.namespace, 'my.name.space'
    schema.fields.each do |field|
      assert_equal field.type.name, 'InnerRecord'
      assert_equal field.type.fullname, 'my.name.space.InnerRecord'
      assert_equal field.type.namespace, 'my.name.space'
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

    assert_equal schema.name, 'OuterRecord'
    assert_equal schema.fullname, 'my.name.space.OuterRecord'
    assert_equal schema.namespace, 'my.name.space'
    schema.fields.each do |field|
      assert_equal field.type.name, 'InnerEnum'
      assert_equal field.type.fullname, 'my.name.space.InnerEnum'
      assert_equal field.type.namespace, 'my.name.space'
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

    assert_equal schema.name, 'OuterRecord'
    assert_equal schema.fullname, 'outer.OuterRecord'
    assert_equal schema.namespace, 'outer'
    middle = schema.fields.first.type
    assert_equal middle.name, 'MiddleRecord'
    assert_equal middle.fullname, 'middle.MiddleRecord'
    assert_equal middle.namespace, 'middle'
    inner = middle.fields.first.type
    assert_equal inner.name, 'InnerRecord'
    assert_equal inner.fullname, 'middle.InnerRecord'
    assert_equal inner.namespace, 'middle'
    assert_equal inner.fields.first.type, middle
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

    assert_equal schema.to_avro, {
      'type' => 'record', 'name' => 'OuterRecord', 'namespace' => 'my.name.space',
      'fields' => [
        {'name' => 'definition', 'type' => {
          'type' => 'fixed', 'name' => 'InnerFixed', 'namespace' => 'my.name.space',
          'size' => 16
        }},
        {'name' => 'reference', 'type' => 'my.name.space.InnerFixed'}
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
end
