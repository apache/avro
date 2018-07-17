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

class TestDefaultValidation < Test::Unit::TestCase
  def hash_to_schema(hash)
    Avro::Schema.parse(hash.to_json)
  end

  def test_validate_defaults
    exception = assert_raise(Avro::SchemaParseError) do
      hash_to_schema(
        type: 'record',
        name: 'fruits',
        fields: [
          {
            name: 'veggies',
            type: 'string',
            default: nil
          }
        ]
      )
    end
    assert_equal('Error validating default for veggies: at . expected type string, got null',
                 exception.to_s)
  end

  def test_validate_record_valid_default
    assert_nothing_raised(Avro::SchemaParseError) do
      hash_to_schema(
        type: 'record',
        name: 'with_subrecord',
        fields: [
          {
            name: 'sub',
            type: {
              name: 'subrecord',
              type: 'record',
              fields: [
                { type: 'string', name: 'x' }
              ]
            },
            default: {
              x: "y"
            }
          }
        ]
      )
    end
  end

  def test_validate_record_invalid_default
    exception = assert_raise(Avro::SchemaParseError) do
      hash_to_schema(
        type: 'record',
        name: 'with_subrecord',
        fields: [
          {
            name: 'sub',
            type: {
              name: 'subrecord',
              type: 'record',
              fields: [
                { type: 'string', name: 'x' }
              ]
            },
            default: {
              a: 1
            }
          }
        ]
      )
    end
    assert_equal('Error validating default for sub: at .x expected type string, got null',
                 exception.to_s)
  end

  def test_validate_union_defaults
    exception = assert_raise(Avro::SchemaParseError) do
      hash_to_schema(
        type: 'record',
        name: 'fruits',
        fields: [
          {
            name: 'veggies',
            type: %w(string null),
            default: 5
          }
        ]
      )
    end
    assert_equal('Error validating default for veggies: at . expected type string, got int with value 5',
                 exception.to_s)
  end

  def test_validate_union_default_first_type
    exception = assert_raise(Avro::SchemaParseError) do
      hash_to_schema(
        type: 'record',
        name: 'fruits',
        fields: [
          {
            name: 'veggies',
            type: %w(null string),
            default: 'apple'
          }
        ]
      )
    end
    assert_equal('Error validating default for veggies: at . expected type null, got string with value "apple"',
                 exception.to_s)
  end
end
