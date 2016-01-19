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

module Avro
  class SchemaValidator
    ROOT_IDENTIFIER = '.'.freeze
    PATH_SEPARATOR = '.'.freeze

    class Result
      attr_reader :errors

      def initialize
        @errors = []
      end

      def add_error(path, message)
        @errors << "at #{path} #{message}"
      end

      def failure?
        @errors.any?
      end

      def to_s
        errors.join("\n")
      end
    end

    class ValidationError < StandardError
      attr_reader :result

      def initialize(result = Result.new)
        @result = result
        super
      end

      def to_s
        result.to_s
      end
    end

    MismatchError = Class.new(ValidationError)

    class << self
      def validate!(expected_schema, datum)
        result = Result.new
        validate_recursive(expected_schema, datum, ROOT_IDENTIFIER, result)
        fail ValidationError, result if result.failure?
        result
      end

      private

      def validate_recursive(expected_schema, datum, path, result)
        case expected_schema.type_sym
        when :null
          fail MismatchError unless datum.nil?
        when :boolean
          fail MismatchError unless [true, false].include?(datum)
        when :string, :bytes
          fail MismatchError unless datum.is_a?(String)
        when :int
          fail MismatchError unless datum.is_a?(Fixnum) || datum.is_a?(Bignum)
          range = (Schema::INT_MIN_VALUE..Schema::INT_MAX_VALUE)
          result.add_error(path, "out of bound value #{datum}") unless range.cover?(datum)
        when :long
          fail MismatchError unless datum.is_a?(Fixnum) || datum.is_a?(Bignum)
          range = (Schema::LONG_MIN_VALUE..Schema::LONG_MAX_VALUE)
          result.add_error(path, "out of bound value #{datum}") unless range.cover?(datum)
        when :float, :double
          fail MismatchError unless [Float, Fixnum, Bignum].any?(&datum.method(:is_a?))
        when :fixed
          if datum.is_a? String
            message = "expected fixed with size #{expected_schema.size}, got \"#{datum}\" with size #{datum.size}"
            result.add_error(path, message) unless datum.bytesize == expected_schema.size
          else
            result.add_error(path, "expected fixed with size #{expected_schema.size}, got #{actual_value_message(datum)}")
          end
        when :enum
          message = "expected enum with values #{expected_schema.symbols}, got #{actual_value_message(datum)}"
          result.add_error(path, message) unless expected_schema.symbols.include?(datum)
        when :array
          validate_array(expected_schema, datum, path, result)
        when :map
          validate_map(expected_schema, datum, path, result)
        when :union
          validate_union(expected_schema, datum, path, result)
        when :record, :error, :request
          fail MismatchError unless datum.is_a?(Hash)
          expected_schema.fields.each do |field|
            deeper_path = deeper_path_for_hash(field.name, path)
            validate_recursive(field.type, datum[field.name], deeper_path, result)
          end
        else
          fail "Unexpected schema type #{expected_schema.type_sym} #{expected_schema.inspect}"
        end
      rescue MismatchError
        result.add_error(path, "expected type #{expected_schema.type_sym}, got #{actual_value_message(datum)}")
      end

      def validate_array(expected_schema, datum, path, result)
        fail MismatchError unless datum.is_a?(Array)
        datum.each_with_index do |d, i|
          validate_recursive(expected_schema.items, d, path + "[#{i}]", result)
        end
      end

      def validate_map(expected_schema, datum, path, result)
        datum.keys.each do |k|
          result.add_error(path, "unexpected key type '#{ruby_to_avro_type(k.class)}' in map") unless k.is_a?(String)
        end
        datum.each do |k, v|
          deeper_path = deeper_path_for_hash(k, path)
          validate_recursive(expected_schema.values, v, deeper_path, result)
        end
      end

      def validate_union(expected_schema, datum, path, result)
        if expected_schema.schemas.size == 1
          validate_recursive(expected_schema.schemas.first, datum, path, result)
        else
          r = Result.new
          expected_schema.schemas.each do |schema|
            validate_recursive(schema, datum, path, r)
          end
          if expected_schema.schemas.size == r.errors.size
            types = expected_schema.schemas.map { |s| "'#{s.type_sym}'" }.join(', ')
            result.add_error(path, "expected union of [#{types}], got #{actual_value_message(datum)}")
          end
        end
      end

      def deeper_path_for_hash(sub_key, path)
        "#{path}#{PATH_SEPARATOR}#{sub_key}".squeeze(PATH_SEPARATOR)
      end

      private

      def actual_value_message(value)
        avro_type = ruby_to_avro_type(value.class)
        if value.nil?
          avro_type
        else
          "#{avro_type} with value #{value.inspect}"
        end
      end

      def ruby_to_avro_type(ruby_class)
        {
          NilClass => 'null',
          String => 'string',
          Fixnum => 'int',
          Bignum => 'long',
          Float => 'float',
          Hash => 'record'
        }.fetch(ruby_class, ruby_class)
      end
    end
  end
end
