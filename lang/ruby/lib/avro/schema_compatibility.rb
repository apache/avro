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
  module SchemaCompatibility
    # Perform a full, recursive check that a datum written using the writers_schema
    # can be read using the readers_schema.
    def self.can_read?(writers_schema, readers_schema)
      Checker.new.can_read?(writers_schema, readers_schema)
    end

    # Perform a full, recursive check that a datum written using either the
    # writers_schema or the readers_schema can be read using the other schema.
    def self.mutual_read?(writers_schema, readers_schema)
      Checker.new.mutual_read?(writers_schema, readers_schema)
    end

    # Perform a basic check that a datum written with the writers_schema could
    # be read using the readers_schema. This check only includes matching the types,
    # including schema promotion, and matching the full name for named types.
    # Aliases for named types are not supported here, and the ruby implementation
    # of Avro in general does not include support for aliases.
    def self.match_schemas(writers_schema, readers_schema)
      w_type = writers_schema.type_sym
      r_type = readers_schema.type_sym

      # This conditional is begging for some OO love.
      if w_type == :union || r_type == :union
        return true
      end

      if w_type == r_type
        return true if Schema::PRIMITIVE_TYPES_SYM.include?(r_type)

        case r_type
        when :record
          return writers_schema.fullname == readers_schema.fullname
        when :error
          return writers_schema.fullname == readers_schema.fullname
        when :request
          return true
        when :fixed
          return writers_schema.fullname == readers_schema.fullname &&
            writers_schema.size == readers_schema.size
        when :enum
          return writers_schema.fullname == readers_schema.fullname
        when :map
          return match_schemas(writers_schema.values, readers_schema.values)
        when :array
          return match_schemas(writers_schema.items, readers_schema.items)
        end
      end

      # Handle schema promotion
      if w_type == :int && [:long, :float, :double].include?(r_type)
        return true
      elsif w_type == :long && [:float, :double].include?(r_type)
        return true
      elsif w_type == :float && r_type == :double
        return true
      elsif w_type == :string && r_type == :bytes
        return true
      elsif w_type == :bytes && r_type == :string
        return true
      end

      return false
    end

    class Checker
      SIMPLE_CHECKS = Schema::PRIMITIVE_TYPES_SYM.dup.add(:fixed).freeze

      attr_reader :recursion_set
      private :recursion_set

      def initialize
        @recursion_set = Set.new
      end

      def can_read?(writers_schema, readers_schema)
        full_match_schemas(writers_schema, readers_schema)
      end

      def mutual_read?(writers_schema, readers_schema)
        can_read?(writers_schema, readers_schema) && can_read?(readers_schema, writers_schema)
      end

      private

      def full_match_schemas(writers_schema, readers_schema)
        return true if recursion_in_progress?(writers_schema, readers_schema)

        return false unless Avro::SchemaCompatibility.match_schemas(writers_schema, readers_schema)

        if writers_schema.type_sym != :union && SIMPLE_CHECKS.include?(readers_schema.type_sym)
          return true
        end

        case readers_schema.type_sym
        when :record
          match_record_schemas(writers_schema, readers_schema)
        when :map
          full_match_schemas(writers_schema.values, readers_schema.values)
        when :array
          full_match_schemas(writers_schema.items, readers_schema.items)
        when :union
          match_union_schemas(writers_schema, readers_schema)
        when :enum
          # reader's symbols must contain all writer's symbols
          (writers_schema.symbols - readers_schema.symbols).empty?
        else
          if writers_schema.type_sym == :union && writers_schema.schemas.size == 1
            full_match_schemas(writers_schema.schemas.first, readers_schema)
          else
            false
          end
        end
      end

      def match_union_schemas(writers_schema, readers_schema)
        raise 'readers_schema must be a union' unless readers_schema.type_sym == :union

        case writers_schema.type_sym
        when :union
          writers_schema.schemas.all? { |writer_type| full_match_schemas(writer_type, readers_schema) }
        else
          readers_schema.schemas.any? { |reader_type| full_match_schemas(writers_schema, reader_type) }
        end
      end

      def match_record_schemas(writers_schema, readers_schema)
        writer_fields_hash = writers_schema.fields_hash
        readers_schema.fields.each do |field|
          if writer_fields_hash.key?(field.name)
            return false unless full_match_schemas(writer_fields_hash[field.name].type, field.type)
          else
            return false unless field.default?
          end
        end

        return true
      end

      def recursion_in_progress?(writers_schema, readers_schema)
        key = [writers_schema.object_id, readers_schema.object_id]

        if recursion_set.include?(key)
          true
        else
          recursion_set.add(key)
          false
        end
      end
    end
  end
end
