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
  class Schema
    # FIXME turn these into symbols to prevent some gc pressure
    PRIMITIVE_TYPES = Set.new(%w[null boolean string bytes int long float double])
    NAMED_TYPES =     Set.new(%w[fixed enum record error])

    VALID_TYPES = PRIMITIVE_TYPES + NAMED_TYPES + Set.new(%w[array map union request])

    INT_MIN_VALUE = -(1 << 31)
    INT_MAX_VALUE = (1 << 31) - 1
    LONG_MIN_VALUE = -(1 << 63)
    LONG_MAX_VALUE = (1 << 63) - 1

    def self.parse(json_string)
      real_parse(Yajl.load(json_string), {})
    end

    # Build Avro Schema from data parsed out of JSON string.
    def self.real_parse(json_obj, names=nil)
      if json_obj.is_a? Hash
        type = json_obj['type']
        if PRIMITIVE_TYPES.include?(type)
          return PrimitiveSchema.new(type)
        elsif NAMED_TYPES.include? type
          name = json_obj['name']
          namespace = json_obj['namespace']
          case type
          when 'fixed'
            size = json_obj['size']
            return FixedSchema.new(name, namespace, size, names)
          when 'enum'
            symbols = json_obj['symbols']
            return EnumSchema.new(name, namespace, symbols, names)
          when 'record', 'error'
            fields = json_obj['fields']
            return RecordSchema.new(name, namespace, fields, names, type)
          else
            raise SchemaParseError.new("Unknown Named Type: #{type}")
          end
        elsif VALID_TYPES.include?(type)
          case type
          when 'array'
            return ArraySchema.new(json_obj['items'], names)
          when 'map'
            return MapSchema.new(json_obj['values'], names)
          else
            raise SchemaParseError.new("Unknown Valid Type: #{type}")
          end
        elsif type.nil?
          raise SchemaParseError.new("No \"type\" property: #{json_obj}")
        else
          raise SchemaParseError.new("Undefined type: #{type}")
        end
      elsif json_obj.is_a? Array
        # JSON array (union)
        return UnionSchema.new(json_obj, names)
      elsif PRIMITIVE_TYPES.include? json_obj
        return PrimitiveSchema.new(json_obj)
      else
        msg = "Could not make an Avro Schema object from #{json_obj}"
        raise SchemaParseError.new(msg)
      end
    end

    # Determine if a ruby datum is an instance of a schema
    def self.validate(expected_schema, datum)
      case expected_schema.type
      when 'null'
        datum.nil?
      when 'boolean'
        datum == true || datum == false
      when 'string', 'bytes'
        datum.is_a? String
      when 'int'
        (datum.is_a?(Fixnum) || datum.is_a?(Bignum)) &&
            (INT_MIN_VALUE <= datum) && (datum <= INT_MAX_VALUE)
      when 'long'
        (datum.is_a?(Fixnum) || datum.is_a?(Bignum)) &&
            (LONG_MIN_VALUE <= datum) && (datum <= LONG_MAX_VALUE)
      when 'float', 'double'
        datum.is_a?(Float) || datum.is_a?(Fixnum) || datum.is_a?(Bignum)
      when 'fixed'
        datum.is_a?(String) && datum.size == expected_schema.size
      when 'enum'
        expected_schema.symbols.include? datum
      when 'array'
        datum.is_a?(Array) &&
          datum.all?{|d| validate(expected_schema.items, d) }
      when 'map':
          datum.keys.all?{|k| k.is_a? String } &&
          datum.values.all?{|v| validate(expected_schema.values, v) }
      when 'union'
        expected_schema.schemas.any?{|s| validate(s, datum) }
      when 'record', 'error', 'request'
        datum.is_a?(Hash) &&
          expected_schema.fields.all?{|f| validate(f.type, datum[f.name]) }
      else
        raise "you suck #{expected_schema.inspect} is not allowed."
      end
    end

    def initialize(type)
      @type = type
    end

    def type; @type; end

    def ==(other, seen=nil)
      other.is_a?(Schema) && @type == other.type
    end

    def hash(seen=nil)
      @type.hash
    end

    def to_hash
      {'type' => @type}
    end

    def to_s
      Yajl.dump to_hash
    end

    class NamedSchema < Schema
      attr_reader :name, :namespace
      def initialize(type, name, namespace=nil, names=nil)
        super(type)
        @name, @namespace = Name.extract_namespace(name, namespace)
        names = Name.add_name(names, self)
      end

      def to_hash
        props = {'name' => @name}
        props.merge!('namespace' => @namespace) if @namespace
        super.merge props
      end

      def fullname
        Name.make_fullname(@name, @namespace)
      end
    end

    class RecordSchema < NamedSchema
      attr_reader :fields

      def self.make_field_objects(field_data, names)
        field_objects, field_names = [], Set.new
        field_data.each_with_index do |field, i|
          if field.respond_to?(:[]) # TODO(jmhodges) wtffffff
            type = field['type']
            name = field['name']
            default = field['default']
            order = field['order']
            new_field = Field.new(type, name, default, order, names)
            # make sure field name has not been used yet
            if field_names.include?(new_field.name)
              raise SchemaParseError, "Field name #{new_field.name.inspect} is already in use"
            end
            field_names << new_field.name
          else
            raise SchemaParseError, "Not a valid field: #{field}"
          end
          field_objects << new_field
        end
        field_objects
      end

      def initialize(name, namespace, fields, names=nil, schema_type='record')
        if schema_type == 'request'
          @type = schema_type
        else
          super(schema_type, name, namespace, names)
        end
        @fields = RecordSchema.make_field_objects(fields, names)
      end

      def fields_hash
        fields.inject({}){|hsh, field| hsh[field.name] = field; hsh }
      end

      def to_hash
        hsh = super.merge('fields' => @fields.map {|f|Yajl.load(f.to_s)} )
        if type == 'request'
          hsh['fields']
        else
          hsh
        end
      end
    end

    class ArraySchema < Schema
      attr_reader :items, :items_schema_from_names
      def initialize(items, names=nil)
        @items_schema_from_names = false

        super('array')

        if items.is_a?(String) && names.has_key?(items)
          @items = names[items]
          @items_schema_from_names = true
        else
          begin
            @items = Schema.real_parse(items, names)
          rescue => e
            msg = "Items schema not a valid Avro schema" + e.to_s
            raise SchemaParseError, msg
          end
        end
      end

      def to_hash
        name_or_json = if items_schema_from_names
                         items.fullname
                       else
                         Yajl.load(items.to_s)
                       end
        super.merge('items' => name_or_json)
      end
    end

    class MapSchema < Schema
      attr_reader :values, :values_schema_from_names

      def initialize(values, names=nil)
        @values_schema_from_names = false
        super('map')
        if values.is_a?(String) && names.has_key?(values)
          values_schema = names[values]
          @values_schema_from_names = true
        else
          begin
            values_schema = Schema.real_parse(values, names)
          rescue => e
            raise SchemaParseError.new('Values schema not a valid Avro schema.' + e.to_s)
          end
        end
        @values = values_schema
      end

      def to_hash
        to_dump = super
        if values_schema_from_names
          to_dump['values'] = values
        else
          to_dump['values'] = Yajl.load(values.to_s)
        end
        to_dump
      end
    end

    class UnionSchema < Schema
      attr_reader :schemas, :schema_from_names_indices
      def initialize(schemas, names=nil)
        super('union')

        schema_objects = []
        @schema_from_names_indices = []
        schemas.each_with_index do |schema, i|
          from_names = false
          if schema.is_a?(String) && names.has_key?(schema)
            new_schema = names[schema]
            from_names = true
          else
            begin
              new_schema = Schema.real_parse(schema, names)
            rescue
              raise SchemaParseError, 'Union item must be a valid Avro schema'
            end
          end

          ns_type = new_schema.type
          if VALID_TYPES.include?(ns_type) &&
              !NAMED_TYPES.include?(ns_type) &&
              schema_objects.map(&:type).include?(ns_type)
            raise SchemaParseError, "#{ns_type} is already in Union"
          elsif ns_type == 'union'
            raise SchemaParseError, "Unions cannot contain other unions"
          else
            schema_objects << new_schema
            @schema_from_names_indices << i if from_names
          end
          @schemas = schema_objects
        end
      end

      def to_s
        # FIXME(jmhodges) this from_name pattern is really weird and
        # seems code-smelly.
        to_dump = []
        schemas.each_with_index do |schema, i|
          if schema_from_names_indices.include?(i)
            to_dump << schema.fullname
          else
            to_dump << Yajl.load(schema.to_s)
          end
        end
        Yajl.dump(to_dump)
      end
    end

    class EnumSchema < NamedSchema
      attr_reader :symbols
      def initialize(name, space, symbols, names=nil)
        if symbols.uniq.length < symbols.length
          fail_msg = 'Duplicate symbol: %s' % symbols
          raise Avro::SchemaParseError, fail_msg
        end
        super('enum', name, space, names)
        @symbols = symbols
      end

      def to_hash
        super.merge('symbols' => symbols)
      end
    end

    # Valid primitive types are in PRIMITIVE_TYPES.
    class PrimitiveSchema < Schema
      def initialize(type)
        unless PRIMITIVE_TYPES.include? type
          raise AvroError.new("#{type} is not a valid primitive type.")
        end

        super(type)
      end

      def to_s
        to_hash.size == 1 ? type.inspect : Yajl.dump(to_hash)
      end
    end

    class FixedSchema < NamedSchema
      attr_reader :size
      def initialize(name, space, size, names=nil)
        # Ensure valid cto args
        unless size.is_a?(Fixnum) || size.is_a?(Bignum)
          raise AvroError, 'Fixed Schema requires a valid integer for size property.'
        end
        super('fixed', name, space, names)
        @size = size
      end

      def to_hash
        super.merge('size' => @size)
      end
    end

    class Field
      attr_reader :type, :name, :default, :order, :type_from_names
      def initialize(type, name, default=nil, order=nil, names=nil)
        @type_from_names = false
        if type.is_a?(String) && names && names.has_key?(type)
          type_schema = names[type]
          @type_from_names = true
        else
          type_schema = Schema.real_parse(type, names)
        end
        @type = type_schema
        @name = name
        @default = default
        @order = order
      end

      def to_hash
        sigh_type = type_from_names ? type.fullname : Yajl.load(type.to_s)
        hsh = {
          'name' => name,
          'type' => sigh_type
        }
        hsh['default'] = default if default
        hsh['order'] = order if order
        hsh
      end

      def to_s
        Yajl.dump(to_hash)
      end
    end
  end

  class SchemaParseError < AvroError; end

  module Name
    def self.extract_namespace(name, namespace)
      parts = name.split('.')
      if parts.size > 1
        namespace, name = parts[0..-2].join('.'), parts.last
      end
      return name, namespace
    end

    # Add a new schema object to the names dictionary (in place).
    def self.add_name(names, new_schema)
      new_fullname = new_schema.fullname
      if Avro::Schema::VALID_TYPES.include?(new_fullname)
        raise SchemaParseError, "#{new_fullname} is a reserved type name."
      elsif names.nil?
        names = {}
      elsif names.has_key?(new_fullname)
        raise SchemaParseError, "The name \"#{new_fullname}\" is already in use."
      end

      names[new_fullname] = new_schema
      names
    end

    def self.make_fullname(name, namespace)
      if !name.include?('.') && !namespace.nil?
        namespace + '.' + name
      else
        name
      end
    end
  end
end
