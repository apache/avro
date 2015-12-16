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
    # Sets of strings, for backwards compatibility. See below for sets of symbols,
    # for better performance.
    PRIMITIVE_TYPES = Set.new(%w[null boolean string bytes int long float double])
    NAMED_TYPES =     Set.new(%w[fixed enum record error])

    VALID_TYPES = PRIMITIVE_TYPES + NAMED_TYPES + Set.new(%w[array map union request])

    PRIMITIVE_TYPES_SYM = Set.new(PRIMITIVE_TYPES.map(&:to_sym))
    NAMED_TYPES_SYM     = Set.new(NAMED_TYPES.map(&:to_sym))
    VALID_TYPES_SYM     = Set.new(VALID_TYPES.map(&:to_sym))

    INT_MIN_VALUE = -(1 << 31)
    INT_MAX_VALUE = (1 << 31) - 1
    LONG_MIN_VALUE = -(1 << 63)
    LONG_MAX_VALUE = (1 << 63) - 1

    def self.parse(json_string)
      real_parse(MultiJson.load(json_string), {})
    end

    # Build Avro Schema from data parsed out of JSON string.
    def self.real_parse(json_obj, names=nil, default_namespace=nil)
      if json_obj.is_a? Hash
        type = json_obj['type']
        raise SchemaParseError, %Q(No "type" property: #{json_obj}) if type.nil?

        # Check that the type is valid before calling #to_sym, since symbols are never garbage
        # collected (important to avoid DoS if we're accepting schemas from untrusted clients)
        unless VALID_TYPES.include?(type)
          raise SchemaParseError, "Unknown type: #{type}"
        end

        type_sym = type.to_sym
        if PRIMITIVE_TYPES_SYM.include?(type_sym)
          return PrimitiveSchema.new(type_sym)

        elsif NAMED_TYPES_SYM.include? type_sym
          name = json_obj['name']
          namespace = json_obj.include?('namespace') ? json_obj['namespace'] : default_namespace
          case type_sym
          when :fixed
            size = json_obj['size']
            return FixedSchema.new(name, namespace, size, names)
          when :enum
            symbols = json_obj['symbols']
            return EnumSchema.new(name, namespace, symbols, names)
          when :record, :error
            fields = json_obj['fields']
            return RecordSchema.new(name, namespace, fields, names, type_sym)
          else
            raise SchemaParseError.new("Unknown named type: #{type}")
          end

        else
          case type_sym
          when :array
            return ArraySchema.new(json_obj['items'], names, default_namespace)
          when :map
            return MapSchema.new(json_obj['values'], names, default_namespace)
          else
            raise SchemaParseError.new("Unknown Valid Type: #{type}")
          end
        end

      elsif json_obj.is_a? Array
        # JSON array (union)
        return UnionSchema.new(json_obj, names, default_namespace)
      elsif PRIMITIVE_TYPES.include? json_obj
        return PrimitiveSchema.new(json_obj)
      else
        raise UnknownSchemaError.new(json_obj)
      end
    end

    # Determine if a ruby datum is an instance of a schema
    def self.validate(expected_schema, datum)
      case expected_schema.type_sym
      when :null
        datum.nil?
      when :boolean
        datum == true || datum == false
      when :string, :bytes
        datum.is_a? String
      when :int
        (datum.is_a?(Fixnum) || datum.is_a?(Bignum)) &&
            (INT_MIN_VALUE <= datum) && (datum <= INT_MAX_VALUE)
      when :long
        (datum.is_a?(Fixnum) || datum.is_a?(Bignum)) &&
            (LONG_MIN_VALUE <= datum) && (datum <= LONG_MAX_VALUE)
      when :float, :double
        datum.is_a?(Float) || datum.is_a?(Fixnum) || datum.is_a?(Bignum)
      when :fixed
        datum.is_a?(String) && datum.size == expected_schema.size
      when :enum
        expected_schema.symbols.include? datum
      when :array
        datum.is_a?(Array) &&
          datum.all?{|d| validate(expected_schema.items, d) }
      when :map
          datum.keys.all?{|k| k.is_a? String } &&
          datum.values.all?{|v| validate(expected_schema.values, v) }
      when :union
        expected_schema.schemas.any?{|s| validate(s, datum) }
      when :record, :error, :request
        datum.is_a?(Hash) &&
          expected_schema.fields.all?{|f| validate(f.type, datum[f.name]) }
      else
        raise "you suck #{expected_schema.inspect} is not allowed."
      end
    end

    def initialize(type)
      @type_sym = type.is_a?(Symbol) ? type : type.to_sym
    end

    attr_reader :type_sym

    # Returns the type as a string (rather than a symbol), for backwards compatibility.
    # Deprecated in favor of {#type_sym}.
    def type; @type_sym.to_s; end

    # Returns the MD5 fingerprint of the schema as an Integer.
    def md5_fingerprint
      parsing_form = SchemaNormalization.to_parsing_form(self)
      Digest::MD5.hexdigest(parsing_form).to_i(16)
    end

    # Returns the SHA-256 fingerprint of the schema as an Integer.
    def sha256_fingerprint
      parsing_form = SchemaNormalization.to_parsing_form(self)
      Digest::SHA256.hexdigest(parsing_form).to_i(16)
    end

    def ==(other, seen=nil)
      other.is_a?(Schema) && type_sym == other.type_sym
    end

    def hash(seen=nil)
      type_sym.hash
    end

    def subparse(json_obj, names=nil, namespace=nil)
      if json_obj.is_a?(String) && names
        fullname = Name.make_fullname(json_obj, namespace)
        return names[fullname] if names.include?(fullname)
      end

      begin
        Schema.real_parse(json_obj, names, namespace)
      rescue => e
        raise e if e.is_a? SchemaParseError
        raise SchemaParseError, "Sub-schema for #{self.class.name} not a valid Avro schema. Bad schema: #{json_obj}"
      end
    end

    def to_avro(names=nil)
      {'type' => type}
    end

    def to_s
      MultiJson.dump to_avro
    end

    class NamedSchema < Schema
      attr_reader :name, :namespace
      def initialize(type, name, namespace=nil, names=nil)
        super(type)
        @name, @namespace = Name.extract_namespace(name, namespace)
        names = Name.add_name(names, self)
      end

      def to_avro(names=Set.new)
        if @name
          return fullname if names.include?(fullname)
          names << fullname
        end
        props = {'name' => @name}
        props.merge!('namespace' => @namespace) if @namespace
        super.merge props
      end

      def fullname
        @fullname ||= Name.make_fullname(@name, @namespace)
      end
    end

    class RecordSchema < NamedSchema
      attr_reader :fields

      def self.make_field_objects(field_data, names, namespace=nil)
        field_objects, field_names = [], Set.new
        field_data.each_with_index do |field, i|
          if field.respond_to?(:[]) # TODO(jmhodges) wtffffff
            type = field['type']
            name = field['name']
            default = field['default']
            order = field['order']
            new_field = Field.new(type, name, default, order, names, namespace)
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

      def initialize(name, namespace, fields, names=nil, schema_type=:record)
        if schema_type == :request || schema_type == 'request'
          @type_sym = schema_type.to_sym
          @namespace = namespace
        else
          super(schema_type, name, namespace, names)
        end
        @fields = RecordSchema.make_field_objects(fields, names, self.namespace)
      end

      def fields_hash
        @fields_hash ||= fields.inject({}){|hsh, field| hsh[field.name] = field; hsh }
      end

      def to_avro(names=Set.new)
        hsh = super
        return hsh unless hsh.is_a?(Hash)
        hsh['fields'] = @fields.map {|f| f.to_avro(names) }
        if type_sym == :request
          hsh['fields']
        else
          hsh
        end
      end
    end

    class ArraySchema < Schema
      attr_reader :items

      def initialize(items, names=nil, default_namespace=nil)
        super(:array)
        @items = subparse(items, names, default_namespace)
      end

      def to_avro(names=Set.new)
        super.merge('items' => items.to_avro(names))
      end
    end

    class MapSchema < Schema
      attr_reader :values

      def initialize(values, names=nil, default_namespace=nil)
        super(:map)
        @values = subparse(values, names, default_namespace)
      end

      def to_avro(names=Set.new)
        super.merge('values' => values.to_avro(names))
      end
    end

    class UnionSchema < Schema
      attr_reader :schemas

      def initialize(schemas, names=nil, default_namespace=nil)
        super(:union)

        schema_objects = []
        schemas.each_with_index do |schema, i|
          new_schema = subparse(schema, names, default_namespace)
          ns_type = new_schema.type_sym

          if VALID_TYPES_SYM.include?(ns_type) &&
              !NAMED_TYPES_SYM.include?(ns_type) &&
              schema_objects.any?{|o| o.type_sym == ns_type }
            raise SchemaParseError, "#{ns_type} is already in Union"
          elsif ns_type == :union
            raise SchemaParseError, "Unions cannot contain other unions"
          else
            schema_objects << new_schema
          end
          @schemas = schema_objects
        end
      end

      def to_avro(names=Set.new)
        schemas.map {|schema| schema.to_avro(names) }
      end
    end

    class EnumSchema < NamedSchema
      attr_reader :symbols
      def initialize(name, space, symbols, names=nil)
        if symbols.uniq.length < symbols.length
          fail_msg = 'Duplicate symbol: %s' % symbols
          raise Avro::SchemaParseError, fail_msg
        end
        super(:enum, name, space, names)
        @symbols = symbols
      end

      def to_avro(names=Set.new)
        avro = super
        avro.is_a?(Hash) ? avro.merge('symbols' => symbols) : avro
      end
    end

    # Valid primitive types are in PRIMITIVE_TYPES.
    class PrimitiveSchema < Schema
      def initialize(type)
        if PRIMITIVE_TYPES_SYM.include?(type)
          super(type)
        elsif PRIMITIVE_TYPES.include?(type)
          super(type.to_sym)
        else
          raise AvroError.new("#{type} is not a valid primitive type.")
        end
      end

      def to_avro(names=nil)
        hsh = super
        hsh.size == 1 ? type : hsh
      end
    end

    class FixedSchema < NamedSchema
      attr_reader :size
      def initialize(name, space, size, names=nil)
        # Ensure valid cto args
        unless size.is_a?(Fixnum) || size.is_a?(Bignum)
          raise AvroError, 'Fixed Schema requires a valid integer for size property.'
        end
        super(:fixed, name, space, names)
        @size = size
      end

      def to_avro(names=Set.new)
        avro = super
        avro.is_a?(Hash) ? avro.merge('size' => size) : avro
      end
    end

    class Field < Schema
      attr_reader :type, :name, :default, :order

      def initialize(type, name, default=nil, order=nil, names=nil, namespace=nil)
        @type = subparse(type, names, namespace)
        @name = name
        @default = default
        @order = order
      end

      def to_avro(names=Set.new)
        {'name' => name, 'type' => type.to_avro(names)}.tap do |avro|
          avro['default'] = default if default
          avro['order'] = order if order
        end
      end
    end
  end

  class SchemaParseError < AvroError; end

  class UnknownSchemaError < SchemaParseError
    attr_reader :type_name

    def initialize(type)
      @type_name = type
      super("#{type.inspect} is not a schema we know about.")
    end
  end

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
