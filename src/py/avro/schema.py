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
"""
Contains the Schema classes.

A schema may be one of:
  A record, mapping field names to field value data;
  An error, equivalent to a record;
  An enum, containing one of a small set of symbols;
  An array of values, all of the same schema;
  A map containing string/value pairs, each of a declared schema;
  A union of other schemas;
  A fixed sized binary object;
  A unicode string;
  A sequence of bytes;
  A 32-bit signed int;
  A 64-bit signed long;
  A 32-bit floating-point float;
  A 64-bit floating-point double;
  A boolean; or
  Null.
"""
try:
  import simplejson as json
except ImportError:
  import json

#
# Constants
#

PRIMITIVE_TYPES = (
  'null',
  'boolean',
  'string',
  'bytes',
  'int',
  'long',
  'float',
  'double',
)

NAMED_TYPES = (
  'fixed',
  'enum',
  'record',
  'error',
)

VALID_TYPES = PRIMITIVE_TYPES + NAMED_TYPES + (
  'array',
  'map',
  'union',
)

RESERVED_PROPS = (
  'type',
  'name',
  'namespace',
  'fields',     # Record
  'items',      # Array
  'size',       # Fixed
  'symbols',    # Enum
  'values',     # Map
)

VALID_FIELD_SORT_ORDERS = (
  'ascending',
  'descending',
  'ignore',
)

#
# Exceptions
#

class AvroException(Exception):
  pass

class SchemaParseException(AvroException):
  pass

#
# Base Classes
#

class Schema(object):
  """Base class for all Schema classes."""
  def __init__(self, type):
    # Ensure valid ctor args
    if not isinstance(type, basestring):
      fail_msg = 'Schema type must be a string.'
      raise SchemaParseException(fail_msg)
    elif type not in VALID_TYPES:
      fail_msg = '%s is not a valid type.' % type
      raise SchemaParseException(fail_msg)

    # add members
    if not hasattr(self, '_props'): self._props = {}
    self.set_prop('type', type)

  # read-only properties
  props = property(lambda self: self._props)
  type = property(lambda self: self.get_prop('type'))

  # utility functions to manipulate properties dict
  def get_prop(self, key):
    return self.props.get(key)
  def set_prop(self, key, value):
    self.props[key] = value


class Name(object):
  """Container class for static methods on Avro names."""
  @staticmethod
  def make_fullname(name, namespace):
    if name.find('.') < 0 and namespace is not None:
      return '.'.join([namespace, name])
    else:
      return name

  @staticmethod
  def extract_namespace(name, namespace):
    parts = name.rsplit('.', 1)
    if len(parts) > 1:
      namespace, name = parts
    return name, namespace

  @staticmethod
  def add_name(names, new_schema):
    """Add a new schema object to the names dictionary (in place)."""
    new_fullname = new_schema.fullname
    if new_fullname in VALID_TYPES:
      fail_msg = '%s is a reserved type name.' % new_fullname
      raise SchemaParseException(fail_msg)
    elif names is not None and names.has_key(new_fullname):
      fail_msg = 'The name "%s" is already in use.' % new_fullname
      raise SchemaParseException(fail_msg)
    elif names is None:
      names = {}

    names[new_fullname] = new_schema
    return names

class NamedSchema(Schema):
  """Named Schemas specified in NAMED_TYPES."""
  def __init__(self, type, name, namespace=None, names=None):
    # Ensure valid ctor args
    if not name:
      fail_msg = 'Named Schemas must have a non-empty name.'
      raise SchemaParseException(fail_msg)
    elif not isinstance(name, basestring):
      fail_msg = 'The name property must be a string.'
      raise SchemaParseException(fail_msg)
    elif namespace is not None and not isinstance(namespace, basestring):
      fail_msg = 'The namespace property must be a string.'
      raise SchemaParseException(fail_msg)

    # Call parent ctor
    Schema.__init__(self, type)

    # Add class members
    name, namespace = Name.extract_namespace(name, namespace)
    self.set_prop('name', name)
    if namespace is not None: self.set_prop('namespace', namespace)

    # Add name to names dictionary
    names = Name.add_name(names, self)

  # read-only properties
  name = property(lambda self: self.get_prop('name'))
  namespace = property(lambda self: self.get_prop('namespace'))
  fullname = property(lambda self: 
                      Name.make_fullname(self.name, self.namespace))

class Field(object):
  def __init__(self, type, name, default=None, order=None, names=None):
    self._props = {}
    self._type_from_names = False

    # Ensure valid ctor args
    if not name:
      fail_msg = 'Fields must have a non-empty name.'
      raise SchemaParseException(fail_msg)
    elif not isinstance(name, basestring):
      fail_msg = 'The name property must be a string.'
      raise SchemaParseException(fail_msg)
    elif order is not None and order not in VALID_FIELD_SORT_ORDERS:
      fail_msg = 'The order property %s is not valid.' % order
      raise SchemaParseException(fail_msg)

    # add members
    if (isinstance(type, basestring) and names is not None
        and names.has_key(type)):
      type_schema = names[type]
      self._type_from_names = True
    else:
      try:
        type_schema = make_avsc_object(type, names)
      except:
        fail_msg = 'Type property not a valid Avro schema.'
        raise SchemaParseException(fail_msg)
    self.set_prop('type', type_schema)
    self.set_prop('name', name)
    # TODO(hammer): check to ensure default is valid
    if default is not None: self.set_prop('default', default)
    if order is not None: self.set_prop('order', order)

  # read-only properties
  type = property(lambda self: self.get_prop('type'))
  name = property(lambda self: self.get_prop('name'))
  default = property(lambda self: self.get_prop('default'))
  order = property(lambda self: self.get_prop('order'))
  props = property(lambda self: self._props)
  type_from_names = property(lambda self: self._type_from_names)

  # utility functions to manipulate properties dict
  def get_prop(self, key):
    return self.props.get(key)
  def set_prop(self, key, value):
    self.props[key] = value

  def __str__(self):
    to_dump = self.props.copy()
    if self.type_from_names:
      to_dump['type'] = self.type.fullname
    else:
      to_dump['type'] = json.loads(str(to_dump['type']))
    return json.dumps(to_dump)

  def __eq__(self, that):
    to_cmp = json.loads(str(self))
    return to_cmp == json.loads(str(that))

#
# Primitive Types
#
class PrimitiveSchema(Schema):
  """Valid primitive types are in PRIMITIVE_TYPES."""
  def __init__(self, type):
    # Ensure valid ctor args
    if type not in PRIMITIVE_TYPES:
      raise AvroException("%s is not a valid primitive type." % type)

    # Call parent ctor
    Schema.__init__(self, type)

  def __str__(self):
    # if there are no arbitrary properties, use short form
    if len(self.props) == 1:
      return '"%s"' % self.type
    else:
      return json.dumps(self.props)

  def __eq__(self, that):
    return self.props == that.props

#
# Complex Types (non-recursive)
#

class FixedSchema(NamedSchema):
  def __init__(self, name, namespace, size, names=None):
    # Ensure valid ctor args
    if not isinstance(size, int):
      fail_msg = 'Fixed Schema requires a valid integer for size property.'
      raise AvroException(fail_msg)

    # Call parent ctor
    NamedSchema.__init__(self, 'fixed', name, namespace, names)

    # Add class members
    self.set_prop('size', size)

  # read-only properties
  size = property(lambda self: self.get_prop('size'))

  def __str__(self):
    return json.dumps(self.props)

  def __eq__(self, that):
    return self.props == that.props

class EnumSchema(NamedSchema):
  def __init__(self, name, namespace, symbols, names=None):
    # Ensure valid ctor args
    if not isinstance(symbols, list):
      fail_msg = 'Enum Schema requires a JSON array for the symbols property.'
      raise AvroException(fail_msg)
    elif False in [isinstance(s, basestring) for s in symbols]:
      fail_msg = 'Enum Schems requires All symbols to be JSON strings.'
      raise AvroException(fail_msg)

    # Call parent ctor
    NamedSchema.__init__(self, 'enum', name, namespace, names)

    # Add class members
    self.set_prop('symbols', symbols)

  # read-only properties
  symbols = property(lambda self: self.get_prop('symbols'))

  def __str__(self):
    return json.dumps(self.props)

  def __eq__(self, that):
    return self.props == that.props

#
# Complex Types (recursive)
#

class ArraySchema(Schema):
  def __init__(self, items, names=None):
    # initialize private class members
    self._items_schema_from_names = False

    # Call parent ctor
    Schema.__init__(self, 'array')

    # Add class members
    if isinstance(items, basestring) and names.has_key(items):
      items_schema = names[items]
      self._items_schema_from_names = True
    else:
      try:
        items_schema = make_avsc_object(items, names)
      except:
        fail_msg = 'Items schema not a valid Avro schema.'
        raise SchemaParseException(fail_msg)

    self.set_prop('items', items_schema)

  # read-only properties
  items = property(lambda self: self.get_prop('items'))
  items_schema_from_names = property(lambda self: self._items_schema_from_names)

  def __str__(self):
    to_dump = self.props.copy()
    if self.items_schema_from_names:
      to_dump['items'] = self.get_prop('items').fullname
    else:
      to_dump['items'] = json.loads(str(to_dump['items']))
    return json.dumps(to_dump)

  def __eq__(self, that):
    to_cmp = json.loads(str(self))
    return to_cmp == json.loads(str(that))

class MapSchema(Schema):
  def __init__(self, values, names=None):
    # initialize private class members
    self._values_schema_from_names = False

    # Call parent ctor
    Schema.__init__(self, 'map')

    # Add class members
    if isinstance(values, basestring) and names.has_key(values):
      values_schema = names[values]
      self._values_schema_from_names = True
    else:
      try:
        values_schema = make_avsc_object(values, names)
      except:
        fail_msg = 'Values schema not a valid Avro schema.'
        raise SchemaParseException(fail_msg)

    self.set_prop('values', values_schema)

  # read-only properties
  values = property(lambda self: self.get_prop('values'))
  values_schema_from_names = property(lambda self:
                                      self._values_schema_from_names)

  def __str__(self):
    to_dump = self.props.copy()
    if self.values_schema_from_names:
      to_dump['values'] = self.get_prop('values').fullname
    else:
      to_dump['values'] = json.loads(str(to_dump['values']))
    return json.dumps(to_dump)

  def __eq__(self, that):
    to_cmp = json.loads(str(self))
    return to_cmp == json.loads(str(that))

class UnionSchema(Schema):
  """
  names is a dictionary of schema objects
  """
  def __init__(self, schemas, names=None):
    # Ensure valid ctor args
    if not isinstance(schemas, list):
      fail_msg = 'Union schema requires a list of schemas.'
      raise SchemaParseException(fail_msg)

    # Call parent ctor
    Schema.__init__(self, 'union')

    # Add class members
    schema_objects = []
    self._schema_from_names_indices = []
    for i, schema in enumerate(schemas):
      from_names = False
      if isinstance(schema, basestring) and names.has_key(schema):
        new_schema = names[schema]
        from_names = True
      else:
        try:
          new_schema = make_avsc_object(schema, names)
        except:
          raise SchemaParseException('Union item must be a valid Avro schema.')
      # check the new schema
      if (new_schema.type in VALID_TYPES and new_schema.type not in NAMED_TYPES
          and new_schema.type in [schema.type for schema in schema_objects]):
        raise SchemaParseException('%s type already in Union' % new_schema.type)
      elif new_schema.type == 'union':
        raise SchemaParseException('Unions cannont contain other unions.')
      else:
        schema_objects.append(new_schema)
        if from_names: self._schema_from_names_indices.append(i)
    self._schemas = schema_objects

  # read-only properties
  schemas = property(lambda self: self._schemas)
  schema_from_names_indices = property(lambda self:
                                       self._schema_from_names_indices)

  def __str__(self):
    to_dump = []
    for i, schema in enumerate(self.schemas):
      if i in self.schema_from_names_indices:
        to_dump.append(schema.fullname)
      else:
        to_dump.append(json.loads(str(schema)))
    return json.dumps(to_dump)

  def __eq__(self, that):
    to_cmp = json.loads(str(self))
    return to_cmp == json.loads(str(that))

class RecordSchema(NamedSchema):
  @staticmethod
  def make_field_objects(field_data, names):
    """We're going to need to make message parameters too."""
    field_objects = []
    field_names = []
    for i, field in enumerate(field_data):
      if hasattr(field, 'get') and callable(field.get):
        type = field.get('type')
        name = field.get('name')
        default = field.get('default')
        order = field.get('order')
        new_field = Field(type, name, default, order, names)
        # make sure field name has not been used yet
        if new_field.name in field_names:
          fail_msg = 'Field name %s already in use.' % new_field.name
          raise SchemaParseException(fail_msg)
        field_names.append(new_field.name)
      else:
        raise SchemaParseException('Not a valid field: %s' % field)
      field_objects.append(new_field)
    return field_objects

  def __init__(self, name, namespace, fields, names=None, schema_type='record'):
    # Ensure valid ctor args
    if fields is None:
      fail_msg = 'Record schema requires a non-empty fields property.'
      raise SchemaParseException(fail_msg)
    elif not isinstance(fields, list):
      fail_msg = 'Fields property must be a list of Avro schemas.'
      raise SchemaParseException(fail_msg)

    # Call parent ctor (adds own name to namespace, too)
    NamedSchema.__init__(self, schema_type, name, namespace, names)

    # Add class members
    field_objects = RecordSchema.make_field_objects(fields, names)
    self.set_prop('fields', field_objects)

  # read-only properties
  fields = property(lambda self: self.get_prop('fields'))

  @property
  def fields_dict(self):
    fields_dict = {}
    for field in self.fields:
      fields_dict[field.name] = field
    return fields_dict

  def __str__(self):
    to_dump = self.props.copy()
    new_fields = []
    for field in to_dump['fields']:
      new_fields.append(json.loads(str(field)))
    to_dump['fields'] = new_fields
    return json.dumps(to_dump)

  def __eq__(self, that):
    to_cmp = json.loads(str(self))
    return to_cmp == json.loads(str(that))

#
# Module Methods
#

# TODO(hammer): handle non-reserved properties
def make_avsc_object(json_data, names=None):
  """
  Build Avro Schema from data parsed out of JSON string.

  @arg names: dict of schema name, object pairs
  """
  # JSON object (non-union)
  if hasattr(json_data, 'get') and callable(json_data.get):
    type = json_data.get('type')
    if type in PRIMITIVE_TYPES:
      return PrimitiveSchema(type)
    elif type in NAMED_TYPES:
      name = json_data.get('name')
      namespace = json_data.get('namespace')
      if type == 'fixed':
        size = json_data.get('size')
        return FixedSchema(name, namespace, size, names)
      elif type == 'enum':
        symbols = json_data.get('symbols')
        return EnumSchema(name, namespace, symbols, names)
      elif type in ['record', 'error']:
        fields = json_data.get('fields')
        return RecordSchema(name, namespace, fields, names, type)
      else:
        raise SchemaParseException('Unknown Named Type: %s' % type)
    elif type in VALID_TYPES:
      if type == 'array':
        items = json_data.get('items')
        return ArraySchema(items, names)
      elif type == 'map':
        values = json_data.get('values')
        return MapSchema(values, names)
      else:
        raise SchemaParseException('Unknown Valid Type: %s' % type)
    elif type is None:
      raise SchemaParseException('No "type" property: %s' % json_data)
    else:
      raise SchemaParseException('Undefined type: %s' % type)
  # JSON array (union)
  elif isinstance(json_data, list):
    return UnionSchema(json_data, names)
  # JSON string (primitive)
  elif json_data in PRIMITIVE_TYPES:
    return PrimitiveSchema(json_data)
  # not for us!
  else:
    fail_msg = "Could not make an Avro Schema object from %s." % json_data
    raise SchemaParseException(fail_msg)

# TODO(hammer): make method for reading from a file?
def parse(json_string):
  """Constructs the Schema from the JSON text."""
  # TODO(hammer): preserve stack trace from JSON parse
  # parse the JSON
  try:
    json_data = json.loads(json_string)
  except:
    raise SchemaParseException('Error parsing JSON: %s' % json_string)

  # Initialize the names dictionary
  names = {}

  # construct the Avro Schema object
  return make_avsc_object(json_data, names)
