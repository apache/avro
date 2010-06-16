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
  'request',
  'error_union'
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

  # Read-only properties dict. Printing schemas
  # creates JSON properties directly from this dict. 
  props = property(lambda self: self._props)
  type = property(lambda self: self.get_prop('type'))

  # utility functions to manipulate properties dict
  def get_prop(self, key):
    return self.props.get(key)
  def set_prop(self, key, value):
    self.props[key] = value

class Name(object):
  """Class to describe Avro name."""
  
  def __init__(self, name_attr, space_attr, default_space):
    """
    Formulate full name according to the specification.
    
    @arg name_attr: name value read in schema or None.
    @arg space_attr: namespace value read in schema or None.
    @ard default_space: the current default space or None.
    """
    # Ensure valid ctor args
    if not (isinstance(name_attr, basestring) or (name_attr is None)):
      fail_msg = 'Name must be non-empty string or None.'
      raise SchemaParseException(fail_msg)
    elif name_attr == "":
      fail_msg = 'Name must be non-empty string or None.'
      raise SchemaParseException(fail_msg)

    if not (isinstance(space_attr, basestring) or (space_attr is None)):
      fail_msg = 'Space must be non-empty string or None.'
      raise SchemaParseException(fail_msg)
    elif name_attr == "":
      fail_msg = 'Space must be non-empty string or None.'
      raise SchemaParseException(fail_msg)
  
    if not (isinstance(default_space, basestring) or (default_space is None)):
      fail_msg = 'Default space must be non-empty string or None.'
      raise SchemaParseException(fail_msg)
    elif name_attr == "":
      fail_msg = 'Default must be non-empty string or None.'
      raise SchemaParseException(fail_msg)
    
    self._full = None; 
    
    if name_attr is None or name_attr == "":
        return;
    
    if (name_attr.find('.') < 0):
      if (space_attr is not None) and (space_attr != ""):
        self._full = "%s.%s" % (space_attr, name_attr)
      else:
        if (default_space is not None) and (default_space != ""):
           self._full = "%s.%s" % (default_space, name_attr)
        else:
          self._full = name_attr
    else:
        self._full = name_attr         
    
  def __eq__(self, other):
    if not isinstance(other, Name):
        return False
    return (self.fullname == other.fullname)
      
  fullname = property(lambda self: self._full)

  def get_space(self):
    """Back out a namespace from full name."""
    if self._full is None:
        return None
    
    if (self._full.find('.') > 0):
      return self._full.rsplit(".", 1)[0]
    else:
      return ""

class Names(object):
  """Track name set and default namespace during parsing."""
  def __init__(self, default_namespace=None):
      self.names = {}
      self.default_namespace = default_namespace
      
  def has_name(self, name_attr, space_attr):
      test = Name(name_attr, space_attr, self.default_namespace).fullname
      return self.names.has_key(test)
  
  def get_name(self, name_attr, space_attr):    
      test = Name(name_attr, space_attr, self.default_namespace).fullname
      if not self.names.has_key(test):
          return None
      return self.names[test]
      
  def add_name(self, name_attr, space_attr, new_schema):
    """
    Add a new schema object to the name set.
    
      @arg name_attr: name value read in schema
      @arg space_attr: namespace value read in schema.
      
      @return: the Name that was just added.
    """
    to_add = Name(name_attr, space_attr, self.default_namespace)
    
    if to_add.fullname in VALID_TYPES:
      fail_msg = '%s is a reserved type name.' % to_add.fullname
      raise SchemaParseException(fail_msg)
    elif self.names.has_key(to_add.fullname):
      fail_msg = 'The name "%s" is already in use.' % to_add.fullname
      raise SchemaParseException(fail_msg)

    self.names[to_add.fullname] = new_schema
    return to_add

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
    new_name = names.add_name(name, namespace, self)

    # Store name and namespace as they were read in origin schema
    self.set_prop('name', name)
    if namespace is not None: 
      self.set_prop('namespace', new_name.get_space())

    # Store full name as calculated from name, namespace
    self._fullname = new_name.fullname
    

  # read-only properties
  name = property(lambda self: self.get_prop('name'))
  namespace = property(lambda self: self.get_prop('namespace'))
  fullname = property(lambda self: self._fullname)

class Field(object):
  def __init__(self, type, name, has_default, default=None, order=None, names=None):
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
    self._props = {}
    self._type_from_names = False
    self._has_default = has_default

    if (isinstance(type, basestring) and names is not None
        and names.has_name(type, None)):
      type_schema = names.get_name(type, None)
      self._type_from_names = True
    else:
      try:
        type_schema = make_avsc_object(type, names)
      except:
        fail_msg = 'Type property "%s" not a valid Avro schema.' % type
        raise SchemaParseException(fail_msg)
    self.set_prop('type', type_schema)
    self.set_prop('name', name)
    # TODO(hammer): check to ensure default is valid
    if has_default: self.set_prop('default', default)
    if order is not None: self.set_prop('order', order)

  # read-only properties
  type = property(lambda self: self.get_prop('type'))
  name = property(lambda self: self.get_prop('name'))
  default = property(lambda self: self.get_prop('default'))
  has_default = property(lambda self: self._has_default)
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
    elif len(set(symbols)) < len(symbols):
      fail_msg = 'Duplicate symbol: %s' % symbols
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

    if isinstance(items, basestring) and names.has_name(items, None):
      items_schema = names.get_name(items, None)
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
    if isinstance(values, basestring) and names.has_name(values, None):
      values_schema = names.get_name(values)
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
      if isinstance(schema, basestring) and names.has_name(schema, None):
        new_schema = names.get_name(schema, None)
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
        raise SchemaParseException('Unions cannot contain other unions.')
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

class ErrorUnionSchema(UnionSchema):
  def __init__(self, schemas, names=None):
    # Prepend "string" to handle system errors
    UnionSchema.__init__(self, ['string'] + schemas, names)

  def __str__(self):
    to_dump = []
    for i, schema in enumerate(self.schemas):
      # Don't print the system error schema
      if schema.type == 'string': continue
      if i in self.schema_from_names_indices:
        to_dump.append(schema.fullname)
      else:
        to_dump.append(json.loads(str(schema)))
    return json.dumps(to_dump)

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

        # null values can have a default value of None
        has_default = False
        default = None
        if field.has_key('default'):
          has_default = True
          default = field.get('default')

        order = field.get('order')
        new_field = Field(type, name, has_default, default, order, names)
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
    if schema_type == 'request':
      Schema.__init__(self, schema_type)
    else:
      NamedSchema.__init__(self, schema_type, name, namespace, names)

    if schema_type == 'record': 
      old_default = names.default_namespace
      names.default_namespace = Name(name, namespace,
                                     names.default_namespace).get_space()

    # Add class members
    field_objects = RecordSchema.make_field_objects(fields, names)
    self.set_prop('fields', field_objects)
    
    if schema_type == 'record':
      names.default_namespace = old_default

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
    to_dump['fields'] = [json.loads(str(f)) for f in self.fields]
    if self.type == 'request':
      return json.dumps(to_dump['fields'])
    else:
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

  @arg names: A Name object (tracks seen names and default space)
  """
  if names == None:
    names = Names()
  
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
      elif type == 'error_union':
        declared_errors = json_data.get('declared_errors')
        return ErrorUnionSchema(declared_errors, names)
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

  # Initialize the names object
  names = Names()

  # construct the Avro Schema object
  return make_avsc_object(json_data, names)
