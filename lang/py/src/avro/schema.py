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
  import json
except ImportError:
  import simplejson as json

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

SCHEMA_RESERVED_PROPS = (
  'type',
  'name',
  'namespace',
  'fields',     # Record
  'items',      # Array
  'size',       # Fixed
  'symbols',    # Enum
  'values',     # Map
  'doc',
)

FIELD_RESERVED_PROPS = (
  'default',
  'name',
  'doc',
  'order',
  'type',
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
  def __init__(self, type, other_props=None):
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
    self.type = type
    self._props.update(other_props or {})

  # Read-only properties dict. Printing schemas
  # creates JSON properties directly from this dict. 
  props = property(lambda self: self._props)

  # Read-only property dict. Non-reserved properties
  other_props = property(lambda self: get_other_props(self._props, SCHEMA_RESERVED_PROPS),
                         doc="dictionary of non-reserved properties")

  # utility functions to manipulate properties dict
  def get_prop(self, key):
    return self._props.get(key)

  def set_prop(self, key, value):
    self._props[key] = value

  def __str__(self):
    return json.dumps(self.to_json())

  def to_json(self, names):
    """
    Converts the schema object into its AVRO specification representation.

    Schema types that have names (records, enums, and fixed) must
    be aware of not re-defining schemas that are already listed
    in the parameter names.
    """
    raise Exception("Must be implemented by subclasses.")

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
  
  def prune_namespace(self, properties):
    """given a properties, return properties with namespace removed if
    it matches the own default namespace"""
    if self.default_namespace is None:
      # I have no default -- no change
      return properties
    if 'namespace' not in properties:
      # he has no namespace - no change
      return properties
    if properties['namespace'] != self.default_namespace:
      # we're different - leave his stuff alone
      return properties
    # we each have a namespace and it's redundant. delete his.
    prunable = properties.copy()
    del(prunable['namespace'])
    return prunable

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
  def __init__(self, type, name, namespace=None, names=None, other_props=None):
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
    Schema.__init__(self, type, other_props)

    # Add class members
    new_name = names.add_name(name, namespace, self)

    # Store name and namespace as they were read in origin schema
    self.set_prop('name', name)
    if namespace is not None: 
      self.set_prop('namespace', new_name.get_space())

    # Store full name as calculated from name, namespace
    self._fullname = new_name.fullname
    
  def name_ref(self, names):
    if self.namespace == names.default_namespace:
      return self.name
    else:
      return self.fullname

  # read-only properties
  name = property(lambda self: self.get_prop('name'))
  namespace = property(lambda self: self.get_prop('namespace'))
  fullname = property(lambda self: self._fullname)

class Field(object):
  def __init__(self, type, name, has_default, default=None,
               order=None,names=None, doc=None, other_props=None):
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
    self._has_default = has_default
    self._props.update(other_props or {})

    if (isinstance(type, basestring) and names is not None
        and names.has_name(type, None)):
      type_schema = names.get_name(type, None)
    else:
      try:
        type_schema = make_avsc_object(type, names)
      except Exception, e:
        fail_msg = 'Type property "%s" not a valid Avro schema: %s' % (type, e)
        raise SchemaParseException(fail_msg)
    self.set_prop('type', type_schema)
    self.set_prop('name', name)
    self.type = type_schema
    self.name = name
    # TODO(hammer): check to ensure default is valid
    if has_default: self.set_prop('default', default)
    if order is not None: self.set_prop('order', order)
    if doc is not None: self.set_prop('doc', doc)

  # read-only properties
  default = property(lambda self: self.get_prop('default'))
  has_default = property(lambda self: self._has_default)
  order = property(lambda self: self.get_prop('order'))
  doc = property(lambda self: self.get_prop('doc'))
  props = property(lambda self: self._props)

  # Read-only property dict. Non-reserved properties
  other_props = property(lambda self: get_other_props(self._props, FIELD_RESERVED_PROPS),
                         doc="dictionary of non-reserved properties")

# utility functions to manipulate properties dict
  def get_prop(self, key):
    return self._props.get(key)
  def set_prop(self, key, value):
    self._props[key] = value

  def __str__(self):
    return json.dumps(self.to_json())

  def to_json(self, names=None):
    if names is None:
      names = Names()
    to_dump = self.props.copy()
    to_dump['type'] = self.type.to_json(names)
    return to_dump

  def __eq__(self, that):
    to_cmp = json.loads(str(self))
    return to_cmp == json.loads(str(that))

#
# Primitive Types
#
class PrimitiveSchema(Schema):
  """Valid primitive types are in PRIMITIVE_TYPES."""
  def __init__(self, type, other_props=None):
    # Ensure valid ctor args
    if type not in PRIMITIVE_TYPES:
      raise AvroException("%s is not a valid primitive type." % type)

    # Call parent ctor
    Schema.__init__(self, type, other_props=other_props)

    self.fullname = type

  def to_json(self, names=None):
    if len(self.props) == 1:
      return self.fullname
    else:
      return self.props

  def __eq__(self, that):
    return self.props == that.props

#
# Complex Types (non-recursive)
#

class FixedSchema(NamedSchema):
  def __init__(self, name, namespace, size, names=None, other_props=None):
    # Ensure valid ctor args
    if not isinstance(size, int):
      fail_msg = 'Fixed Schema requires a valid integer for size property.'
      raise AvroException(fail_msg)

    # Call parent ctor
    NamedSchema.__init__(self, 'fixed', name, namespace, names, other_props)

    # Add class members
    self.set_prop('size', size)

  # read-only properties
  size = property(lambda self: self.get_prop('size'))

  def to_json(self, names=None):
    if names is None:
      names = Names()
    if self.fullname in names.names:
      return self.name_ref(names)
    else:
      names.names[self.fullname] = self
      return names.prune_namespace(self.props)

  def __eq__(self, that):
    return self.props == that.props

class EnumSchema(NamedSchema):
  def __init__(self, name, namespace, symbols, names=None, doc=None, other_props=None):
    # Ensure valid ctor args
    if not isinstance(symbols, list):
      fail_msg = 'Enum Schema requires a JSON array for the symbols property.'
      raise AvroException(fail_msg)
    elif False in [isinstance(s, basestring) for s in symbols]:
      fail_msg = 'Enum Schema requires all symbols to be JSON strings.'
      raise AvroException(fail_msg)
    elif len(set(symbols)) < len(symbols):
      fail_msg = 'Duplicate symbol: %s' % symbols
      raise AvroException(fail_msg)

    # Call parent ctor
    NamedSchema.__init__(self, 'enum', name, namespace, names, other_props)

    # Add class members
    self.set_prop('symbols', symbols)
    if doc is not None: self.set_prop('doc', doc)

  # read-only properties
  symbols = property(lambda self: self.get_prop('symbols'))
  doc = property(lambda self: self.get_prop('doc'))

  def to_json(self, names=None):
    if names is None:
      names = Names()
    if self.fullname in names.names:
      return self.name_ref(names)
    else:
      names.names[self.fullname] = self
      return names.prune_namespace(self.props)

  def __eq__(self, that):
    return self.props == that.props

#
# Complex Types (recursive)
#

class ArraySchema(Schema):
  def __init__(self, items, names=None, other_props=None):
    # Call parent ctor
    Schema.__init__(self, 'array', other_props)
    # Add class members

    if isinstance(items, basestring) and names.has_name(items, None):
      items_schema = names.get_name(items, None)
    else:
      try:
        items_schema = make_avsc_object(items, names)
      except SchemaParseException, e:
        fail_msg = 'Items schema (%s) not a valid Avro schema: %s (known names: %s)' % (items, e, names.names.keys())
        raise SchemaParseException(fail_msg)

    self.set_prop('items', items_schema)

  # read-only properties
  items = property(lambda self: self.get_prop('items'))

  def to_json(self, names=None):
    if names is None:
      names = Names()
    to_dump = self.props.copy()
    item_schema = self.get_prop('items')
    to_dump['items'] = item_schema.to_json(names)
    return to_dump

  def __eq__(self, that):
    to_cmp = json.loads(str(self))
    return to_cmp == json.loads(str(that))

class MapSchema(Schema):
  def __init__(self, values, names=None, other_props=None):
    # Call parent ctor
    Schema.__init__(self, 'map',other_props)

    # Add class members
    if isinstance(values, basestring) and names.has_name(values, None):
      values_schema = names.get_name(values, None)
    else:
      try:
        values_schema = make_avsc_object(values, names)
      except:
        fail_msg = 'Values schema not a valid Avro schema.'
        raise SchemaParseException(fail_msg)

    self.set_prop('values', values_schema)

  # read-only properties
  values = property(lambda self: self.get_prop('values'))

  def to_json(self, names=None):
    if names is None:
      names = Names()
    to_dump = self.props.copy()
    to_dump['values'] = self.get_prop('values').to_json(names)
    return to_dump

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
    for schema in schemas:
      if isinstance(schema, basestring) and names.has_name(schema, None):
        new_schema = names.get_name(schema, None)
      else:
        try:
          new_schema = make_avsc_object(schema, names)
        except Exception, e:
          raise SchemaParseException('Union item must be a valid Avro schema: %s' % str(e))
      # check the new schema
      if (new_schema.type in VALID_TYPES and new_schema.type not in NAMED_TYPES
          and new_schema.type in [schema.type for schema in schema_objects]):
        raise SchemaParseException('%s type already in Union' % new_schema.type)
      elif new_schema.type == 'union':
        raise SchemaParseException('Unions cannot contain other unions.')
      else:
        schema_objects.append(new_schema)
    self._schemas = schema_objects

  # read-only properties
  schemas = property(lambda self: self._schemas)

  def to_json(self, names=None):
    if names is None:
      names = Names()
    to_dump = []
    for schema in self.schemas:
      to_dump.append(schema.to_json(names))
    return to_dump

  def __eq__(self, that):
    to_cmp = json.loads(str(self))
    return to_cmp == json.loads(str(that))

class ErrorUnionSchema(UnionSchema):
  def __init__(self, schemas, names=None):
    # Prepend "string" to handle system errors
    UnionSchema.__init__(self, ['string'] + schemas, names)

  def to_json(self, names=None):
    if names is None:
      names = Names()
    to_dump = []
    for schema in self.schemas:
      # Don't print the system error schema
      if schema.type == 'string': continue
      to_dump.append(schema.to_json(names))
    return to_dump

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
        doc = field.get('doc')
        other_props = get_other_props(field, FIELD_RESERVED_PROPS)
        new_field = Field(type, name, has_default, default, order, names, doc,
                         other_props)
        # make sure field name has not been used yet
        if new_field.name in field_names:
          fail_msg = 'Field name %s already in use.' % new_field.name
          raise SchemaParseException(fail_msg)
        field_names.append(new_field.name)
      else:
        raise SchemaParseException('Not a valid field: %s' % field)
      field_objects.append(new_field)
    return field_objects

  def __init__(self, name, namespace, fields, names=None, schema_type='record',
               doc=None, other_props=None):
    # Ensure valid ctor args
    if fields is None:
      fail_msg = 'Record schema requires a non-empty fields property.'
      raise SchemaParseException(fail_msg)
    elif not isinstance(fields, list):
      fail_msg = 'Fields property must be a list of Avro schemas.'
      raise SchemaParseException(fail_msg)

    # Call parent ctor (adds own name to namespace, too)
    if schema_type == 'request':
      Schema.__init__(self, schema_type, other_props)
    else:
      NamedSchema.__init__(self, schema_type, name, namespace, names,
                           other_props)

    if schema_type == 'record': 
      old_default = names.default_namespace
      names.default_namespace = Name(name, namespace,
                                     names.default_namespace).get_space()

    # Add class members
    field_objects = RecordSchema.make_field_objects(fields, names)
    self.set_prop('fields', field_objects)
    if doc is not None: self.set_prop('doc', doc)

    if schema_type == 'record':
      names.default_namespace = old_default

  # read-only properties
  fields = property(lambda self: self.get_prop('fields'))
  doc = property(lambda self: self.get_prop('doc'))

  @property
  def fields_dict(self):
    fields_dict = {}
    for field in self.fields:
      fields_dict[field.name] = field
    return fields_dict

  def to_json(self, names=None):
    if names is None:
      names = Names()
    # Request records don't have names
    if self.type == 'request':
      return [ f.to_json(names) for f in self.fields ]

    if self.fullname in names.names:
      return self.name_ref(names)
    else:
      names.names[self.fullname] = self

    to_dump = names.prune_namespace(self.props.copy())
    to_dump['fields'] = [ f.to_json(names) for f in self.fields ]
    return to_dump

  def __eq__(self, that):
    to_cmp = json.loads(str(self))
    return to_cmp == json.loads(str(that))

#
# Module Methods
#
def get_other_props(all_props,reserved_props):
  """
  Retrieve the non-reserved properties from a dictionary of properties
  @args reserved_props: The set of reserved properties to exclude
  """
  if hasattr(all_props, 'items') and callable(all_props.items):
    return dict([(k,v) for (k,v) in all_props.items() if k not in
                 reserved_props ])


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
    other_props = get_other_props(json_data, SCHEMA_RESERVED_PROPS)
    if type in PRIMITIVE_TYPES:
      return PrimitiveSchema(type, other_props)
    elif type in NAMED_TYPES:
      name = json_data.get('name')
      namespace = json_data.get('namespace', names.default_namespace)
      if type == 'fixed':
        size = json_data.get('size')
        return FixedSchema(name, namespace, size, names, other_props)
      elif type == 'enum':
        symbols = json_data.get('symbols')
        doc = json_data.get('doc')
        return EnumSchema(name, namespace, symbols, names, doc, other_props)
      elif type in ['record', 'error']:
        fields = json_data.get('fields')
        doc = json_data.get('doc')
        return RecordSchema(name, namespace, fields, names, type, doc, other_props)
      else:
        raise SchemaParseException('Unknown Named Type: %s' % type)
    elif type in VALID_TYPES:
      if type == 'array':
        items = json_data.get('items')
        return ArraySchema(items, names, other_props)
      elif type == 'map':
        values = json_data.get('values')
        return MapSchema(values, names, other_props)
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
  # parse the JSON
  try:
    json_data = json.loads(json_string)
  except Exception, e:
    import sys
    raise SchemaParseException('Error parsing JSON: %s, error = %s'
                               % (json_string, e)), None, sys.exc_info()[2]

  # Initialize the names object
  names = Names()

  # construct the Avro Schema object
  return make_avsc_object(json_data, names)
