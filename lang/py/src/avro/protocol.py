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
Protocol implementation.
"""
try:
  from hashlib import md5
except ImportError:
  from md5 import md5
try:
  import json
except ImportError:
  import simplejson as json
from avro import schema

#
# Constants
#

# TODO(hammer): confirmed 'fixed' with Doug
VALID_TYPE_SCHEMA_TYPES = ('enum', 'record', 'error', 'fixed')

#
# Exceptions
#

class ProtocolParseException(schema.AvroException):
  pass

#
# Base Classes
#

class Protocol(object):
  """An application protocol."""
  def _parse_types(self, types, type_names):
    type_objects = []
    for type in types:
      type_object = schema.make_avsc_object(type, type_names)
      if type_object.type not in VALID_TYPE_SCHEMA_TYPES:
        fail_msg = 'Type %s not an enum, fixed, record, or error.' % type
        raise ProtocolParseException(fail_msg)
      type_objects.append(type_object)
    return type_objects

  def _parse_messages(self, messages, names):
    message_objects = {}
    for name, body in messages.iteritems():
      if message_objects.has_key(name):
        fail_msg = 'Message name "%s" repeated.' % name
        raise ProtocolParseException(fail_msg)
      elif not(hasattr(body, 'get') and callable(body.get)):
        fail_msg = 'Message name "%s" has non-object body %s.' % (name, body)
        raise ProtocolParseException(fail_msg)
      request = body.get('request')
      response = body.get('response')
      errors = body.get('errors')
      message_objects[name] = Message(name, request, response, errors, names)
    return message_objects

  def __init__(self, name, namespace=None, types=None, messages=None):
    # Ensure valid ctor args
    if not name:
      fail_msg = 'Protocols must have a non-empty name.'
      raise ProtocolParseException(fail_msg)
    elif not isinstance(name, basestring):
      fail_msg = 'The name property must be a string.'
      raise ProtocolParseException(fail_msg)
    elif namespace is not None and not isinstance(namespace, basestring):
      fail_msg = 'The namespace property must be a string.'
      raise ProtocolParseException(fail_msg)
    elif types is not None and not isinstance(types, list):
      fail_msg = 'The types property must be a list.'
      raise ProtocolParseException(fail_msg)
    elif (messages is not None and 
          not(hasattr(messages, 'get') and callable(messages.get))):
      fail_msg = 'The messages property must be a JSON object.'
      raise ProtocolParseException(fail_msg)

    self._props = {}
    self.set_prop('name', name)
    type_names = schema.Names()
    if namespace is not None: 
      self.set_prop('namespace', namespace)
      type_names.default_namespace = namespace
    if types is not None:
      self.set_prop('types', self._parse_types(types, type_names))
    if messages is not None:
      self.set_prop('messages', self._parse_messages(messages, type_names))
    self._md5 = md5(str(self)).digest()

  # read-only properties
  name = property(lambda self: self.get_prop('name'))
  namespace = property(lambda self: self.get_prop('namespace'))
  fullname = property(lambda self: 
                      schema.Name(self.name, self.namespace).fullname)
  types = property(lambda self: self.get_prop('types'))
  types_dict = property(lambda self: dict([(type.name, type)
                                           for type in self.types]))
  messages = property(lambda self: self.get_prop('messages'))
  md5 = property(lambda self: self._md5)
  props = property(lambda self: self._props)

  # utility functions to manipulate properties dict
  def get_prop(self, key):
    return self.props.get(key)
  def set_prop(self, key, value):
    self.props[key] = value  

  def to_json(self):
    to_dump = {}
    to_dump['protocol'] = self.name
    names = schema.Names(default_namespace=self.namespace)
    if self.namespace: 
      to_dump['namespace'] = self.namespace
    if self.types:
      to_dump['types'] = [ t.to_json(names) for t in self.types ]
    if self.messages:
      messages_dict = {}
      for name, body in self.messages.iteritems():
        messages_dict[name] = body.to_json(names)
      to_dump['messages'] = messages_dict
    return to_dump

  def __str__(self):
    return json.dumps(self.to_json())

  def __eq__(self, that):
    to_cmp = json.loads(str(self))
    return to_cmp == json.loads(str(that))

class Message(object):
  """A Protocol message."""
  def _parse_request(self, request, names):
    if not isinstance(request, list):
      fail_msg = 'Request property not a list: %s' % request
      raise ProtocolParseException(fail_msg)
    return schema.RecordSchema(None, None, request, names, 'request')
  
  def _parse_response(self, response, names):
    if isinstance(response, basestring) and names.has_name(response, None):
      return names.get_name(response, None)
    else:
      return schema.make_avsc_object(response, names)

  def _parse_errors(self, errors, names):
    if not isinstance(errors, list):
      fail_msg = 'Errors property not a list: %s' % errors
      raise ProtocolParseException(fail_msg)
    errors_for_parsing = {'type': 'error_union', 'declared_errors': errors}
    return schema.make_avsc_object(errors_for_parsing, names)

  def __init__(self,  name, request, response, errors=None, names=None):
    self._name = name

    self._props = {}
    self.set_prop('request', self._parse_request(request, names))
    self.set_prop('response', self._parse_response(response, names))
    if errors is not None:
      self.set_prop('errors', self._parse_errors(errors, names))

  # read-only properties
  name = property(lambda self: self._name)
  request = property(lambda self: self.get_prop('request'))
  response = property(lambda self: self.get_prop('response'))
  errors = property(lambda self: self.get_prop('errors'))
  props = property(lambda self: self._props)

  # utility functions to manipulate properties dict
  def get_prop(self, key):
    return self.props.get(key)
  def set_prop(self, key, value):
    self.props[key] = value  

  def __str__(self):
    return json.dumps(self.to_json())

  def to_json(self, names=None):
    if names is None:
      names = schema.Names()
    to_dump = {}
    to_dump['request'] = self.request.to_json(names)
    to_dump['response'] = self.response.to_json(names)
    if self.errors:
      to_dump['errors'] = self.errors.to_json(names)
    return to_dump

  def __eq__(self, that):
    return self.name == that.name and self.props == that.props
      
def make_avpr_object(json_data):
  """Build Avro Protocol from data parsed out of JSON string."""
  if hasattr(json_data, 'get') and callable(json_data.get):
    name = json_data.get('protocol')
    namespace = json_data.get('namespace')
    types = json_data.get('types')
    messages = json_data.get('messages')
    return Protocol(name, namespace, types, messages)
  else:
    raise ProtocolParseException('Not a JSON object: %s' % json_data)

def parse(json_string):
  """Constructs the Protocol from the JSON text."""
  try:
    json_data = json.loads(json_string)
  except:
    raise ProtocolParseException('Error parsing JSON: %s' % json_string)

  # construct the Avro Protocol object
  return make_avpr_object(json_data)

