#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Protocol implementation.
"""

from types import MappingProxyType

import hashlib
import json
import logging

from avro import schema

# ------------------------------------------------------------------------------
# Constants

# Allowed top-level schemas in a protocol:
VALID_TYPE_SCHEMA_TYPES = frozenset(['enum', 'record', 'error', 'fixed'])


# ------------------------------------------------------------------------------
# Exceptions


class ProtocolParseException(schema.AvroException):
  """Error while parsing a JSON protocol descriptor."""
  pass


# ------------------------------------------------------------------------------
# Base Classes


class Protocol(object):
  """An application protocol."""

  @staticmethod
  def _ParseTypeDesc(type_desc, names):
    type_schema = schema.SchemaFromJSONData(type_desc, names=names)
    if type_schema.type not in VALID_TYPE_SCHEMA_TYPES:
      raise ProtocolParseException(
          'Invalid type %r in protocol %r: '
          'protocols can only declare types %s.'
          % (type_schema, avro_name, ','.join(VALID_TYPE_SCHEMA_TYPES)))
    return type_schema

  @staticmethod
  def _ParseMessageDesc(name, message_desc, names):
    """Parses a protocol message descriptor.

    Args:
      name: Name of the message.
      message_desc: Descriptor of the message.
      names: Tracker of the named Avro schema.
    Returns:
      The parsed protocol message.
    Raises:
      ProtocolParseException: if the descriptor is invalid.
    """
    request_desc = message_desc.get('request')
    if request_desc is None:
      raise ProtocolParseException(
          'Invalid message descriptor with no "request": %r.' % message_desc)
    request_schema = Message._ParseRequestFromJSONDesc(
        request_desc=request_desc,
        names=names,
    )

    response_desc = message_desc.get('response')
    if response_desc is None:
      raise ProtocolParseException(
          'Invalid message descriptor with no "response": %r.' % message_desc)
    response_schema = Message._ParseResponseFromJSONDesc(
        response_desc=response_desc,
        names=names,
    )

    # Errors are optional:
    errors_desc = message_desc.get('errors', tuple())
    error_union_schema = Message._ParseErrorsFromJSONDesc(
        errors_desc=errors_desc,
        names=names,
    )

    return Message(
        name=name,
        request=request_schema,
        response=response_schema,
        errors=error_union_schema,
    )

  @staticmethod
  def _ParseMessageDescMap(message_desc_map, names):
    for name, message_desc in message_desc_map.items():
      yield Protocol._ParseMessageDesc(
          name=name,
          message_desc=message_desc,
          names=names,
      )

  def __init__(
      self,
      name,
      namespace=None,
      types=tuple(),
      messages=tuple(),
  ):
    """Initializes a new protocol object.

    Args:
      name: Protocol name (absolute or relative).
      namespace: Optional explicit namespace (if name is relative).
      types: Collection of types in the protocol.
      messages: Collection of messages in the protocol.
    """
    self._avro_name = schema.Name(name=name, namespace=namespace)
    self._fullname = self._avro_name.fullname
    self._name = self._avro_name.simple_name
    self._namespace = self._avro_name.namespace

    self._props = {}
    self._props['name'] = self._name
    if self._namespace:
      self._props['namespace'] = self._namespace

    self._names = schema.Names(default_namespace=self._namespace)

    self._types = tuple(types)
    # Map: type full name -> type schema
    self._type_map = MappingProxyType({type.fullname: type for type in self._types})
    # This assertion cannot fail unless we don't track named schemas properly:
    assert (len(self._types) == len(self._type_map)), (
        'Type list %r does not match type map: %r'
        % (self._types, self._type_map))
    # TODO: set props['types']

    self._messages = tuple(messages)

    # Map: message name -> Message
    # Note that message names are simple names unique within the protocol.
    self._message_map = MappingProxyType({message.name: message for message in self._messages})
    if len(self._messages) != len(self._message_map):
      raise ProtocolParseException(
          'Invalid protocol %s with duplicate message name: %r'
          % (self._avro_name, self._messages))
    # TODO: set props['messages']

    self._md5 = hashlib.md5(str(self).encode('utf-8')).digest()

  @property
  def name(self):
    """Returns: the simple name of the protocol."""
    return self._name

  @property
  def namespace(self):
    """Returns: the namespace this protocol belongs to."""
    return self._namespace

  @property
  def fullname(self):
    """Returns: the fully qualified name of this protocol."""
    return self._fullname

  @property
  def types(self):
    """Returns: the collection of types declared in this protocol."""
    return self._types

  @property
  def type_map(self):
    """Returns: the map of types in this protocol, indexed by their full name."""
    return self._type_map

  @property
  def messages(self):
    """Returns: the collection of messages declared in this protocol."""
    return self._messages

  @property
  def message_map(self):
    """Returns: the map of messages in this protocol, indexed by their name."""
    return self._message_map

  @property
  def md5(self):
    return self._md5

  @property
  def props(self):
    return self._props

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
      for name, body in self.message_map.items():
        messages_dict[name] = body.to_json(names)
      to_dump['messages'] = messages_dict
    return to_dump

  def __str__(self):
    return json.dumps(self.to_json(), cls=schema.MappingProxyEncoder)

  def __eq__(self, that):
    to_cmp = json.loads(str(self))
    return to_cmp == json.loads(str(that))


# ------------------------------------------------------------------------------


class Message(object):
  """A Protocol message."""

  @staticmethod
  def _ParseRequestFromJSONDesc(request_desc, names):
    """Parses the request descriptor of a protocol message.

    Args:
      request_desc: Descriptor of the message request.
          This is a list of fields that defines an unnamed record.
      names: Tracker for named Avro schemas.
    Returns:
      The parsed request schema, as an unnamed record.
    """
    fields = schema.RecordSchema._MakeFieldList(request_desc, names=names)
    return schema.RecordSchema(
        name=None,
        namespace=None,
        fields=fields,
        names=names,
        record_type=schema.REQUEST,
    )

  @staticmethod
  def _ParseResponseFromJSONDesc(response_desc, names):
    """Parses the response descriptor of a protocol message.

    Args:
      response_desc: Descriptor of the message response.
          This is an arbitrary Avro schema descriptor.
    Returns:
      The parsed response schema.
    """
    return schema.SchemaFromJSONData(response_desc, names=names)

  @staticmethod
  def _ParseErrorsFromJSONDesc(errors_desc, names):
    """Parses the errors descriptor of a protocol message.

    Args:
      errors_desc: Descriptor of the errors thrown by the protocol message.
          This is a list of error types understood as an implicit union.
          Each error type is an arbitrary Avro schema.
      names: Tracker for named Avro schemas.
    Returns:
      The parsed ErrorUnionSchema.
    """
    error_union_desc = {
        'type': schema.ERROR_UNION,
        'declared_errors': errors_desc,
    }
    return schema.SchemaFromJSONData(error_union_desc, names=names)

  def __init__(self,  name, request, response, errors=None):
    self._name = name

    self._props = {}
    # TODO: set properties
    self._request = request
    self._response = response
    self._errors = errors

  @property
  def name(self):
    return self._name

  @property
  def request(self):
    return self._request

  @property
  def response(self):
    return self._response

  @property
  def errors(self):
    return self._errors

  def props(self):
    return self._props

  def __str__(self):
    return json.dumps(self.to_json(), cls=schema.MappingProxyEncoder)

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


# ------------------------------------------------------------------------------


def ProtocolFromJSONData(json_data):
  """Builds an Avro  Protocol from its JSON descriptor.

  Args:
    json_data: JSON data representing the descriptor of the Avro protocol.
  Returns:
    The Avro Protocol parsed from the JSON descriptor.
  Raises:
    ProtocolParseException: if the descriptor is invalid.
  """
  if type(json_data) != dict:
    raise ProtocolParseException(
        'Invalid JSON descriptor for an Avro protocol: %r' % json_data)

  name = json_data.get('protocol')
  if name is None:
    raise ProtocolParseException(
        'Invalid protocol descriptor with no "name": %r' % json_data)

  # Namespace is optional
  namespace = json_data.get('namespace')

  avro_name = schema.Name(name=name, namespace=namespace)
  names = schema.Names(default_namespace=avro_name.namespace)

  type_desc_list = json_data.get('types', tuple())
  types = tuple(map(
      lambda desc: Protocol._ParseTypeDesc(desc, names=names),
      type_desc_list))

  message_desc_map = json_data.get('messages', dict())
  messages = tuple(Protocol._ParseMessageDescMap(message_desc_map, names=names))

  return Protocol(
      name=name,
      namespace=namespace,
      types=types,
      messages=messages,
  )


def Parse(json_string):
  """Constructs a Protocol from its JSON descriptor in text form.

  Args:
    json_string: String representation of the JSON descriptor of the protocol.
  Returns:
    The parsed protocol.
  Raises:
    ProtocolParseException: on JSON parsing error,
        or if the JSON descriptor is invalid.
  """
  try:
    json_data = json.loads(json_string)
  except Exception as exn:
    raise ProtocolParseException(
        'Error parsing protocol from JSON: %r. '
        'Error message: %r.'
        % (json_string, exn))

  return ProtocolFromJSONData(json_data)
