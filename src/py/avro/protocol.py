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

import cStringIO
import md5
# Use simplejson or Python 2.6 json, prefer simplejson.
try:
  import simplejson as json
except ImportError:
  import json

from avro import schema

#The version implemented.
VERSION = 1

_SYSTEM_ERROR = schema._StringSchema()
_SYSTEM_ERRORS = schema._UnionSchema([_SYSTEM_ERROR])

class Protocol(object):
  """A set of messages forming an application protocol."""
  def __init__(self, name=None, namespace=None):
    self.__types = schema._Names()
    self.__messages = dict()
    self.__name = name
    self.__namespace = namespace
    self.__md5 = None

  def getname(self):
    return self.__name

  def getnamespace(self):
    return self.__namespace

  def gettypes(self):
    return self.__types

  def getmessages(self):
    return self.__messages

  def getMD5(self):
    """Return the MD5 hash of the text of this protocol."""
    if self.__md5 is None:
      self.__md5 = md5.new(self.__str__()).digest()
    return self.__md5

  class Message(object):
    """ A Protocol message."""
    def __init__(self, proto, name, request, response, errors):
      self.__proto = proto
      self.__name = name
      self.__request = request
      self.__response = response
      self.__errors = errors

    def getname(self):
      return self.__name

    def getrequest(self):
      return self.__request

    def getresponse(self):
      return self.__response

    def geterrors(self):
      return self.__errors

    def __str__(self):
      str = cStringIO.StringIO()
      str.write("{\"request\": [")
      count = 0
      for field in self.__request.getfields().values():
        str.write("{\"name\": \"")
        str.write(field.getname())
        str.write("\", \"type\": ")
        str.write(field.getschema().str(self.__proto.gettypes()))
        str.write("}")
        count+=1
        if count < len(self.__request.getfields()):
          str.write(", ")
      str.write("], \"response\": "+
                self.__response.str(self.__proto.gettypes()))
      list = self.__errors.getelementtypes()
      if len(list) > 1:
        str.write(",")
        sch = schema._UnionSchema(list.__getslice__(1, len(list)))
        str.write("\"errors\":"+sch.str(self.__proto.gettypes()))
      str.write("}")
      return str.getvalue()

  def __str__(self):
    str = cStringIO.StringIO()
    str.write("{\n")
    str.write("\"protocol\": \""+self.__name+"\", \n")
    str.write("\"namespace\": \""+self.__namespace+"\", \n")
    str.write("\"types\": [\n")
    count = 0
    for type in self.__types.values():
      typesCopy = self.__types
      if isinstance(type, schema.NamedSchema):
        typesCopy = self.__types.copy()
        typesCopy.pop(type.getname(), None)
      str.write(type.str(typesCopy)+"\n")
      count+=1
      if count < len(self.__types):
        str.write(",\n")
    str.write("], \"messages\": {\n")
    count = 0
    for k,v in self.__messages.items():
      str.write("\""+k.__str__()+"\": "+v.__str__())
      count+=1
      if count < len(self.__messages):
        str.write(",\n")
    str.write("}\n}")
    return str.getvalue()

  def _parse(self, obj):
    if obj.get("namespace") is not None:
      self.__namespace = obj.get("namespace")

    if obj.get("protocol") is None:
      raise SchemaParseException("No protocol name specified: "+obj.__str__())
    self.__name = obj.get("protocol")

    defs = obj.get("types")
    if defs is not None:
      if not isinstance(defs, list):
        raise SchemaParseException("Types not an array: "+defs.__str__())
      for type in defs:
        if not isinstance(type, object):
          raise SchemaParseException("Types not an object: "+type.__str__())
        schema._parse(type, self.__types)

    msgs = obj.get("messages")
    if msgs is not None:
      for k,v in msgs.items():
        self.__messages[k] = self.__parse_msg(k,v)

  def __parse_msg(self, msgname, obj):
    req = obj.get("request")
    res = obj.get("response")
    if req is None:
      raise SchemaParseException("No request specified: "+obj.__str__())
    if res is None:
      raise SchemaParseException("No response specified: "+obj.__str__())
    fields = dict()
    for field in req:
      fieldname = field.get("name")
      if fieldname is None:
        raise SchemaParseException("No param name: "+field.__str__())
      fieldtype = field.get("type")
      if fieldtype is None:
        raise SchemaParseException("No param type: "+field.__str__())
      fields[fieldname] = schema.Field(fieldname, 
                                       schema._parse(fieldtype, self.__types))
    request = schema._RecordSchema(fields)
    response = schema._parse(res, self.__types)

    erorrs = list()
    erorrs.append(_SYSTEM_ERROR)
    errs = obj.get("errors")
    if errs is not None:
      if not isinstance(errs, list):
        raise SchemaParseException("Errors not an array: "+errs.__str__())
      for err in errs:
        name = err.__str__()
        sch = self.__types.get(name)
        if sch is None:
          raise SchemaParseException("Undefined error: "+name.__str__())
        if sch.iserror is False:
          raise SchemaParseException("Not an error: "+name.__str__())
        erorrs.append(sch)

    return self.Message(self, msgname, request, response, 
              schema._UnionSchema(erorrs))

def parse(json_string):
  """Constructs the Protocol from the json text."""
  protocol = Protocol()
  protocol._parse(json.loads(json_string))
  return protocol
