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

from avro.schema import *

class AvpySchema():
    @staticmethod
    def fromJson(jsonFile):
        data = ""
        with open(jsonFile) as fp:
            data = fp.read()
        schema = parse(data)
        return AvpySchema.getIdlSchema(schema)
    @staticmethod
    def getIdlSchema(schema, name=None, default=None):
        primitive = lambda: Primitive.fromJson(schema, name, default)
        lambdaValue = {
            'record': lambda: Record.fromJson(schema, name, default),
            'union': lambda: Union.fromJson(schema, name, default),
            'map': lambda: Map.fromJson(schema, name, default),
            'array': lambda: Array.fromJson(schema, name, default),
            'fixed': lambda: Fixed.fromJson(schema, name, default),
            'enum': lambda: Enum.fromJson(schema, name, default)
        }.get(schema.type) or primitive
        return lambdaValue()
    @staticmethod
    def getLevelIndent(level):
        return level * '    '
    @staticmethod
    def toString(value):
        return "'%s'" % value

class Record(AvpySchema):
    def __init__(self, name, namespace, fields, default=None):
        self.name = name
        self.namespace = namespace
        self.fields = fields
        self.default = default
    def toSchema(self):
        return make_avsc_object(self.toJson())
    def toJson(self, named=False):
        name = self.name
        type = {"type": "record", "name": name, "namespace": self.namespace, "fields": [field.toJson(named=True) for field in self.fields]}
        if named:
            return {"name": name, "type": type, "default": self.default}
        else:
            return type
    @staticmethod
    def fromJson(schema, name=None, default=None):
        fields = list()
        for field in schema.fields:
            fields.append(AvpySchema.getIdlSchema(field.type, name=field.name, default=field.default))
        return Record(schema.name, schema.namespace, fields, default=default)
    def toIdlString(self, level=0, named=False):
        indent = AvpySchema.getLevelIndent(level)
        indentp1 = AvpySchema.getLevelIndent(level+1)
        fieldsString = "\n%s[\n" % indentp1
        fieldArray = list()
        for field in self.fields:
            fieldArray.append(field.toIdlString(level+2, named=True))
        fieldsString += ',\n'.join(fieldArray)
        fieldsString += "\n%s]" % indentp1
        defaultString = ""
        if named:
            defaultString = ",\n%sdefault=%s" % (indentp1, json.dumps(self.default))
        return "%sRecord(\n%s%s,\n%s%s,%s%s\n%s)" % (indent, indentp1, AvpySchema.toString(self.name), indentp1, AvpySchema.toString(self.namespace), fieldsString, defaultString, indent)

class Union(AvpySchema):
    def __init__(self, schemas, name=None, default=None):
        self.schemas = schemas
        self.name = name
        self.default = default
    def toSchema(self):
        return make_avsc_object(self.toJson())
    def toJson(self, named=False):
        name = self.name
        type = [schema.toJson() for schema in self.schemas]
        if named:
            return {"name": name, "type": type, "default": self.default}
        else:
            return type
    @staticmethod
    def fromJson(schema, name=None, default=None):
        schemas = list()
        for schema in schema._schemas:
            schemas.append(AvpySchema.getIdlSchema(schema))
        return Union(schemas, name=name, default=default)
    def toIdlString(self, level=0, named=False):
        indent = AvpySchema.getLevelIndent(level)
        indentp1 = AvpySchema.getLevelIndent(level+1)
        schemaArray = list()
        for schema in self.schemas:
            schemaArray.append(schema.toIdlString(level+2))
        schemasString = ',\n'.join(schemaArray) + "\n"
        namedString = ""
        if named:
            namedString = ",\n%sname=%s,\n%sdefault=%s" % (indentp1, AvpySchema.toString(self.name), indentp1, json.dumps(self.default))
        return "%sUnion(\n%s[\n%s%s]%s\n%s)" % (indent, indentp1, schemasString, indentp1, namedString, indent)

class Map(AvpySchema):
    def __init__(self, values, name=None, default=None):
        self.values = values
        self.name = name
        self.default = default
    def toSchema(self):
        return make_avsc_object(self.toJson())
    def toJson(self, named=False):
        type = {"type": "map", "values": self.values.toJson()}
        name = self.name
        if named:
            return {"name": name, "type": type, "default": self.default}
        else:
            return type

    @staticmethod
    def fromJson(schema, name=None, default=None):
        return Map(AvpySchema.getIdlSchema(schema.values), name=name, default=default)
    def toIdlString(self, level=0, named=False):
        indent = AvpySchema.getLevelIndent(level)
        indentp1 = AvpySchema.getLevelIndent(level+1)
        namedString = ""
        if named:
            namedString = ",\n%sname=%s,\n%sdefault=%s" % (indentp1, AvpySchema.toString(self.name), indentp1, json.dumps(self.default))
        return "%sMap(\n%s%s\n%s)" % (indent, self.values.toIdlString(level+1), namedString, indent)

class Array(AvpySchema):
    def __init__(self, items, name=None, default=None):
        self.fields = dict()
        self.fields["items"] = items
        self.fields["name"] = name
        self.default = default
    def toSchema(self):
        return make_avsc_object(self.toJson())
    def toJson(self, named=False):
        type = {"type": "array", "items": self.fields["items"].toJson()}
        name = self.fields["name"]
        if named:
            return {"name": name, "type": type, "default": self.default}
        else:
            return type
    @staticmethod
    def fromJson(schema, name=None, default=None):
        return Array(AvpySchema.getIdlSchema(schema.items), name=name, default=default)
    def toIdlString(self, level=0, named=False):
        indent = AvpySchema.getLevelIndent(level)
        indentp1 = AvpySchema.getLevelIndent(level+1)
        namedString = ""
        if named:
            namedString = ",\n%sname=%s,\n%sdefault=%s" % (indentp1, AvpySchema.toString(self.fields["name"]), indentp1, json.dumps(self.default))
        return "%sArray(\n%s%s\n%s)" % (indent, self.fields["items"].toIdlString(level+1), namedString, indent)

class Primitive(AvpySchema):
    def __init__(self, type, name=None, default=None):
        self.fields = dict()
        self.fields["type"] = type
        self.fields["name"] = name
        self.default = default
    def toSchema(self):
        return make_avsc_object(self.toJson())
    def toJson(self, named=False):
        type = self.fields["type"]
        name = self.fields["name"]
        if named:
            return {"name": name, "type": type, "default": self.default}
        else:
            return type
    @staticmethod
    def fromJson(schema, name=None, default=None):
        return Primitive(schema.type, name=name, default=default)
    def toIdlString(self, level=0, named=False):
        indent = AvpySchema.getLevelIndent(level)
        indentp1 = AvpySchema.getLevelIndent(level+1)
        namedString = ""
        if named:
            namedString = ",\n%sname=%s,\n%sdefault=%s" % (indentp1, AvpySchema.toString(self.fields["name"]), indentp1, json.dumps(self.default))
        return "%sPrimitive(\n%s%s%s\n%s)" % (indent, indentp1, AvpySchema.toString(self.fields["type"]), namedString, indent)

class Fixed(AvpySchema):
    def __init__(self, name, namespace, size, default=None):
        self.fields = dict()
        self.fields["name"] = name
        self.fields["namespace"] = namespace
        self.fields["size"] = size
        self.fields["type"] = "fixed"
        self.default = default
    def toSchema(self):
        return make_avsc_object(self.toJson())
    def toJson(self, named=False):
        if named:
            return {"name": self.fields["name"], "type": self.fields, "default": self.default}
        else:
            return self.fields

    @staticmethod
    def fromJson(schema, name=None, default=None):
        return Fixed(schema.name, schema.namespace, schema.size, default=default)
    def toIdlString(self, level=0, named=False):
        indent = AvpySchema.getLevelIndent(level)
        indentp1 = AvpySchema.getLevelIndent(level+1)
        defaultString = ""
        if named:
            defaultString = ",\n%sdefault=%s" % (indentp1, json.dumps(self.default))
        return "%sFixed(\n%s%s,\n%s%s,\n%s%s%s\n%s)" % (indent, indentp1, AvpySchema.toString(self.fields["name"]), indentp1, AvpySchema.toString(self.fields["namespace"]), indentp1, self.fields["size"], defaultString, indent)

class Enum(AvpySchema):
    def __init__(self, name, namespace, symbols, default=None):
        self.fields = dict()
        self.fields["name"] = name
        self.fields["namespace"] = namespace
        self.fields["symbols"] = symbols
        self.fields["type"] = "enum"
        self.default = default
    def toSchema(self):
        return make_avsc_object(self.toJson())
    def toJson(self, named=False):
        if named:
            return {"name": self.fields["name"], "type": self.fields, "default": self.default}
        else:
            return self.fields

    @staticmethod
    def fromJson(schema, name=None, default=None):
        return Enum(schema.name, schema.namespace, schema.symbols, default=default)
    def toIdlString(self, level=0, named=False):
        indent = AvpySchema.getLevelIndent(level)
        indentp1 = AvpySchema.getLevelIndent(level+1)
        defaultString = ""
        if named:
            defaultString = ",\n%sdefault=%s" % (indentp1, json.dumps(self.default))
        return "%sEnum(\n%s%s,\n%s%s,\n%s%s%s\n%s)" % (indent, indentp1, AvpySchema.toString(self.fields["name"]), indentp1, AvpySchema.toString(self.fields["namespace"]), indentp1, self.fields["symbols"], defaultString, indent)

