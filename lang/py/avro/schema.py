#!/usr/bin/env python3

##
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

import abc
import collections
import datetime
import decimal
import json
import math
import re
import sys
import uuid
import warnings
from typing import List, Sequence, cast

import avro.constants
import avro.errors

#
# Constants
#

# The name portion of a fullname, record field names, and enum symbols must:
# start with [A-Za-z_]
# subsequently contain only [A-Za-z0-9_]
_BASE_NAME_PATTERN = re.compile(r"(?:^|\.)[A-Za-z_][A-Za-z0-9_]*$")

PRIMITIVE_TYPES = (
    "null",
    "boolean",
    "string",
    "bytes",
    "int",
    "long",
    "float",
    "double",
)

NAMED_TYPES = (
    "fixed",
    "enum",
    "record",
    "error",
)

VALID_TYPES = PRIMITIVE_TYPES + NAMED_TYPES + ("array", "map", "union", "request", "error_union")

SCHEMA_RESERVED_PROPS = (
    "type",
    "name",
    "namespace",
    "fields",  # Record
    "items",  # Array
    "size",  # Fixed
    "symbols",  # Enum
    "values",  # Map
    "doc",
)

FIELD_RESERVED_PROPS = (
    "default",
    "name",
    "doc",
    "order",
    "type",
)

VALID_FIELD_SORT_ORDERS = (
    "ascending",
    "descending",
    "ignore",
)

CANONICAL_FIELD_ORDER = (
    "name",
    "type",
    "fields",
    "symbols",
    "items",
    "values",
    "size",
)

INT_MIN_VALUE = -(1 << 31)
INT_MAX_VALUE = (1 << 31) - 1
LONG_MIN_VALUE = -(1 << 63)
LONG_MAX_VALUE = (1 << 63) - 1


def validate_basename(basename):
    """Raise InvalidName if the given basename is not a valid name."""
    if not _BASE_NAME_PATTERN.search(basename):
        raise avro.errors.InvalidName(f"{basename!s} is not a valid Avro name because it does not match the pattern {_BASE_NAME_PATTERN.pattern!s}")


def _is_timezone_aware_datetime(dt):
    return dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None


#
# Base Classes
#


class EqualByJsonMixin:
    """Equal if the json serializations are equal."""

    def __eq__(self, that):
        try:
            that_str = json.loads(str(that))
        except json.decoder.JSONDecodeError:
            return False
        return json.loads(str(self)) == that_str


class EqualByPropsMixin:
    """Equal if the props are equal."""

    def __eq__(self, that):
        try:
            return self.props == that.props
        except AttributeError:
            return False


class CanonicalPropertiesMixin:
    """A Mixin that provides canonical properties to Schema and Field types."""

    @property
    def canonical_properties(self):
        props = self.props
        return collections.OrderedDict((key, props[key]) for key in CANONICAL_FIELD_ORDER if key in props)


class Schema(abc.ABC, CanonicalPropertiesMixin):
    """Base class for all Schema classes."""

    _props = None

    def __init__(self, type, other_props=None):
        # Ensure valid ctor args
        if not isinstance(type, str):
            fail_msg = "Schema type must be a string."
            raise avro.errors.SchemaParseException(fail_msg)
        elif type not in VALID_TYPES:
            fail_msg = f"{type} is not a valid type."
            raise avro.errors.SchemaParseException(fail_msg)

        # add members
        if self._props is None:
            self._props = {}
        self.set_prop("type", type)
        self.type = type
        self._props.update(other_props or {})

    @property
    def props(self):
        return self._props

    @property
    def other_props(self):
        """Dictionary of non-reserved properties"""
        return get_other_props(self.props, SCHEMA_RESERVED_PROPS)

    def check_props(self, other, props):
        """Check that the given props are identical in two schemas.

        @arg other: The other schema to check
        @arg props: An iterable of properties to check
        @return bool: True if all the properties match
        """
        return all(getattr(self, prop) == getattr(other, prop) for prop in props)

    @abc.abstractmethod
    def match(self, writer):
        """Return True if the current schema (as reader) matches the writer schema.

        @arg writer: the writer schema to match against.
        @return bool
        """

    # utility functions to manipulate properties dict
    def get_prop(self, key):
        return self._props.get(key)

    def set_prop(self, key, value):
        self._props[key] = value

    def __str__(self):
        return json.dumps(self.to_json())

    @abc.abstractmethod
    def to_json(self, names):
        """
        Converts the schema object into its AVRO specification representation.

        Schema types that have names (records, enums, and fixed) must
        be aware of not re-defining schemas that are already listed
        in the parameter names.
        """

    @abc.abstractmethod
    def validate(self, datum):
        """Returns the appropriate schema object if datum is valid for that schema, else None.

        To be implemented in subclasses.

        Validation concerns only shape and type of data in the top level of the current schema.
        In most cases, the returned schema object will be self. However, for UnionSchema objects,
        the returned Schema will be the first branch schema for which validation passes.

        @arg datum: The data to be checked for validity according to this schema
        @return Optional[Schema]
        """

    @abc.abstractmethod
    def to_canonical_json(self, names=None):
        """
        Converts the schema object into its Canonical Form
        http://avro.apache.org/docs/current/spec.html#Parsing+Canonical+Form+for+Schemas

        To be implemented in subclasses.
        """

    @property
    def canonical_form(self):
        # The separators eliminate whitespace around commas and colons.
        return json.dumps(self.to_canonical_json(), separators=(",", ":"))

    @abc.abstractmethod
    def __eq__(self, that):
        """
        Determines how two schema are compared.
        Consider the mixins EqualByPropsMixin and EqualByJsonMixin
        """


class Name:
    """Class to describe Avro name."""

    _full = None

    def __init__(self, name_attr, space_attr, default_space):
        """The fullname is determined in one of the following ways:

        - A name and namespace are both specified. For example, one might use "name": "X",
            "namespace": "org.foo" to indicate the fullname org.foo.X.
        - A fullname is specified. If the name specified contains a dot,
            then it is assumed to be a fullname, and any namespace also specified is ignored.
            For example, use "name": "org.foo.X" to indicate the fullname org.foo.X.
        - A name only is specified, i.e., a name that contains no dots.
            In this case the namespace is taken from the most tightly enclosing schema or protocol.
            For example, if "name": "X" is specified, and this occurs within a field of
            the record definition of org.foo.Y, then the fullname is org.foo.X.
            If there is no enclosing namespace then the null namespace is used.

        References to previously defined names are as in the latter two cases above:
        if they contain a dot they are a fullname,
        if they do not contain a dot, the namespace is the namespace of the enclosing definition.

        @arg name_attr: name value read in schema or None.
        @arg space_attr: namespace value read in schema or None. The empty string may be used as a namespace
            to indicate the null namespace.
        @arg default_space: the current default space or None.
        """
        if name_attr is None:
            return
        if name_attr == "":
            raise avro.errors.SchemaParseException("Name must not be the empty string.")

        if "." in name_attr or space_attr == "" or not (space_attr or default_space):
            # The empty string may be used as a namespace to indicate the null namespace.
            self._full = name_attr
        else:
            self._full = f"{space_attr or default_space!s}.{name_attr!s}"

        self._validate_fullname(self._full)

    def _validate_fullname(self, fullname):
        for name in fullname.split("."):
            validate_basename(name)

    def __eq__(self, other):
        """Equality of names is defined on the fullname and is case-sensitive."""
        try:
            return self.fullname == other.fullname
        except AttributeError:
            return False

    @property
    def fullname(self):
        return self._full

    @property
    def space(self):
        """Back out a namespace from full name."""
        if self._full is None:
            return None
        return self._full.rsplit(".", 1)[0] if "." in self._full else None

    def get_space(self):
        warnings.warn("Name.get_space() is deprecated in favor of Name.space")
        return self.space


class Names:
    """Track name set and default namespace during parsing."""

    def __init__(self, default_namespace=None):
        self.names = {}
        self.default_namespace = default_namespace

    def has_name(self, name_attr, space_attr):
        test = Name(name_attr, space_attr, self.default_namespace).fullname
        return test in self.names

    def get_name(self, name_attr, space_attr):
        test = Name(name_attr, space_attr, self.default_namespace).fullname
        return self.names.get(test)

    def prune_namespace(self, properties):
        """given a properties, return properties with namespace removed if
        it matches the own default namespace"""
        if self.default_namespace is None:
            # I have no default -- no change
            return properties

        if "namespace" not in properties:
            # he has no namespace - no change
            return properties

        if properties["namespace"] != self.default_namespace:
            # we're different - leave his stuff alone
            return properties

        # we each have a namespace and it's redundant. delete his.
        prunable = properties.copy()
        del prunable["namespace"]
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
            fail_msg = f"{to_add.fullname} is a reserved type name."
            raise avro.errors.SchemaParseException(fail_msg)
        elif to_add.fullname in self.names:
            fail_msg = f'The name "{to_add.fullname}" is already in use.'
            raise avro.errors.SchemaParseException(fail_msg)

        self.names[to_add.fullname] = new_schema
        return to_add


class NamedSchema(Schema):
    """Named Schemas specified in NAMED_TYPES."""

    def __init__(self, type, name, namespace=None, names=None, other_props=None):
        # Ensure valid ctor args
        if not name:
            fail_msg = "Named Schemas must have a non-empty name."
            raise avro.errors.SchemaParseException(fail_msg)
        elif not isinstance(name, str):
            fail_msg = "The name property must be a string."
            raise avro.errors.SchemaParseException(fail_msg)
        elif namespace is not None and not isinstance(namespace, str):
            fail_msg = "The namespace property must be a string."
            raise avro.errors.SchemaParseException(fail_msg)

        namespace = namespace or None  # Empty string -> None

        # Call parent ctor
        Schema.__init__(self, type, other_props)

        # Add class members
        new_name = names.add_name(name, namespace, self)

        # Store name and namespace as they were read in origin schema
        self.set_prop("name", name)
        if namespace is not None:
            self.set_prop("namespace", new_name.space)

        # Store full name as calculated from name, namespace
        self._fullname = new_name.fullname

    def name_ref(self, names):
        return self.name if self.namespace == names.default_namespace else self.fullname

    # read-only properties
    name = property(lambda self: self.get_prop("name"))
    namespace = property(lambda self: self.get_prop("namespace"))
    fullname = property(lambda self: self._fullname)


#
# Logical type class
#


class LogicalSchema:
    def __init__(self, logical_type):
        self.logical_type = logical_type


#
# Decimal logical schema
#


class DecimalLogicalSchema(LogicalSchema):
    def __init__(self, precision, scale=0, max_precision=0):
        if not isinstance(precision, int) or precision <= 0:
            raise avro.errors.IgnoredLogicalType(f"Invalid decimal precision {precision}. Must be a positive integer.")

        if precision > max_precision:
            raise avro.errors.IgnoredLogicalType(f"Invalid decimal precision {precision}. Max is {max_precision}.")

        if not isinstance(scale, int) or scale < 0:
            raise avro.errors.IgnoredLogicalType(f"Invalid decimal scale {scale}. Must be a positive integer.")

        if scale > precision:
            raise avro.errors.IgnoredLogicalType(f"Invalid decimal scale {scale}. Cannot be greater than precision {precision}.")

        super().__init__("decimal")


class Field(CanonicalPropertiesMixin, EqualByJsonMixin):
    def __init__(
        self,
        type,
        name,
        has_default,
        default=None,
        order=None,
        names=None,
        doc=None,
        other_props=None,
    ):
        # Ensure valid ctor args
        if not name:
            fail_msg = "Fields must have a non-empty name."
            raise avro.errors.SchemaParseException(fail_msg)
        elif not isinstance(name, str):
            fail_msg = "The name property must be a string."
            raise avro.errors.SchemaParseException(fail_msg)
        elif order is not None and order not in VALID_FIELD_SORT_ORDERS:
            fail_msg = f"The order property {order} is not valid."
            raise avro.errors.SchemaParseException(fail_msg)

        # add members
        self._props = {}
        self._has_default = has_default
        self._props.update(other_props or {})

        if isinstance(type, str) and names is not None and names.has_name(type, None):
            type_schema = names.get_name(type, None)
        else:
            try:
                type_schema = make_avsc_object(type, names)
            except Exception as e:
                fail_msg = f'Type property "{type}" not a valid Avro schema: {e}'
                raise avro.errors.SchemaParseException(fail_msg)
        self.set_prop("type", type_schema)
        self.set_prop("name", name)
        self.type = type_schema
        self.name = name
        # TODO(hammer): check to ensure default is valid
        if has_default:
            self.set_prop("default", default)
        if order is not None:
            self.set_prop("order", order)
        if doc is not None:
            self.set_prop("doc", doc)

    # read-only properties
    default = property(lambda self: self.get_prop("default"))
    has_default = property(lambda self: self._has_default)
    order = property(lambda self: self.get_prop("order"))
    doc = property(lambda self: self.get_prop("doc"))
    props = property(lambda self: self._props)

    # Read-only property dict. Non-reserved properties
    other_props = property(
        lambda self: get_other_props(self._props, FIELD_RESERVED_PROPS),
        doc="dictionary of non-reserved properties",
    )

    # utility functions to manipulate properties dict
    def get_prop(self, key):
        return self._props.get(key)

    def set_prop(self, key, value):
        self._props[key] = value

    def __str__(self):
        return json.dumps(self.to_json())

    def to_json(self, names=None):
        names = names or Names()

        to_dump = self.props.copy()
        to_dump["type"] = self.type.to_json(names)

        return to_dump

    def to_canonical_json(self, names=None):
        names = names or Names()

        to_dump = self.canonical_properties
        to_dump["type"] = self.type.to_canonical_json(names)

        return to_dump


#
# Primitive Types
#


class PrimitiveSchema(EqualByPropsMixin, Schema):
    """Valid primitive types are in PRIMITIVE_TYPES."""

    _validators = {
        "null": lambda x: x is None,
        "boolean": lambda x: isinstance(x, bool),
        "string": lambda x: isinstance(x, str),
        "bytes": lambda x: isinstance(x, bytes),
        "int": lambda x: isinstance(x, int) and INT_MIN_VALUE <= x <= INT_MAX_VALUE,
        "long": lambda x: isinstance(x, int) and LONG_MIN_VALUE <= x <= LONG_MAX_VALUE,
        "float": lambda x: isinstance(x, (int, float)),
        "double": lambda x: isinstance(x, (int, float)),
    }

    def __init__(self, type, other_props=None):
        # Ensure valid ctor args
        if type not in PRIMITIVE_TYPES:
            raise avro.errors.AvroException(f"{type} is not a valid primitive type.")

        # Call parent ctor
        Schema.__init__(self, type, other_props=other_props)

        self.fullname = type

    def match(self, writer):
        """Return True if the current schema (as reader) matches the writer schema.

        @arg writer: the schema to match against
        @return bool
        """
        return (
            self.type == writer.type
            or {
                "float": self.type == "double",
                "int": self.type in {"double", "float", "long"},
                "long": self.type
                in {
                    "double",
                    "float",
                },
            }.get(writer.type, False)
        )

    def to_json(self, names=None):
        if len(self.props) == 1:
            return self.fullname
        else:
            return self.props

    def to_canonical_json(self, names=None):
        return self.fullname if len(self.props) == 1 else self.canonical_properties

    def validate(self, datum):
        """Return self if datum is a valid representation of this type of primitive schema, else None

        @arg datum: The data to be checked for validity according to this schema
        @return Schema object or None
        """
        validator = self._validators.get(self.type, lambda x: False)
        return self if validator(datum) else None


#
# Decimal Bytes Type
#


class BytesDecimalSchema(PrimitiveSchema, DecimalLogicalSchema):
    def __init__(self, precision, scale=0, other_props=None):
        DecimalLogicalSchema.__init__(self, precision, scale, max_precision=((1 << 31) - 1))
        PrimitiveSchema.__init__(self, "bytes", other_props)
        self.set_prop("precision", precision)
        self.set_prop("scale", scale)

    # read-only properties
    precision = property(lambda self: self.get_prop("precision"))
    scale = property(lambda self: self.get_prop("scale"))

    def to_json(self, names=None):
        return self.props

    def validate(self, datum):
        """Return self if datum is a Decimal object, else None."""
        return self if isinstance(datum, decimal.Decimal) else None


#
# Complex Types (non-recursive)
#
class FixedSchema(EqualByPropsMixin, NamedSchema):
    def __init__(self, name, namespace, size, names=None, other_props=None):
        # Ensure valid ctor args
        if not isinstance(size, int) or size < 0:
            fail_msg = "Fixed Schema requires a valid positive integer for size property."
            raise avro.errors.AvroException(fail_msg)

        # Call parent ctor
        NamedSchema.__init__(self, "fixed", name, namespace, names, other_props)

        # Add class members
        self.set_prop("size", size)

    # read-only properties
    size = property(lambda self: self.get_prop("size"))

    def match(self, writer):
        """Return True if the current schema (as reader) matches the writer schema.

        @arg writer: the schema to match against
        @return bool
        """
        return self.type == writer.type and self.check_props(writer, ["fullname", "size"])

    def to_json(self, names=None):
        names = names or Names()

        if self.fullname in names.names:
            return self.name_ref(names)

        names.names[self.fullname] = self
        return names.prune_namespace(self.props)

    def to_canonical_json(self, names=None):
        to_dump = self.canonical_properties
        to_dump["name"] = self.fullname

        return to_dump

    def validate(self, datum):
        """Return self if datum is a valid representation of this schema, else None."""
        return self if isinstance(datum, bytes) and len(datum) == self.size else None


#
# Decimal Fixed Type
#


class FixedDecimalSchema(FixedSchema, DecimalLogicalSchema):
    def __init__(
        self,
        size,
        name,
        precision,
        scale=0,
        namespace=None,
        names=None,
        other_props=None,
    ):
        max_precision = int(math.floor(math.log10(2) * (8 * size - 1)))
        DecimalLogicalSchema.__init__(self, precision, scale, max_precision)
        FixedSchema.__init__(self, name, namespace, size, names, other_props)
        self.set_prop("precision", precision)
        self.set_prop("scale", scale)

    # read-only properties
    precision = property(lambda self: self.get_prop("precision"))
    scale = property(lambda self: self.get_prop("scale"))

    def to_json(self, names=None):
        return self.props

    def validate(self, datum):
        """Return self if datum is a Decimal object, else None."""
        return self if isinstance(datum, decimal.Decimal) else None


class EnumSchema(EqualByPropsMixin, NamedSchema):
    def __init__(
        self,
        name,
        namespace,
        symbols,
        names=None,
        doc=None,
        other_props=None,
        validate_enum_symbols=True,
    ):
        """
        @arg validate_enum_symbols: If False, will allow enum symbols that are not valid Avro names.
        """
        if validate_enum_symbols:
            for symbol in symbols:
                try:
                    validate_basename(symbol)
                except avro.errors.InvalidName:
                    raise avro.errors.InvalidName("An enum symbol must be a valid schema name.")

        if len(set(symbols)) < len(symbols):
            fail_msg = f"Duplicate symbol: {symbols}"
            raise avro.errors.AvroException(fail_msg)

        # Call parent ctor
        NamedSchema.__init__(self, "enum", name, namespace, names, other_props)

        # Add class members
        self.set_prop("symbols", symbols)
        if doc is not None:
            self.set_prop("doc", doc)

    # read-only properties
    @property
    def symbols(self) -> Sequence[str]:
        symbols = self.get_prop("symbols")
        return cast(List[str], symbols)

    doc = property(lambda self: self.get_prop("doc"))

    def match(self, writer):
        """Return True if the current schema (as reader) matches the writer schema.

        @arg writer: the schema to match against
        @return bool
        """
        return self.type == writer.type and self.check_props(writer, ["fullname"])

    def to_json(self, names=None):
        names = names or Names()

        if self.fullname in names.names:
            return self.name_ref(names)

        names.names[self.fullname] = self
        return names.prune_namespace(self.props)

    def to_canonical_json(self, names=None):
        names_as_json = self.to_json(names)

        if isinstance(names_as_json, str):
            to_dump = self.fullname
        else:
            to_dump = self.canonical_properties
            to_dump["name"] = self.fullname

        return to_dump

    def validate(self, datum):
        """Return self if datum is a valid member of this Enum, else None."""
        return self if datum in self.symbols else None


#
# Complex Types (recursive)
#


class ArraySchema(EqualByJsonMixin, Schema):
    def __init__(self, items, names=None, other_props=None):
        # Call parent ctor
        Schema.__init__(self, "array", other_props)
        # Add class members

        if isinstance(items, str) and names.has_name(items, None):
            items_schema = names.get_name(items, None)
        else:
            try:
                items_schema = make_avsc_object(items, names)
            except avro.errors.SchemaParseException as e:
                fail_msg = f"Items schema ({items}) not a valid Avro schema: {e} (known names: {names.names.keys()})"
                raise avro.errors.SchemaParseException(fail_msg)

        self.set_prop("items", items_schema)

    # read-only properties
    items = property(lambda self: self.get_prop("items"))

    def match(self, writer):
        """Return True if the current schema (as reader) matches the writer schema.

        @arg writer: the schema to match against
        @return bool
        """
        return self.type == writer.type and self.items.check_props(writer.items, ["type"])

    def to_json(self, names=None):
        names = names or Names()

        to_dump = self.props.copy()
        item_schema = self.get_prop("items")
        to_dump["items"] = item_schema.to_json(names)

        return to_dump

    def to_canonical_json(self, names=None):
        names = names or Names()

        to_dump = self.canonical_properties
        item_schema = self.get_prop("items")
        to_dump["items"] = item_schema.to_canonical_json(names)

        return to_dump

    def validate(self, datum):
        """Return self if datum is a valid representation of this schema, else None."""
        return self if isinstance(datum, list) else None


class MapSchema(EqualByJsonMixin, Schema):
    def __init__(self, values, names=None, other_props=None):
        # Call parent ctor
        Schema.__init__(self, "map", other_props)

        # Add class members
        if isinstance(values, str) and names.has_name(values, None):
            values_schema = names.get_name(values, None)
        else:
            try:
                values_schema = make_avsc_object(values, names)
            except avro.errors.SchemaParseException:
                raise
            except Exception:
                raise avro.errors.SchemaParseException("Values schema is not a valid Avro schema.")

        self.set_prop("values", values_schema)

    # read-only properties
    values = property(lambda self: self.get_prop("values"))

    def match(self, writer):
        """Return True if the current schema (as reader) matches the writer schema.

        @arg writer: the schema to match against
        @return bool
        """
        return writer.type == self.type and self.values.check_props(writer.values, ["type"])

    def to_json(self, names=None):
        names = names or Names()

        to_dump = self.props.copy()
        to_dump["values"] = self.get_prop("values").to_json(names)

        return to_dump

    def to_canonical_json(self, names=None):
        names = names or Names()

        to_dump = self.canonical_properties
        to_dump["values"] = self.get_prop("values").to_canonical_json(names)

        return to_dump

    def validate(self, datum):
        """Return self if datum is a valid representation of this schema, else None."""
        return self if isinstance(datum, dict) and all(isinstance(key, str) for key in datum) else None


class UnionSchema(EqualByJsonMixin, Schema):
    """
    names is a dictionary of schema objects
    """

    def __init__(self, schemas, names=None):
        # Ensure valid ctor args
        if not isinstance(schemas, list):
            fail_msg = "Union schema requires a list of schemas."
            raise avro.errors.SchemaParseException(fail_msg)

        # Call parent ctor
        Schema.__init__(self, "union")

        # Add class members
        schema_objects = []
        for schema in schemas:
            if isinstance(schema, str) and names.has_name(schema, None):
                new_schema = names.get_name(schema, None)
            else:
                try:
                    new_schema = make_avsc_object(schema, names)
                except Exception as e:
                    raise avro.errors.SchemaParseException(f"Union item must be a valid Avro schema: {e}")
            # check the new schema
            if (
                new_schema.type in VALID_TYPES
                and new_schema.type not in NAMED_TYPES
                and new_schema.type in [schema.type for schema in schema_objects]
            ):
                raise avro.errors.SchemaParseException(f"{new_schema.type} type already in Union")
            elif new_schema.type == "union":
                raise avro.errors.SchemaParseException("Unions cannot contain other unions.")
            else:
                schema_objects.append(new_schema)
        self._schemas = schema_objects

    # read-only properties
    schemas = property(lambda self: self._schemas)

    def match(self, writer):
        """Return True if the current schema (as reader) matches the writer schema.

        @arg writer: the schema to match against
        @return bool
        """
        return writer.type in {"union", "error_union"} or any(s.match(writer) for s in self.schemas)

    def to_json(self, names=None):
        names = names or Names()

        to_dump = []
        for schema in self.schemas:
            to_dump.append(schema.to_json(names))

        return to_dump

    def to_canonical_json(self, names=None):
        names = names or Names()

        return [schema.to_canonical_json(names) for schema in self.schemas]

    def validate(self, datum):
        """Return the first branch schema of which datum is a valid example, else None."""
        for branch in self.schemas:
            if branch.validate(datum) is not None:
                return branch


class ErrorUnionSchema(UnionSchema):
    def __init__(self, schemas, names=None):
        # Prepend "string" to handle system errors
        UnionSchema.__init__(self, ["string"] + schemas, names)

    def to_json(self, names=None):
        names = names or Names()

        to_dump = []
        for schema in self.schemas:
            # Don't print the system error schema
            if schema.type == "string":
                continue
            to_dump.append(schema.to_json(names))

        return to_dump


class RecordSchema(EqualByJsonMixin, NamedSchema):
    @staticmethod
    def make_field_objects(field_data, names):
        """We're going to need to make message parameters too."""
        field_objects = []
        field_names = []
        for i, field in enumerate(field_data):
            if callable(getattr(field, "get", None)):
                type = field.get("type")
                name = field.get("name")

                # null values can have a default value of None
                has_default = False
                default = None
                if "default" in field:
                    has_default = True
                    default = field.get("default")

                order = field.get("order")
                doc = field.get("doc")
                other_props = get_other_props(field, FIELD_RESERVED_PROPS)
                new_field = Field(type, name, has_default, default, order, names, doc, other_props)
                # make sure field name has not been used yet
                if new_field.name in field_names:
                    fail_msg = f"Field name {new_field.name} already in use."
                    raise avro.errors.SchemaParseException(fail_msg)
                field_names.append(new_field.name)
            else:
                raise avro.errors.SchemaParseException(f"Not a valid field: {field}")
            field_objects.append(new_field)
        return field_objects

    def match(self, writer):
        """Return True if the current schema (as reader) matches the other schema.

        @arg writer: the schema to match against
        @return bool
        """
        return writer.type == self.type and (self.type == "request" or self.check_props(writer, ["fullname"]))

    def __init__(
        self,
        name,
        namespace,
        fields,
        names=None,
        schema_type="record",
        doc=None,
        other_props=None,
    ):
        # Ensure valid ctor args
        if fields is None:
            fail_msg = "Record schema requires a non-empty fields property."
            raise avro.errors.SchemaParseException(fail_msg)
        elif not isinstance(fields, list):
            fail_msg = "Fields property must be a list of Avro schemas."
            raise avro.errors.SchemaParseException(fail_msg)

        # Call parent ctor (adds own name to namespace, too)
        if schema_type == "request":
            Schema.__init__(self, schema_type, other_props)
        else:
            NamedSchema.__init__(self, schema_type, name, namespace, names, other_props)

        if schema_type == "record":
            old_default = names.default_namespace
            names.default_namespace = Name(name, namespace, names.default_namespace).space

        # Add class members
        field_objects = RecordSchema.make_field_objects(fields, names)
        self.set_prop("fields", field_objects)
        if doc is not None:
            self.set_prop("doc", doc)

        if schema_type == "record":
            names.default_namespace = old_default

    # read-only properties
    fields = property(lambda self: self.get_prop("fields"))
    doc = property(lambda self: self.get_prop("doc"))

    @property
    def fields_dict(self):
        fields_dict = {}
        for field in self.fields:
            fields_dict[field.name] = field
        return fields_dict

    def to_json(self, names=None):
        names = names or Names()

        # Request records don't have names
        if self.type == "request":
            return [f.to_json(names) for f in self.fields]

        if self.fullname in names.names:
            return self.name_ref(names)
        else:
            names.names[self.fullname] = self

        to_dump = names.prune_namespace(self.props.copy())
        to_dump["fields"] = [f.to_json(names) for f in self.fields]

        return to_dump

    def to_canonical_json(self, names=None):
        names = names or Names()

        if self.type == "request":
            raise NotImplementedError("Canonical form (probably) does not make sense on type request")

        to_dump = self.canonical_properties
        to_dump["name"] = self.fullname

        if self.fullname in names.names:
            return self.name_ref(names)

        names.names[self.fullname] = self
        to_dump["fields"] = [f.to_canonical_json(names) for f in self.fields]

        return to_dump

    def validate(self, datum):
        """Return self if datum is a valid representation of this schema, else None"""
        return self if isinstance(datum, dict) and {f.name for f in self.fields}.issuperset(datum.keys()) else None


#
# Date Type
#


class DateSchema(LogicalSchema, PrimitiveSchema):
    def __init__(self, other_props=None):
        LogicalSchema.__init__(self, avro.constants.DATE)
        PrimitiveSchema.__init__(self, "int", other_props)

    def to_json(self, names=None):
        return self.props

    def validate(self, datum):
        """Return self if datum is a valid date object, else None."""
        return self if isinstance(datum, datetime.date) else None


#
# time-millis Type
#


class TimeMillisSchema(LogicalSchema, PrimitiveSchema):
    def __init__(self, other_props=None):
        LogicalSchema.__init__(self, avro.constants.TIME_MILLIS)
        PrimitiveSchema.__init__(self, "int", other_props)

    def to_json(self, names=None):
        return self.props

    def validate(self, datum):
        """Return self if datum is a valid representation of this schema, else None."""
        return self if isinstance(datum, datetime.time) else None


#
# time-micros Type
#


class TimeMicrosSchema(LogicalSchema, PrimitiveSchema):
    def __init__(self, other_props=None):
        LogicalSchema.__init__(self, avro.constants.TIME_MICROS)
        PrimitiveSchema.__init__(self, "long", other_props)

    def to_json(self, names=None):
        return self.props

    def validate(self, datum):
        """Return self if datum is a valid representation of this schema, else None."""
        return self if isinstance(datum, datetime.time) else None


#
# timestamp-millis Type
#


class TimestampMillisSchema(LogicalSchema, PrimitiveSchema):
    def __init__(self, other_props=None):
        LogicalSchema.__init__(self, avro.constants.TIMESTAMP_MILLIS)
        PrimitiveSchema.__init__(self, "long", other_props)

    def to_json(self, names=None):
        return self.props

    def validate(self, datum):
        return self if isinstance(datum, datetime.datetime) and _is_timezone_aware_datetime(datum) else None


#
# timestamp-micros Type
#


class TimestampMicrosSchema(LogicalSchema, PrimitiveSchema):
    def __init__(self, other_props=None):
        LogicalSchema.__init__(self, avro.constants.TIMESTAMP_MICROS)
        PrimitiveSchema.__init__(self, "long", other_props)

    def to_json(self, names=None):
        return self.props

    def validate(self, datum):
        return self if isinstance(datum, datetime.datetime) and _is_timezone_aware_datetime(datum) else None


#
# uuid Type
#


class UUIDSchema(LogicalSchema, PrimitiveSchema):
    def __init__(self, other_props=None):
        LogicalSchema.__init__(self, avro.constants.UUID)
        PrimitiveSchema.__init__(self, "string", other_props)

    def to_json(self, names=None):
        return self.props

    def validate(self, datum):
        try:
            val = uuid.UUID(datum)
        except ValueError:
            # If it's a value error, then the string
            # is not a valid hex code for a UUID.
            return None

        return self


#
# Module Methods
#


def get_other_props(all_props, reserved_props):
    """
    Retrieve the non-reserved properties from a dictionary of properties
    @args reserved_props: The set of reserved properties to exclude
    """
    if callable(getattr(all_props, "items", None)):
        return {k: v for k, v in all_props.items() if k not in reserved_props}


def make_bytes_decimal_schema(other_props):
    """Make a BytesDecimalSchema from just other_props."""
    return BytesDecimalSchema(other_props.get("precision"), other_props.get("scale", 0))


def make_logical_schema(logical_type, type_, other_props):
    """Map the logical types to the appropriate literal type and schema class."""
    logical_types = {
        (avro.constants.DATE, "int"): DateSchema,
        (avro.constants.DECIMAL, "bytes"): make_bytes_decimal_schema,
        # The fixed decimal schema is handled later by returning None now.
        (avro.constants.DECIMAL, "fixed"): lambda x: None,
        (avro.constants.TIMESTAMP_MICROS, "long"): TimestampMicrosSchema,
        (avro.constants.TIMESTAMP_MILLIS, "long"): TimestampMillisSchema,
        (avro.constants.TIME_MICROS, "long"): TimeMicrosSchema,
        (avro.constants.TIME_MILLIS, "int"): TimeMillisSchema,
        (avro.constants.UUID, "string"): UUIDSchema,
    }
    try:
        schema_type = logical_types.get((logical_type, type_), None)
        if schema_type is not None:
            return schema_type(other_props)

        expected_types = sorted(literal_type for lt, literal_type in logical_types if lt == logical_type)
        if expected_types:
            warnings.warn(
                avro.errors.IgnoredLogicalType(f"Logical type {logical_type} requires literal type {'/'.join(expected_types)}, not {type_}.")
            )
        else:
            warnings.warn(avro.errors.IgnoredLogicalType(f"Unknown {logical_type}, using {type_}."))
    except avro.errors.IgnoredLogicalType as warning:
        warnings.warn(warning)
    return None


def make_avsc_object(json_data, names=None, validate_enum_symbols=True):
    """
    Build Avro Schema from data parsed out of JSON string.

    @arg names: A Names object (tracks seen names and default space)
    @arg validate_enum_symbols: If False, will allow enum symbols that are not valid Avro names.
    """
    names = names or Names()

    # JSON object (non-union)
    if callable(getattr(json_data, "get", None)):
        type = json_data.get("type")
        other_props = get_other_props(json_data, SCHEMA_RESERVED_PROPS)
        logical_type = json_data.get("logicalType")

        if logical_type:
            logical_schema = make_logical_schema(logical_type, type, other_props or {})
            if logical_schema is not None:
                return logical_schema

        if type in NAMED_TYPES:
            name = json_data.get("name")
            namespace = json_data.get("namespace", names.default_namespace)
            if type == "fixed":
                size = json_data.get("size")
                if logical_type == "decimal":
                    precision = json_data.get("precision")
                    scale = 0 if json_data.get("scale") is None else json_data.get("scale")
                    try:
                        return FixedDecimalSchema(size, name, precision, scale, namespace, names, other_props)
                    except avro.errors.IgnoredLogicalType as warning:
                        warnings.warn(warning)
                return FixedSchema(name, namespace, size, names, other_props)
            elif type == "enum":
                symbols = json_data.get("symbols")
                doc = json_data.get("doc")
                return EnumSchema(
                    name,
                    namespace,
                    symbols,
                    names,
                    doc,
                    other_props,
                    validate_enum_symbols,
                )
            elif type in ["record", "error"]:
                fields = json_data.get("fields")
                doc = json_data.get("doc")
                return RecordSchema(name, namespace, fields, names, type, doc, other_props)
            else:
                raise avro.errors.SchemaParseException(f"Unknown Named Type: {type}")

        if type in PRIMITIVE_TYPES:
            return PrimitiveSchema(type, other_props)

        if type in VALID_TYPES:
            if type == "array":
                items = json_data.get("items")
                return ArraySchema(items, names, other_props)
            elif type == "map":
                values = json_data.get("values")
                return MapSchema(values, names, other_props)
            elif type == "error_union":
                declared_errors = json_data.get("declared_errors")
                return ErrorUnionSchema(declared_errors, names)
            else:
                raise avro.errors.SchemaParseException(f"Unknown Valid Type: {type}")
        elif type is None:
            raise avro.errors.SchemaParseException(f'No "type" property: {json_data}')
        else:
            raise avro.errors.SchemaParseException(f"Undefined type: {type}")
    # JSON array (union)
    elif isinstance(json_data, list):
        return UnionSchema(json_data, names)
    # JSON string (primitive)
    elif json_data in PRIMITIVE_TYPES:
        return PrimitiveSchema(json_data)
    # not for us!
    else:
        fail_msg = f"Could not make an Avro Schema object from {json_data}"
        raise avro.errors.SchemaParseException(fail_msg)


# TODO(hammer): make method for reading from a file?


def parse(json_string, validate_enum_symbols=True):
    """Constructs the Schema from the JSON text.

    @arg json_string: The json string of the schema to parse
    @arg validate_enum_symbols: If False, will allow enum symbols that are not valid Avro names.
    @return Schema
    """
    # parse the JSON
    try:
        json_data = json.loads(json_string)
    except Exception as e:
        msg = f"Error parsing JSON: {json_string}, error = {e}"
        new_exception = avro.errors.SchemaParseException(msg)
        traceback = sys.exc_info()[2]
        raise new_exception.with_traceback(traceback)

    # Initialize the names object
    names = Names()

    # construct the Avro Schema object
    return make_avsc_object(json_data, names, validate_enum_symbols)
