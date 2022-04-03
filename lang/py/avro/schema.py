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
import uuid
import warnings
from pathlib import Path
from typing import (
    Callable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
)

import avro.constants
import avro.errors
from avro.constants import NAMED_TYPES, PRIMITIVE_TYPES, VALID_TYPES
from avro.name import Name, Names, validate_basename

#
# Constants
#

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


def _is_timezone_aware_datetime(dt: datetime.datetime) -> bool:
    return dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None


#
# Base Classes
#


PropsType = MutableMapping[str, object]


class PropertiesMixin:
    """A mixin that provides basic properties."""

    _reserved_properties: Sequence[str] = ()
    _props: Optional[PropsType] = None

    @property
    def props(self) -> PropsType:
        if self._props is None:
            self._props = {}
        return self._props

    def get_prop(self, key: str) -> Optional[object]:
        return self.props.get(key)

    def set_prop(self, key: str, value: object) -> None:
        self.props[key] = value

    def check_props(self, other: "PropertiesMixin", props: Sequence[str]) -> bool:
        """Check that the given props are identical in two schemas.

        @arg other: The other schema to check
        @arg props: An iterable of properties to check
        @return bool: True if all the properties match
        """
        return all(getattr(self, prop) == getattr(other, prop) for prop in props)

    @property
    def other_props(self) -> PropsType:
        """Dictionary of non-reserved properties"""
        return get_other_props(self.props, self._reserved_properties)


class EqualByJsonMixin:
    """A mixin that defines equality as equal if the json deserializations are equal."""

    def __eq__(self, that: object) -> bool:
        try:
            that_obj = json.loads(str(that))
        except json.decoder.JSONDecodeError:
            return False
        return cast(bool, json.loads(str(self)) == that_obj)


class EqualByPropsMixin(PropertiesMixin):
    """A mixin that defines equality as equal if the props are equal."""

    def __eq__(self, that: object) -> bool:
        return hasattr(that, "props") and self.props == getattr(that, "props")


class CanonicalPropertiesMixin(PropertiesMixin):
    """A Mixin that provides canonical properties to Schema and Field types."""

    @property
    def canonical_properties(self) -> PropsType:
        props = self.props
        return collections.OrderedDict((key, props[key]) for key in CANONICAL_FIELD_ORDER if key in props)


class Schema(abc.ABC, CanonicalPropertiesMixin):
    """Base class for all Schema classes."""

    _reserved_properties = SCHEMA_RESERVED_PROPS

    def __init__(self, type_: str, other_props: Optional[PropsType] = None) -> None:
        if not isinstance(type_, str):
            raise avro.errors.SchemaParseException("Schema type must be a string.")
        if type_ not in VALID_TYPES:
            raise avro.errors.SchemaParseException(f"{type_} is not a valid type.")
        self.set_prop("type", type_)
        self.type = type_
        self.props.update(other_props or {})

    @abc.abstractmethod
    def match(self, writer: "Schema") -> bool:
        """Return True if the current schema (as reader) matches the writer schema.

        @arg writer: the writer schema to match against.
        @return bool
        """

    def __str__(self) -> str:
        return json.dumps(self.to_json())

    @abc.abstractmethod
    def to_json(self, names: Optional[Names] = None) -> object:
        """
        Converts the schema object into its AVRO specification representation.

        Schema types that have names (records, enums, and fixed) must
        be aware of not re-defining schemas that are already listed
        in the parameter names.
        """

    @abc.abstractmethod
    def validate(self, datum: object) -> Optional["Schema"]:
        """Returns the appropriate schema object if datum is valid for that schema, else None.

        To be implemented in subclasses.

        Validation concerns only shape and type of data in the top level of the current schema.
        In most cases, the returned schema object will be self. However, for UnionSchema objects,
        the returned Schema will be the first branch schema for which validation passes.

        @arg datum: The data to be checked for validity according to this schema
        @return Optional[Schema]
        """

    @abc.abstractmethod
    def to_canonical_json(self, names: Optional[Names] = None) -> object:
        """
        Converts the schema object into its Canonical Form
        http://avro.apache.org/docs/current/spec.html#Parsing+Canonical+Form+for+Schemas

        To be implemented in subclasses.
        """

    @property
    def canonical_form(self) -> str:
        # The separators eliminate whitespace around commas and colons.
        return json.dumps(self.to_canonical_json(), separators=(",", ":"))

    @abc.abstractmethod
    def __eq__(self, that: object) -> bool:
        """
        Determines how two schema are compared.
        Consider the mixins EqualByPropsMixin and EqualByJsonMixin
        """


class NamedSchema(Schema):
    """Named Schemas specified in NAMED_TYPES."""

    def __init__(
        self,
        type_: str,
        name: str,
        namespace: Optional[str] = None,
        names: Optional[Names] = None,
        other_props: Optional[PropsType] = None,
    ) -> None:
        super().__init__(type_, other_props)
        if not name:
            raise avro.errors.SchemaParseException("Named Schemas must have a non-empty name.")
        if not isinstance(name, str):
            raise avro.errors.SchemaParseException("The name property must be a string.")
        if namespace is not None and not isinstance(namespace, str):
            raise avro.errors.SchemaParseException("The namespace property must be a string.")
        namespace = namespace or None  # Empty string -> None
        names = names or Names()
        new_name = names.add_name(name, namespace, self)

        # Store name and namespace as they were read in origin schema
        self.set_prop("name", name)
        if namespace is not None:
            self.set_prop("namespace", new_name.space)

        # Store full name as calculated from name, namespace
        self._fullname = new_name.fullname

    @property
    def name(self) -> str:
        name = str(self.get_prop("name"))
        return name

    @property
    def namespace(self) -> Optional[str]:
        ns = self.get_prop("namespace")
        return None if ns is None else str(ns)

    @property
    def fullname(self) -> Optional[str]:
        return self._fullname

    def name_ref(self, names: Names) -> Optional[str]:
        return self.name if self.namespace == names.default_namespace else self.fullname


class LogicalSchema:
    """
    A logical type is an Avro primitive or complex type with extra attributes to represent a derived type.  The attribute logicalType must always be present for a logical type, and is a string with the name of one of the logical types listed later in this section. Other attributes may be defined for particular logical types.

    A logical type is always serialized using its underlying Avro type so that values are encoded in exactly the same way as the equivalent Avro type that does not have a logicalType attribute. Language implementations may choose to represent logical types with an appropriate native type, although this is not required.

    Language implementations must ignore unknown logical types when reading, and should use the underlying Avro type. If a logical type is invalid, for example a decimal with scale greater than its precision, then implementations should ignore the logical type and use the underlying Avro type.
    """

    def __init__(self, logical_type: str) -> None:
        self.logical_type = logical_type


class DecimalLogicalSchema(LogicalSchema):
    """
    The decimal logical type represents an arbitrary-precision signed decimal number of the form unscaled × 10-scale.

    A decimal logical type annotates Avro bytes or fixed types. The byte array must contain the two's-complement representation of the unscaled integer value in big-endian byte order. The scale is fixed, and is specified using an attribute.

    The following attributes are supported:

        scale, a JSON integer representing the scale (optional). If not specified the scale is 0.
        precision, a JSON integer representing the (maximum) precision of decimals stored in this type (required).

    For example, the following schema represents decimal numbers with a maximum precision of 4 and a scale of 2:

    {
      "type": "bytes",
      "logicalType": "decimal",
      "precision": 4,
      "scale": 2
    }

    Precision must be a positive integer greater than zero. If the underlying type is a fixed, then the precision is limited by its size. An array of length n can store at most floor(log_10(28 × n - 1 - 1)) base-10 digits of precision.

    Scale must be zero or a positive integer less than or equal to the precision.

    For the purposes of schema resolution, two schemas that are decimal logical types match if their scales and precisions match.
    """

    def __init__(self, precision: int, scale: int = 0, max_precision: int = 0) -> None:
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
    _reserved_properties: Sequence[str] = FIELD_RESERVED_PROPS

    def __init__(
        self,
        type_: object,
        name: str,
        has_default: bool,
        default: Optional[object] = None,
        order: Optional[str] = None,
        names: Optional[Names] = None,
        doc: Optional[str] = None,
        other_props: Optional[PropsType] = None,
    ) -> None:
        if not name:
            raise avro.errors.SchemaParseException("Fields must have a non-empty name.")
        if not isinstance(name, str):
            raise avro.errors.SchemaParseException("The name property must be a string.")
        if order is not None and order not in VALID_FIELD_SORT_ORDERS:
            raise avro.errors.SchemaParseException(f"The order property {order} is not valid.")
        self._has_default = has_default
        self.props.update(other_props or {})

        type_schema: Schema
        if isinstance(type_, str) and names is not None and names.has_name(type_, None):
            type_schema = cast(NamedSchema, names.get_name(type_, None))
        else:
            try:
                type_schema = make_avsc_object(type_, names)
            except Exception as e:
                raise avro.errors.SchemaParseException(f'Type property "{type_}" not a valid Avro schema: {e}')
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

    @property
    def default(self) -> object:
        return self.get_prop("default")

    @property
    def has_default(self) -> bool:
        return self._has_default

    @property
    def order(self) -> Optional[str]:
        order = self.get_prop("order")
        return None if order is None else str(order)

    @property
    def doc(self) -> Optional[str]:
        doc = self.get_prop("doc")
        return None if doc is None else str(doc)

    def to_json(self, names: Optional[Names] = None) -> PropsType:
        names = names or Names()
        return {**self.props, "type": self.type.to_json(names)}

    def to_canonical_json(self, names: Optional[Names] = None) -> object:
        names = names or Names()
        return {**self.canonical_properties, "type": self.type.to_canonical_json(names)}

    def __str__(self) -> str:
        return json.dumps(self.to_json())


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

    def __init__(self, type_: str, other_props: Optional[PropsType] = None) -> None:
        if type_ not in PRIMITIVE_TYPES:
            raise avro.errors.AvroException(f"{type_} is not a valid primitive type.")
        Schema.__init__(self, type_, other_props=other_props)
        self.fullname = type_

    def match(self, writer: Schema) -> bool:
        """Return True if the current schema (as reader) matches the writer schema.

        @arg writer: the schema to match against
        @return bool
        """
        return self.type == writer.type or {
            "float": self.type == "double",
            "int": self.type in {"double", "float", "long"},
            "long": self.type
            in {
                "double",
                "float",
            },
        }.get(writer.type, False)

    def to_json(self, names: Optional[Names] = None) -> object:
        return self.fullname if len(self.props) == 1 else self.props

    def to_canonical_json(self, names: Optional[Names] = None) -> Union[str, PropsType]:
        return self.fullname if len(self.props) == 1 else self.canonical_properties

    def validate(self, datum: object) -> Optional["PrimitiveSchema"]:
        """Return self if datum is a valid representation of this type of primitive schema, else None

        @arg datum: The data to be checked for validity according to this schema
        @return Schema object or None
        """
        validator: Callable[[object], bool] = self._validators.get(self.type, lambda x: False)
        return self if validator(datum) else None


#
# Decimal Bytes Type
#


class BytesDecimalSchema(PrimitiveSchema, DecimalLogicalSchema):
    def __init__(self, precision: int, scale: int = 0, other_props: Optional[PropsType] = None) -> None:
        DecimalLogicalSchema.__init__(self, precision, scale, max_precision=((1 << 31) - 1))
        PrimitiveSchema.__init__(self, "bytes", other_props)
        self.set_prop("precision", precision)
        self.set_prop("scale", scale)

    @property
    def precision(self) -> int:
        precision = self.get_prop("precision")
        if isinstance(precision, int):
            return precision
        raise avro.errors.SchemaParseException(f"Precision `{precision}` must be an integer")

    @property
    def scale(self) -> int:
        scale = self.get_prop("scale")
        if isinstance(scale, int):
            return scale
        raise avro.errors.SchemaParseException(f"Scale `{scale}` must be an integer")

    def to_json(self, names: Optional[Names] = None) -> PropsType:
        return self.props

    def validate(self, datum: object) -> Optional["BytesDecimalSchema"]:
        """Return self if datum is a Decimal object, else None."""
        return self if isinstance(datum, decimal.Decimal) else None


#
# Complex Types (non-recursive)
#
class FixedSchema(EqualByPropsMixin, NamedSchema):
    def __init__(self, name: str, namespace: str, size: int, names: Optional[Names] = None, other_props: Optional[PropsType] = None) -> None:
        if not isinstance(size, int) or size < 0:
            raise avro.errors.AvroException("Fixed Schema requires a valid positive integer for size property.")
        NamedSchema.__init__(self, "fixed", name, namespace, names, other_props)
        self.set_prop("size", size)

    @property
    def size(self) -> int:
        size = self.get_prop("size")
        if isinstance(size, int):
            return size
        raise avro.errors.SchemaParseException(f"Size `{size}` must be an integer")

    def match(self, writer: Schema) -> bool:
        """Return True if the current schema (as reader) matches the writer schema.

        @arg writer: the schema to match against
        @return bool
        """
        return self.type == writer.type and self.check_props(writer, ["fullname", "size"])

    def to_json(self, names: Optional[Names] = None) -> Union[PropsType, str]:
        names = names or Names()

        if self.fullname in names.names:
            return cast(str, self.name_ref(names))

        names.names[cast(str, self.fullname)] = self
        return names.prune_namespace({**self.props})

    def to_canonical_json(self, names: Optional[Names] = None) -> PropsType:
        return {**self.canonical_properties, "name": self.fullname}

    def validate(self, datum: object) -> Optional["FixedSchema"]:
        """Return self if datum is a valid representation of this schema, else None."""
        return self if isinstance(datum, bytes) and len(datum) == self.size else None


#
# Decimal Fixed Type
#


class FixedDecimalSchema(FixedSchema, DecimalLogicalSchema):
    def __init__(
        self,
        size: int,
        name: str,
        precision: int,
        scale: int = 0,
        namespace: str = "",
        names: Optional[Names] = None,
        other_props: Optional[PropsType] = None,
    ) -> None:
        max_precision = int(math.floor(math.log10(2) * (8 * size - 1)))
        DecimalLogicalSchema.__init__(self, precision, scale, max_precision)
        FixedSchema.__init__(self, name, namespace, size, names, other_props)
        self.set_prop("precision", precision)
        self.set_prop("scale", scale)

    @property
    def precision(self) -> int:
        precision = self.get_prop("precision")
        if isinstance(precision, int):
            return precision
        raise avro.errors.SchemaParseException(f"Precision `{precision}` must be an integer")

    @property
    def scale(self) -> int:
        scale = self.get_prop("scale")
        if isinstance(scale, int):
            return scale
        raise avro.errors.SchemaParseException(f"Scale `{scale}` must be an integer")

    def to_json(self, names: Optional[Names] = None) -> PropsType:
        return self.props

    def validate(self, datum: object) -> Optional["FixedDecimalSchema"]:
        """Return self if datum is a Decimal object, else None."""
        return self if isinstance(datum, decimal.Decimal) else None


class EnumSchema(EqualByPropsMixin, NamedSchema):
    def __init__(
        self,
        name: str,
        namespace: str,
        symbols: Sequence[str],
        names: Optional[avro.name.Names] = None,
        doc: Optional[str] = None,
        other_props: Optional[PropsType] = None,
        validate_enum_symbols: bool = True,
    ) -> None:
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
            raise avro.errors.AvroException(f"Duplicate symbol: {symbols}")

        # Call parent ctor
        NamedSchema.__init__(self, "enum", name, namespace, names, other_props)

        # Add class members
        self.set_prop("symbols", symbols)
        if doc is not None:
            self.set_prop("doc", doc)

        if other_props and "default" in other_props:
            default = other_props["default"]
            if default not in symbols:
                raise avro.errors.InvalidDefault(f"Enum default '{default}' is not a valid member of symbols '{symbols}'")

    @property
    def symbols(self) -> Sequence[str]:
        symbols = self.get_prop("symbols")
        if isinstance(symbols, Sequence):
            return symbols
        raise Exception

    @property
    def doc(self) -> Optional[str]:
        doc = self.get_prop("doc")
        return None if doc is None else str(doc)

    def match(self, writer: Schema) -> bool:
        """Return True if the current schema (as reader) matches the writer schema.

        @arg writer: the schema to match against
        @return bool
        """
        return self.type == writer.type and self.check_props(writer, ["fullname"])

    def to_json(self, names: Optional[Names] = None) -> Union[PropsType, str]:
        names = names or Names()

        if self.fullname in names.names:
            return cast(str, self.name_ref(names))

        names.names[cast(str, self.fullname)] = self
        return names.prune_namespace({**self.props})

    def to_canonical_json(self, names: Optional[Names] = None) -> Union[PropsType, str]:
        names_as_json = self.to_json(names)
        return cast(str, self.fullname) if isinstance(names_as_json, str) else {**self.canonical_properties, "name": cast(object, self.fullname)}

    def validate(self, datum: object) -> Optional["EnumSchema"]:
        """Return self if datum is a valid member of this Enum, else None."""
        return self if datum in self.symbols else None


#
# Complex Types (recursive)
#


class ArraySchema(EqualByJsonMixin, Schema):
    def __init__(self, items: object, names: Optional[Names] = None, other_props: Optional[PropsType] = None) -> None:
        # Call parent ctor
        Schema.__init__(self, "array", other_props)
        # Add class members
        names = names or Names()
        items_schema: Schema
        if isinstance(items, str) and names.has_name(items, None):
            items_schema = cast(NamedSchema, names.get_name(items, None))
        else:
            try:
                items_schema = make_avsc_object(items, names)
            except avro.errors.SchemaParseException as e:
                fail_msg = f"Items schema ({items}) not a valid Avro schema: {e} (known names: {names.names.keys()})"
                raise avro.errors.SchemaParseException(fail_msg)

        self.set_prop("items", items_schema)

    @property
    def items(self) -> Schema:
        items = self.get_prop("items")
        if isinstance(items, Schema):
            return items
        raise avro.errors.SchemaParseException(f"Items `{items}` must be a valid Avro schema")

    def match(self, writer: Schema) -> bool:
        """Return True if the current schema (as reader) matches the writer schema.

        @arg writer: the schema to match against
        @return bool
        """
        return self.type == writer.type and isinstance(writer, ArraySchema) and self.items.check_props(writer.items, ["type"])

    def to_json(self, names: Optional[Names] = None) -> PropsType:
        names = names or Names()
        item_json = self.items.to_json(names)
        return {**self.props, "items": item_json}

    def to_canonical_json(self, names: Optional[Names] = None) -> PropsType:
        names = names or Names()
        item_json = self.items.to_canonical_json(names)
        return {**self.canonical_properties, "items": item_json}

    def validate(self, datum: object) -> Optional["ArraySchema"]:
        """Return self if datum is a valid representation of this schema, else None."""
        return self if isinstance(datum, list) else None


class MapSchema(EqualByJsonMixin, Schema):
    def __init__(self, values: object, names: Optional[Names] = None, other_props: Optional[PropsType] = None) -> None:
        Schema.__init__(self, "map", other_props)
        names = names or Names()
        # Add class members
        values_schema: Schema
        if isinstance(values, str) and names.has_name(values, None):
            values_schema = cast(Schema, names.get_name(values, None))
        else:
            try:
                values_schema = make_avsc_object(values, names)
            except avro.errors.SchemaParseException:
                raise
            except Exception:
                raise avro.errors.SchemaParseException("Values schema is not a valid Avro schema.")

        self.set_prop("values", values_schema)

    @property
    def values(self) -> Schema:
        values = self.get_prop("values")
        if isinstance(values, Schema):
            return values
        raise avro.errors.SchemaParseException(f"Values `{values}` must be a valid Avro schema")

    def match(self, writer: Schema) -> bool:
        """Return True if the current schema (as reader) matches the writer schema.

        @arg writer: the schema to match against
        @return bool
        """
        return writer.type == self.type and isinstance(writer, MapSchema) and self.values.check_props(writer.values, ["type"])

    def to_json(self, names: Optional[Names] = None) -> PropsType:
        names = names or Names()
        values_json = self.values.to_json(names)
        return {**self.props, "values": values_json}

    def to_canonical_json(self, names: Optional[Names] = None) -> PropsType:
        names = names or Names()
        values_json = self.values.to_canonical_json(names)
        return {**self.canonical_properties, "values": values_json}

    def validate(self, datum: object) -> Optional["MapSchema"]:
        """Return self if datum is a valid representation of this schema, else None."""
        return self if isinstance(datum, dict) and all(isinstance(key, str) for key in datum) else None


class UnionSchema(EqualByJsonMixin, Schema):
    """
    names is a dictionary of schema objects
    """

    def __init__(self, schemas: Sequence[Union[str, object, Schema]], names: Optional[Names] = None) -> None:
        if not isinstance(schemas, list):
            raise avro.errors.SchemaParseException("Union schema requires a list of schemas.")
        Schema.__init__(self, "union")
        names = names or Names()
        # Add class members
        schema_objects: List[Schema] = []
        new_schema: Schema
        for schema in schemas:
            if isinstance(schema, str) and names.has_name(schema, None):
                new_schema = cast(NamedSchema, names.get_name(schema, None))
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

    @property
    def schemas(self) -> Sequence[Schema]:
        return self._schemas

    def match(self, writer: Schema) -> bool:
        """Return True if the current schema (as reader) matches the writer schema.

        @arg writer: the schema to match against
        @return bool
        """
        return writer.type in {"union", "error_union"} or any(s.match(writer) for s in self.schemas)

    def to_json(self, names: Optional[Names] = None) -> Sequence[object]:
        names = names or Names()
        return [schema.to_json(names) for schema in self.schemas]

    def to_canonical_json(self, names: Optional[Names] = None) -> Sequence[object]:
        names = names or Names()
        return [schema.to_canonical_json(names) for schema in self.schemas]

    def validate(self, datum: object) -> Optional[Schema]:
        """Return the first branch schema of which datum is a valid example, else None."""
        return next((branch for branch in self.schemas if branch.validate(datum) is not None), None)


class ErrorUnionSchema(UnionSchema):
    def __init__(self, schemas: Sequence[Schema], names: Optional[Names] = None) -> None:
        """Prepend "string" to handle system errors"""
        super().__init__(["string", *schemas], names)

    def to_json(self, names: Optional[Names] = None) -> Sequence[object]:
        """Omit the system error schema"""
        return super().to_json(names)[1:]


class RecordSchema(EqualByJsonMixin, NamedSchema):
    _fields: Sequence[Field]

    @staticmethod
    def make_field_objects(field_data: Sequence[Mapping[str, object]], names: Optional[Names]) -> Sequence[Field]:
        """We're going to need to make message parameters too."""
        field_objects = []
        field_names = []
        for field in field_data:
            if not callable(getattr(field, "get", None)):
                raise avro.errors.SchemaParseException(f"Not a valid field: {field}")
            type_ = cast(str, field.get("type"))
            name = cast(str, field.get("name"))

            # null values can have a default value of None
            has_default = "default" in field
            default = field.get("default")
            order = cast(str, field.get("order")) or None
            doc = cast(str, field.get("doc")) or None
            other_props = get_other_props(field, FIELD_RESERVED_PROPS)
            new_field = Field(type_, name, has_default, default, order, names, doc, other_props)
            # make sure field name has not been used yet
            if new_field.name in field_names:
                raise avro.errors.SchemaParseException(f"Field name {new_field.name} already in use.")
            field_names.append(new_field.name)
            field_objects.append(new_field)
        return field_objects

    def match(self, writer: Schema) -> bool:
        """Return True if the current schema (as reader) matches the other schema.

        @arg writer: the schema to match against
        @return bool
        """
        return writer.type == self.type and (self.type == "request" or self.check_props(writer, ["fullname"]))

    def __init__(
        self,
        name: Optional[str],
        namespace: Optional[str],
        fields: Sequence[Mapping[str, object]],
        names: Optional[Names] = None,
        schema_type: str = "record",
        doc: Optional[str] = None,
        other_props: Optional[PropsType] = None,
    ) -> None:
        if fields is None:
            raise avro.errors.SchemaParseException(f"Record schema requires a non-empty fields property.")
        if not isinstance(fields, list):
            raise avro.errors.SchemaParseException("Fields property must be a list of Avro schemas.")
        if schema_type == "request":
            Schema.__init__(self, schema_type, other_props)
        else:
            NamedSchema.__init__(self, schema_type, cast(str, name), namespace, names, other_props)
            if schema_type == "record":
                old_default = cast(Names, names).default_namespace
                cast(Names, names).default_namespace = Name(name, namespace, cast(Names, names).default_namespace).space

        # Add class members
        field_objects = RecordSchema.make_field_objects(fields, names)
        self._fields = field_objects
        self.set_prop("fields", field_objects)
        if doc is not None:
            self.set_prop("doc", doc)

        if schema_type == "record":
            cast(Names, names).default_namespace = old_default

    @property
    def fields(self) -> Sequence[Field]:
        return self._fields

    @property
    def doc(self) -> Optional[str]:
        doc = self.get_prop("doc")
        return None if doc is None else str(doc)

    @property
    def fields_dict(self) -> Mapping[str, Field]:
        return {field.name: field for field in self.fields}

    def to_json(self, names: Optional[Names] = None) -> Union[str, Sequence[PropsType], PropsType]:
        names = names or Names()
        if self.type == "request":  # Request records don't have names
            return [f.to_json(names) for f in self.fields]

        if self.fullname in names.names:
            return cast(str, self.name_ref(names))
        names.names[cast(str, self.fullname)] = self
        return {**names.prune_namespace({**self.props}), "fields": [f.to_json(names) for f in self.fields]}

    def to_canonical_json(self, names: Optional[Names] = None) -> Union[str, Sequence[object], PropsType]:
        names = names or Names()
        if self.type == "request":
            raise NotImplementedError("Canonical form (probably) does not make sense on type request")
        if self.fullname in names.names:
            return cast(str, self.name_ref(names))
        names.names[cast(str, self.fullname)] = self
        return {**self.canonical_properties, "name": self.fullname, "fields": [f.to_canonical_json(names) for f in self.fields]}

    def validate(self, datum: object) -> Optional["RecordSchema"]:
        """Return self if datum is a valid representation of this schema, else None"""
        return self if isinstance(datum, dict) and {f.name for f in self.fields}.issuperset(datum.keys()) else None


#
# Date Type
#


class DateSchema(LogicalSchema, PrimitiveSchema):
    def __init__(self, other_props: Optional[PropsType] = None) -> None:
        LogicalSchema.__init__(self, avro.constants.DATE)
        PrimitiveSchema.__init__(self, "int", other_props)

    def to_json(self, names: Optional[Names] = None) -> PropsType:
        return self.props

    def validate(self, datum: object) -> Optional["DateSchema"]:
        """Return self if datum is a valid date object, else None."""
        return self if isinstance(datum, datetime.date) else None


#
# time-millis Type
#


class TimeMillisSchema(LogicalSchema, PrimitiveSchema):
    def __init__(self, other_props: Optional[PropsType] = None) -> None:
        LogicalSchema.__init__(self, avro.constants.TIME_MILLIS)
        PrimitiveSchema.__init__(self, "int", other_props)

    def to_json(self, names: Optional[Names] = None) -> PropsType:
        return self.props

    def validate(self, datum: object) -> Optional["TimeMillisSchema"]:
        """Return self if datum is a valid representation of this schema, else None."""
        return self if isinstance(datum, datetime.time) else None


#
# time-micros Type
#


class TimeMicrosSchema(LogicalSchema, PrimitiveSchema):
    def __init__(self, other_props: Optional[PropsType] = None) -> None:
        LogicalSchema.__init__(self, avro.constants.TIME_MICROS)
        PrimitiveSchema.__init__(self, "long", other_props)

    def to_json(self, names: Optional[Names] = None) -> PropsType:
        return self.props

    def validate(self, datum: object) -> Optional["TimeMicrosSchema"]:
        """Return self if datum is a valid representation of this schema, else None."""
        return self if isinstance(datum, datetime.time) else None


#
# timestamp-millis Type
#


class TimestampMillisSchema(LogicalSchema, PrimitiveSchema):
    def __init__(self, other_props: Optional[PropsType] = None) -> None:
        LogicalSchema.__init__(self, avro.constants.TIMESTAMP_MILLIS)
        PrimitiveSchema.__init__(self, "long", other_props)

    def to_json(self, names: Optional[Names] = None) -> PropsType:
        return self.props

    def validate(self, datum: object) -> Optional["TimestampMillisSchema"]:
        return self if isinstance(datum, datetime.datetime) and _is_timezone_aware_datetime(datum) else None


#
# timestamp-micros Type
#


class TimestampMicrosSchema(LogicalSchema, PrimitiveSchema):
    def __init__(self, other_props: Optional[PropsType] = None) -> None:
        LogicalSchema.__init__(self, avro.constants.TIMESTAMP_MICROS)
        PrimitiveSchema.__init__(self, "long", other_props)

    def to_json(self, names: Optional[Names] = None) -> PropsType:
        return self.props

    def validate(self, datum: object) -> Optional["TimestampMicrosSchema"]:
        return self if isinstance(datum, datetime.datetime) and _is_timezone_aware_datetime(datum) else None


#
# uuid Type
#


class UUIDSchema(LogicalSchema, PrimitiveSchema):
    def __init__(self, other_props: Optional[PropsType] = None) -> None:
        LogicalSchema.__init__(self, avro.constants.UUID)
        PrimitiveSchema.__init__(self, "string", other_props)

    def to_json(self, names: Optional[Names] = None) -> PropsType:
        return self.props

    def validate(self, datum: object) -> Optional["UUIDSchema"]:
        try:
            uuid.UUID(cast(str, datum))
        except ValueError:
            # If it's a value error, then the string
            # is not a valid hex code for a UUID.
            return None
        return self


#
# Module Methods
#


def get_other_props(all_props: Mapping[str, object], reserved_props: Sequence[str]) -> PropsType:
    """
    Retrieve the non-reserved properties from a dictionary of properties
    @args reserved_props: The set of reserved properties to exclude
    """
    return {k: v for k, v in all_props.items() if k not in reserved_props}


def make_bytes_decimal_schema(other_props: PropsType) -> BytesDecimalSchema:
    """Make a BytesDecimalSchema from just other_props."""
    precision = cast(int, other_props.get("precision"))
    scale = cast(int, other_props.get("scale", 0))
    return BytesDecimalSchema(precision, scale, other_props)


def make_logical_schema(logical_type: str, type_: str, other_props: PropsType) -> Optional[LogicalSchema]:
    """Map the logical types to the appropriate literal type and schema class."""
    logical_literal_pairs = (
        (avro.constants.DECIMAL, "fixed"),
        (avro.constants.DATE, "int"),
        (avro.constants.DECIMAL, "bytes"),
        (avro.constants.TIMESTAMP_MICROS, "long"),
        (avro.constants.TIMESTAMP_MILLIS, "long"),
        (avro.constants.TIME_MICROS, "long"),
        (avro.constants.TIME_MILLIS, "int"),
        (avro.constants.UUID, "string"),
    )
    try:
        if (logical_type, type_) == (avro.constants.DECIMAL, "fixed"):
            return None  # The fixed decimal schema is handled later.
        if (logical_type, type_) == (avro.constants.DATE, "int"):
            return DateSchema(other_props)
        if (logical_type, type_) == (avro.constants.DECIMAL, "bytes"):
            return make_bytes_decimal_schema(other_props)
        if (logical_type, type_) == (avro.constants.TIMESTAMP_MICROS, "long"):
            return TimestampMicrosSchema(other_props)
        if (logical_type, type_) == (avro.constants.TIMESTAMP_MILLIS, "long"):
            return TimestampMillisSchema(other_props)
        if (logical_type, type_) == (avro.constants.TIME_MICROS, "long"):
            return TimeMicrosSchema(other_props)
        if (logical_type, type_) == (avro.constants.TIME_MILLIS, "int"):
            return TimeMillisSchema(other_props)
        if (logical_type, type_) == (avro.constants.UUID, "string"):
            return UUIDSchema(other_props)

        expected_types = sorted(literal_type for lt, literal_type in logical_literal_pairs if lt == logical_type)
        if expected_types:
            warnings.warn(
                avro.errors.IgnoredLogicalType(f"Logical type {logical_type} requires literal type {'/'.join(expected_types)}, not {type_}.")
            )
        else:
            warnings.warn(avro.errors.IgnoredLogicalType(f"Unknown {logical_type}, using {type_}."))
    except avro.errors.IgnoredLogicalType as warning:
        warnings.warn(warning)
    return None


def make_avsc_object(json_data: object, names: Optional[Names] = None, validate_enum_symbols: bool = True) -> Schema:
    """
    Build Avro Schema from data parsed out of JSON string.

    @arg names: A Names object (tracks seen names and default space)
    @arg validate_enum_symbols: If False, will allow enum symbols that are not valid Avro names.
    """
    names = names or Names()

    # JSON object (non-union)
    if callable(getattr(json_data, "get", None)):
        json_data = cast(Mapping[str, object], json_data)
        doc = cast(Optional[str], json_data.get("doc"))
        type_ = cast(str, json_data.get("type"))
        other_props = get_other_props(json_data, SCHEMA_RESERVED_PROPS)
        logical_type = cast(str, json_data.get("logicalType"))

        if logical_type:
            logical_schema = make_logical_schema(logical_type, type_, other_props or {})
            if logical_schema is not None:
                return cast(Schema, logical_schema)

        if type_ in NAMED_TYPES:
            name = json_data.get("name")
            if not isinstance(name, str):
                raise avro.errors.SchemaParseException(f"Name {name} must be a string, but it is {type(name)}.")
            namespace = cast(str, json_data.get("namespace", names.default_namespace))
            if type_ == "fixed":
                size = cast(int, json_data.get("size"))
                if logical_type == "decimal":
                    precision = cast(int, json_data.get("precision"))
                    scale = 0 if json_data.get("scale") is None else cast(int, json_data.get("scale"))
                    try:
                        return FixedDecimalSchema(size, name, precision, scale, namespace, names, other_props)
                    except avro.errors.IgnoredLogicalType as warning:
                        warnings.warn(warning)
                return FixedSchema(name, namespace, size, names, other_props)
            elif type_ == "enum":
                symbols = json_data.get("symbols")
                if not isinstance(symbols, Sequence):
                    raise avro.errors.SchemaParseException(f"Enum symbols must be a sequence of strings, but it is {type(symbols)}")
                for symbol in symbols:
                    if not isinstance(symbol, str):
                        raise avro.errors.SchemaParseException(f"Enum symbols must be a sequence of strings, but one symbol is a {type(symbol)}")
                return EnumSchema(
                    name,
                    namespace,
                    symbols,
                    names,
                    doc,
                    other_props,
                    validate_enum_symbols,
                )
            if type_ in ["record", "error"]:
                fields = cast(Sequence[Mapping[str, object]], json_data.get("fields"))
                return RecordSchema(name, namespace, fields, names, type_, doc, other_props)
            raise avro.errors.SchemaParseException(f"Unknown Named Type: {type_}")

        if type_ in PRIMITIVE_TYPES:
            return PrimitiveSchema(type_, other_props)

        if type_ in VALID_TYPES:
            if type_ == "array":
                items = json_data.get("items")
                return ArraySchema(items, names, other_props)
            elif type_ == "map":
                values = json_data.get("values")
                return MapSchema(values, names, other_props)
            elif type_ == "error_union":
                declared_errors = cast(Sequence[Schema], json_data.get("declared_errors"))
                return ErrorUnionSchema(declared_errors, names)
            else:
                raise avro.errors.SchemaParseException(f"Unknown Valid Type: {type_}")
        elif type_ is None:
            raise avro.errors.SchemaParseException(f'No "type" property: {json_data}')
        else:
            raise avro.errors.SchemaParseException(f"Undefined type: {type_}")
    # JSON array (union)
    elif isinstance(json_data, list):
        return UnionSchema(json_data, names)
    # JSON string (primitive)
    elif json_data in PRIMITIVE_TYPES:
        return PrimitiveSchema(cast(str, json_data))
    # not for us!
    raise avro.errors.SchemaParseException(f"Could not make an Avro Schema object from {json_data}")


def parse(json_string: str, validate_enum_symbols: bool = True) -> Schema:
    """Constructs the Schema from the JSON text.

    @arg json_string: The json string of the schema to parse
    @arg validate_enum_symbols: If False, will allow enum symbols that are not valid Avro names.
    @return Schema
    """
    try:
        json_data = json.loads(json_string)
    except json.decoder.JSONDecodeError as e:
        raise avro.errors.SchemaParseException(f"Error parsing JSON: {json_string}, error = {e}") from e
    return make_avsc_object(json_data, Names(), validate_enum_symbols)


def from_path(path: Union[Path, str], validate_enum_symbols: bool = True) -> Schema:
    """
    Constructs the Schema from a path to an avsc (json) file.
    """
    return parse(Path(path).read_text(), validate_enum_symbols)
