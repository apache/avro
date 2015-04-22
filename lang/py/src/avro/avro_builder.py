# -*- coding: utf-8 -*-
import copy

from avro import schema


class AvroBuildInvalidOperation(Exception):
    pass


class AvroSchemaBuilder(object):
    """
    AvroSchemaBuilder creates json-formatted Avro schemas. It has `create_*`
    function for each primitive type to create primitive types. To create a
    complex type, start with corresponding `begin_*` function and finish it
    with `end` function.

    It defers the schema validation until the end of schema building. When
    the schema building ends, it constructs the corresponding schema object
    which will validate the the syntax of the Avro json object.

    Usage: (unit tests cover more usages)
      ab = AvroSchemaBuilder()

      - building a record schema:
        ab.begin_record('user', namespace='yelp')
        ab.add_field('id', ab.create_int())
        ab.add_field(
            'fav_color',
            ab.begin_enum('color_enum', ['red', 'blue']).end()
        )
        record = ab.end()

      - building an enum schema:
        enum_schema = ab.begin_enum('color_enum', ['red', 'blue']).end()

      - building an array schema:
        array_schema = ab.begin_array(ab.create_string()).end()
    """

    def __init__(self):
        self._schema_json = None  # current avro schema in build
        self._schema_tracker = []

    def create_null(self):
        return 'null'

    def create_boolean(self):
        return 'boolean'

    def create_int(self):
        return 'int'

    def create_long(self):
        return 'long'

    def create_float(self):
        return 'float'

    def create_double(self):
        return 'double'

    def create_bytes(self):
        return 'bytes'

    def create_string(self):
        return 'string'

    def begin_enum(self, name, symbols, namespace=None, aliases=None,
                   doc=None, **metadata):
        enum_schema = {
            'type': 'enum',
            'name': name,
            'symbols': symbols
        }
        if namespace:
            self._set_namespace(enum_schema, namespace)
        if aliases:
            self._set_aliases(enum_schema, aliases)
        if doc:
            self._set_doc(enum_schema, doc)
        enum_schema.update(metadata)

        self._save_current_schema()
        self._set_current_schema(enum_schema)
        return self

    def begin_fixed(self, name, size, namespace=None, aliases=None,
                    **metadata):
        fixed_schema = {
            'type': 'fixed',
            'name': name,
            'size': size
        }
        if namespace:
            self._set_namespace(fixed_schema, namespace)
        if aliases:
            self._set_aliases(fixed_schema, aliases)
        fixed_schema.update(metadata)

        self._save_current_schema()
        self._set_current_schema(fixed_schema)
        return self

    def begin_array(self, items_schema, **metadata):
        array_schema = {'type': 'array', 'items': items_schema}
        array_schema.update(metadata)
        self._save_current_schema()
        self._set_current_schema(array_schema)
        return self

    def begin_map(self, values_schema, **metadata):
        map_schema = {'type': 'map', 'values': values_schema}
        map_schema.update(metadata)
        self._save_current_schema()
        self._set_current_schema(map_schema)
        return self

    def begin_record(self, name, namespace=None, aliases=None, doc=None,
                     **metadata):
        record_schema = {'type': 'record', 'name': name, 'fields': []}
        if namespace is not None:
            self._set_namespace(record_schema, namespace)
        if aliases:
            self._set_aliases(record_schema, aliases)
        if doc:
            self._set_doc(record_schema, doc)
        record_schema.update(metadata)

        self._save_current_schema()
        self._set_current_schema(record_schema)
        return self

    def add_field(self, name, typ, has_default=False, default_value=None,
                  sort_order=None, aliases=None, doc=None, **metadata):
        field = {'name': name, 'type': typ}
        if has_default:
            field['default'] = default_value
        if sort_order:
            field['order'] = sort_order
        if aliases:
            self._set_aliases(field, aliases)
        if doc:
            self._set_doc(field, doc)
        field.update(metadata)

        self._schema_json['fields'].append(field)

    def begin_union(self, *avro_schemas):
        union_schema = list(avro_schemas)
        self._save_current_schema()
        self._set_current_schema(union_schema)
        return self

    def end(self):
        if not self._schema_tracker:
            # this is the top level schema; do the schema validation
            schema_obj = schema.make_avsc_object(self._schema_json)
            self._schema_json = None
            return schema_obj.to_json()

        current_schema_json = self._schema_json
        self._restore_current_schema()
        return current_schema_json

    def _save_current_schema(self):
        if self._schema_json:
            self._schema_tracker.append(self._schema_json)

    def _set_current_schema(self, avro_schema):
        self._schema_json = avro_schema

    def _restore_current_schema(self):
        self._schema_json = self._schema_tracker.pop()

    @classmethod
    def _set_namespace(cls, avro_schema, namespace):
        avro_schema['namespace'] = namespace

    @classmethod
    def _set_aliases(cls, avro_schema, aliases):
        avro_schema['aliases'] = aliases

    @classmethod
    def _set_doc(cls, avro_schema, doc):
        avro_schema['doc'] = doc

    def begin_nullable_type(self, schema_type, default_value=None):
        """Create an Avro schema that represents the nullable `schema_type`.
        The nullable type is a union schema type with `null` primitive type.
        The given default value is used to determine whether the `null` type
        should be the first item in the union type.
        """
        null_type = self.create_null()

        src_type = copy.deepcopy(schema_type)
        if self._is_nullable_type(schema_type):
            nullable_schema = src_type
        else:
            typ = src_type if isinstance(src_type, list) else [src_type]
            if default_value is None:
                typ.insert(0, null_type)
            else:
                typ.append(null_type)
            nullable_schema = self.begin_union(*typ).end()

        self._save_current_schema()
        self._set_current_schema(nullable_schema)
        return self

    def _is_nullable_type(self, schema_type):
        null_type = self.create_null()
        return (schema_type is not None
                and (schema_type == null_type
                     or any(typ == null_type for typ in schema_type)))

    def begin_with_schema_json(self, schema_json):
        """Begin building the given schema json object. Note that, similar to
        other `begin_*` functions, it doesn't validate the input schema json
        object until the end of schema.
        """
        self._save_current_schema()
        self._set_current_schema(copy.deepcopy(schema_json))
        return self

    def remove_field(self, field_name):
        fields = self._schema_json.get('fields', [])
        field = next((f for f in fields if f['name'] == field_name), None)
        if not field:
            raise AvroBuildInvalidOperation(
                'Cannot find field named {0}'.format(field_name)
            )
        fields.remove(field)

    def clear(self):
        """Clear the schemas that are built so far."""
        self._schema_json = None
        self._schema_tracker = []
