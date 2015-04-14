# -*- coding: utf-8 -*-
import unittest2 as unittest

from avro import avro_builder
from avro import schema


class TestAvroSchemaBuilder(unittest.TestCase):

    def setUp(self):
        self.builder = avro_builder.AvroSchemaBuilder()

    def tearDown(self):
        del self.builder

    @property
    def name(self):
        return 'foo'

    @property
    def namespace(self):
        return 'ns'

    @property
    def aliases(self):
        return ['new_foo']

    @property
    def doc(self):
        return 'sample doc'

    @property
    def metadata(self):
        return {'key1': 'val1', 'key2': 'val2'}

    @property
    def enum_symbols(self):
        return ['a', 'b']

    @property
    def fixed_size(self):
        return 16
    
    @property
    def another_name(self):
        return 'bar'

    @property
    def invalid_schemas(self):
        undefined_schema_name = 'unknown'
        yield undefined_schema_name

        non_avro_schema = {'foo': 'bar'}
        yield non_avro_schema

        named_schema_without_name = {'name': '', 'type': 'fixed', 'size': 16}
        yield named_schema_without_name

        invalid_schema = {'name': 'foo', 'type': 'enum', 'symbols': ['a', 'a']}
        yield invalid_schema

        none_schema = None
        yield none_schema

    @property
    def invalid_names(self):
        missing_name = None
        yield missing_name

        reserved_name = 'int'
        yield reserved_name

        non_string_name = 100
        yield non_string_name

    @property
    def duplicate_name_err(self):
        return '"{0}" is already in use.'

    def test_create_primitive_types(self):
        self.assertEqual('null', self.builder.create_null())
        self.assertEqual('boolean', self.builder.create_boolean())
        self.assertEqual('int', self.builder.create_int())
        self.assertEqual('long', self.builder.create_long())
        self.assertEqual('float', self.builder.create_float())
        self.assertEqual('double', self.builder.create_double())
        self.assertEqual('bytes', self.builder.create_bytes())
        self.assertEqual('string', self.builder.create_string())

    def test_create_enum(self):
        actual_json = self.builder.begin_enum(self.name, self.enum_symbols).end()
        expected_json = {
            'type': 'enum',
            'name': self.name,
            'symbols': self.enum_symbols
        }
        self.assertEqual(expected_json, actual_json)

    def test_create_enum_with_optional_attributes(self):
        actual_json = self.builder.begin_enum(
            self.name,
            self.enum_symbols,
            self.namespace,
            self.aliases,
            self.doc,
            **self.metadata
        ).end()

        expected_json = {
            'type': 'enum',
            'name': self.name,
            'symbols': self.enum_symbols,
            'namespace': self.namespace,
            'aliases': self.aliases,
            'doc': self.doc
        }
        expected_json.update(self.metadata)

        self.assertEqual(expected_json, actual_json)

    def test_create_enum_with_invalid_name(self):
        for invalid_name in self.invalid_names:
            self.builder.clear()
            with self.assertRaises(schema.SchemaParseException):
                self.builder.begin_enum(invalid_name, self.enum_symbols).end()

    def test_create_enum_with_dup_name(self):
        with self.assertRaisesRegexp(
                schema.SchemaParseException,
                self.duplicate_name_err.format(self.name)
        ):
            self.builder.begin_record(self.name)
            self.builder.add_field(
                self.another_name,
                self.builder.begin_enum(self.name, self.enum_symbols).end()
            )
            self.builder.end()

    def test_create_enum_with_invalid_symbols(self):
        self.single_test_create_enum_with_invalid_symbols(None)
        self.single_test_create_enum_with_invalid_symbols('')
        self.single_test_create_enum_with_invalid_symbols('a')
        self.single_test_create_enum_with_invalid_symbols(['a', 1])
        self.single_test_create_enum_with_invalid_symbols([1, 2, 3])
        self.single_test_create_enum_with_invalid_symbols(['a', 'a'])

    def single_test_create_enum_with_invalid_symbols(self, invalid_symbols):
        self.builder.clear()
        with self.assertRaises(schema.AvroException):
            self.builder.begin_enum(self.name, invalid_symbols).end()

    def test_create_fixed(self):
        actual_json = self.builder.begin_fixed(self.name, self.fixed_size).end()
        expected_json = {
            'type': 'fixed',
            'name': self.name,
            'size': self.fixed_size
        }
        self.assertEqual(expected_json, actual_json)

    def test_create_fixed_with_optional_attributes(self):
        actual_json = self.builder.begin_fixed(
            self.name,
            self.fixed_size,
            self.namespace,
            self.aliases,
            **self.metadata
        ).end()

        expected_json = {
            'type': 'fixed',
            'name': self.name,
            'size': self.fixed_size,
            'namespace': self.namespace,
            'aliases': self.aliases,
        }
        expected_json.update(self.metadata)

        self.assertEqual(expected_json, actual_json)

    def test_create_fixed_with_invalid_name(self):
        for invalid_name in self.invalid_names:
            self.builder.clear()
            with self.assertRaises(schema.SchemaParseException):
                self.builder.begin_fixed(invalid_name, self.fixed_size).end()

    def test_create_fixed_with_dup_name(self):
        with self.assertRaisesRegexp(
                schema.SchemaParseException,
                self.duplicate_name_err.format(self.name)
        ):
            self.builder.begin_record(self.name)
            self.builder.add_field(
                self.another_name,
                self.builder.begin_fixed(self.name, self.fixed_size).end()
            )
            self.builder.end()

    def test_create_fixed_with_invalid_size(self):
        self.single_test_create_fixed_with_invalid_size(None)
        self.single_test_create_fixed_with_invalid_size('ten')

    def single_test_create_fixed_with_invalid_size(self, invalid_size):
        self.builder.clear()
        with self.assertRaises(schema.AvroException):
            self.builder.begin_fixed(self.name, invalid_size).end()

    def test_create_array(self):
        actual_json = self.builder.begin_array(self.builder.create_int()).end()
        expected_json = {'type': 'array', 'items': 'int'}
        self.assertEqual(expected_json, actual_json)

    def test_create_array_with_optional_attributes(self):
        actual_json = self.builder.begin_array(
            self.builder.create_int(),
            **self.metadata
        ).end()

        expected_json = {'type': 'array', 'items': 'int'}
        expected_json.update(self.metadata)

        self.assertEqual(expected_json, actual_json)

    def test_create_array_with_complex_type(self):
        actual_json = self.builder.begin_array(
            self.builder.begin_enum(self.name, self.enum_symbols).end()
        ).end()
        expected_json = {
            'type': 'array',
            'items': {
                'type': 'enum',
                'name': self.name,
                'symbols': self.enum_symbols
            }
        }
        self.assertEqual(expected_json, actual_json)

    def test_create_array_with_invalid_items_type(self):
        for invalid_schema in self.invalid_schemas:
            self.builder.clear()
            with self.assertRaises(schema.AvroException):
                self.builder.begin_array(invalid_schema).end()

    def test_create_map(self):
        actual_json = self.builder.begin_map(self.builder.create_string()).end()
        expected_json = {'type': 'map', 'values': 'string'}
        self.assertEqual(expected_json, actual_json)

    def test_create_map_with_optional_attributes(self):
        actual_json = self.builder.begin_map(
            self.builder.create_string(),
            **self.metadata
        ).end()

        expected_json = {'type': 'map', 'values': 'string'}
        expected_json.update(self.metadata)

        self.assertEqual(expected_json, actual_json)

    def test_create_map_with_complex_type(self):
        actual_json = self.builder.begin_map(
            self.builder.begin_fixed(self.name, self.fixed_size).end()
        ).end()
        expected_json = {
            'type': 'map',
            'values': {
                'type': 'fixed',
                'name': self.name,
                'size': self.fixed_size
            }
        }
        self.assertEqual(expected_json, actual_json)

    def test_create_map_with_invalid_values_type(self):
        for invalid_schema in self.invalid_schemas:
            self.builder.clear()
            with self.assertRaises(schema.AvroException):
                self.builder.begin_map(invalid_schema).end()

    def test_create_record(self):
        self.builder.begin_record(self.name)
        self.builder.add_field(
            'bar1',
            self.builder.create_int()
        )
        self.builder.add_field(
            'bar2',
            self.builder.begin_map(self.builder.create_double()).end()
        )
        actual_json = self.builder.end()

        expected_json = {
            'type': 'record',
            'name': self.name,
            'fields': [
                {'name': 'bar1', 'type': 'int'},
                {'name': 'bar2', 'type': {'type': 'map', 'values': 'double'}}
            ]
        }
        self.assertEqual(expected_json, actual_json)

    def test_create_record_with_optional_attributes(self):
        self.builder.begin_record(
            self.name,
            namespace=self.namespace,
            aliases=self.aliases,
            doc=self.doc,
            **self.metadata
        )
        self.builder.add_field(
            self.another_name,
            self.builder.create_int()
        )
        actual_json = self.builder.end()

        expected_json = {
            'type': 'record',
            'name': self.name,
            'fields': [{'name': self.another_name, 'type': 'int'}],
            'namespace': self.namespace,
            'aliases': self.aliases,
            'doc': self.doc
        }
        expected_json.update(self.metadata)

        self.assertEqual(expected_json, actual_json)

    def test_create_field_with_optional_attributes(self):
        self.builder.begin_record(self.name)
        self.builder.add_field(
            self.another_name,
            self.builder.create_boolean(),
            has_default=True,
            default_value=True,
            sort_order='ascending',
            aliases=self.aliases,
            doc=self.doc,
            **self.metadata
        )
        actual_json = self.builder.end()

        expected_field = {
            'name': self.another_name,
            'type': 'boolean',
            'default': True,
            'order': 'ascending',
            'aliases': self.aliases,
            'doc': self.doc
        }
        expected_field.update(self.metadata)
        expected_json = {
            'type': 'record',
            'name': self.name,
            'fields': [expected_field]
        }

        self.assertEqual(expected_json, actual_json)

    def test_create_record_with_no_field(self):
        actual_json = self.builder.begin_record(self.name).end()
        expected_json = {'type': 'record', 'name': self.name, 'fields': []}
        self.assertEqual(expected_json, actual_json)

    def test_create_record_with_invalid_name(self):
        for invalid_name in self.invalid_names:
            self.builder.clear()
            with self.assertRaises(schema.SchemaParseException):
                self.builder.begin_record(invalid_name)
                self.builder.add_field(
                    self.another_name,
                    self.builder.create_int()
                )
                self.builder.end()

    def test_create_record_with_dup_name(self):
        with self.assertRaisesRegexp(
                schema.SchemaParseException,
                self.duplicate_name_err.format(self.name)
        ):
            self.builder.begin_record(self.another_name)
            self.builder.add_field(
                'bar1',
                self.builder.begin_enum(self.name, self.enum_symbols).end()
            )
            self.builder.add_field(
                'bar2',
                self.builder.begin_record(self.name).end()
            )
            self.builder.end()

    def test_create_record_with_dup_field_name(self):
        with self.assertRaisesRegexp(
                schema.SchemaParseException,
                "{0} already in use.".format(self.another_name)
        ):
            self.builder.begin_record(self.name)
            self.builder.add_field(
                self.another_name,
                self.builder.create_int()
            )
            self.builder.add_field(
                self.another_name,
                self.builder.create_string()
            )
            self.builder.end()

    def test_create_field_with_invalid_type(self):
        for invalid_schema in self.invalid_schemas:
            self.builder.clear()
            with self.assertRaises(schema.SchemaParseException):
                self.builder.begin_record(self.name)
                self.builder.add_field(
                    self.another_name,
                    invalid_schema
                )
                self.builder.end()

    def test_create_field_with_invalid_sort_order(self):
        with self.assertRaises(schema.SchemaParseException):
            self.builder.begin_record(self.name)
            self.builder.add_field(
                self.another_name,
                self.builder.create_int(),
                sort_order='asc'
            )
            self.builder.end()

    def test_create_union(self):
        actual_json = self.builder.begin_union(
            self.builder.create_null(),
            self.builder.create_string(),
            self.builder.begin_enum(self.name, self.enum_symbols).end()
        ).end()

        expected_json = [
            'null',
            'string',
            {'type': 'enum', 'name': self.name, 'symbols': self.enum_symbols}
        ]
        self.assertEqual(expected_json, actual_json)

    def test_create_union_with_empty_sub_schemas(self):
        actual_json = self.builder.begin_union().end()
        expected_json = []
        self.assertEqual(expected_json, actual_json)

    def test_create_union_with_nested_union_schema(self):
        with self.assertRaises(schema.SchemaParseException):
            self.builder.begin_union(
                self.builder.begin_union(self.builder.create_int()).end()
            ).end()

    def test_create_union_with_invalid_schema(self):
        for invalid_schema in self.invalid_schemas:
            self.builder.clear()
            with self.assertRaises(schema.SchemaParseException):
                self.builder.begin_union(invalid_schema).end()

    def test_create_union_with_dup_primitive_schemas(self):
        with self.assertRaises(schema.SchemaParseException):
            self.builder.begin_union(
                self.builder.create_int(),
                self.builder.create_int()
            ).end()

    def test_create_union_with_dup_named_schemas(self):
        with self.assertRaises(schema.SchemaParseException):
            self.builder.begin_union(
                self.builder.begin_enum(self.name, self.enum_symbols).end(),
                self.builder.begin_fixed(self.name, self.fixed_size).end()
            ).end()

    def test_create_union_with_dup_complex_schemas(self):
        with self.assertRaises(schema.SchemaParseException):
            self.builder.begin_union(
                self.builder.begin_map(self.builder.create_int()).end(),
                self.builder.begin_map(self.builder.create_int()).end()
            ).end()

    def test_create_nullable_type(self):
        # non-union schema type
        actual_json = self.builder.begin_nullable_type(
            self.builder.create_int()
        ).end()
        expected_json = ['null', 'int']
        self.assertEqual(expected_json, actual_json)

        # union schema type
        actual_json = self.builder.begin_nullable_type(
            [self.builder.create_int()]
        ).end()
        expected_json = ['null', 'int']
        self.assertEqual(expected_json, actual_json)

    def test_create_nullable_type_with_default_value(self):
        # non-union schema type
        actual_json = self.builder.begin_nullable_type(
            self.builder.create_int(),
            10
        ).end()
        expected_json = ['int', 'null']
        self.assertEqual(expected_json, actual_json)

        # union schema type
        actual_json = self.builder.begin_nullable_type(
            [self.builder.create_int()],
            10
        ).end()
        expected_json = ['int', 'null']
        self.assertEqual(expected_json, actual_json)

    def test_create_nullable_type_with_null_type(self):
        actual_json = self.builder.begin_nullable_type(
            self.builder.create_null()
        ).end()
        expected_json = 'null'
        self.assertEqual(expected_json, actual_json)

    def test_create_nullable_type_with_nullable_type(self):
        actual_json = self.builder.begin_nullable_type(
            self.builder.begin_union(
                self.builder.create_null(),
                self.builder.create_long()
            ).end(),
            10
        ).end()
        expected_json = ['null', 'long']
        self.assertEqual(expected_json, actual_json)

    def test_create_nullable_type_with_invalid_type(self):
        for invalid_schema in self.invalid_schemas:
            self.builder.clear()
            with self.assertRaises(schema.SchemaParseException):
                self.builder.begin_nullable_type(invalid_schema)

    def test_create_schema_with_preloaded_json(self):
        schema_json = {
            'type': 'record',
            'name': self.name,
            'fields': [
                {'name': 'field', 'type': {'type': 'map', 'values': 'double'}}
            ]
        }
        self.builder.begin_with_schema_json(schema_json)
        self.builder.add_field(
            'field_new',
            self.builder.create_int()
        )
        actual_json = self.builder.end()

        expected_json = schema_json.copy()
        expected_json['fields'].append({'name': 'field_new', 'type': 'int'})

        self.assertEqual(expected_json, actual_json)

    def test_removed_field(self):
        self.builder.begin_record(self.name)
        self.builder.add_field('bar1', self.builder.create_int())
        self.builder.add_field('bar2', self.builder.create_int())
        self.builder.remove_field('bar1')
        actual_json = self.builder.end()

        expected_json = {
            'type': 'record',
            'name': self.name,
            'fields': [{'name': 'bar2', 'type': 'int'}]
        }
        self.assertEqual(expected_json, actual_json)

    def test_removed_nonexistent_field(self):
        schema_json = {
            'type': 'record',
            'name': self.name,
            'fields': [{'name': 'bar2', 'type': 'int'}]
        }
        with self.assertRaises(avro_builder.AvroBuildInvalidOperation):
            self.builder.begin_with_schema_json(schema_json)
            self.builder.remove_field('bar1')
            self.builder.end()


if __name__ == '__main__':
    unittest.main()
