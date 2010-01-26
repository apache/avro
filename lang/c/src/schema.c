/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0 
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License. 
 */
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>

#include "avro.h"
#include "jansson.h"
#include "st.h"
#include "schema.h"

struct avro_schema_error_t {
	st_table *named_schemas;
	json_error_t json_error;
};

static void avro_schema_init(avro_schema_t schema, avro_type_t type)
{
	schema->type = type;
	schema->class_type = AVRO_SCHEMA;
	schema->refcount = 1;
}

static int is_avro_id(const char *name)
{
	size_t i, len;
	if (name) {
		len = strlen(name);
		if (len < 1) {
			return 0;
		}
		for (i = 0; i < len; i++) {
			if (!(isalpha(name[i])
			      || name[i] == '_' || (i && isdigit(name[i])))) {
				return 0;
			}
		}
		/*
		 * starts with [A-Za-z_] subsequent [A-Za-z0-9_] 
		 */
		return 1;
	}
	return 0;
}

static void avro_schema_free(avro_schema_t schema)
{
	if (is_avro_schema(schema)) {
		switch (avro_typeof(schema)) {
		case AVRO_STRING:
		case AVRO_BYTES:
		case AVRO_INT32:
		case AVRO_INT64:
		case AVRO_FLOAT:
		case AVRO_DOUBLE:
		case AVRO_BOOLEAN:
		case AVRO_NULL:
			/* no memory allocated for primitives */
			return;

		case AVRO_RECORD:{
				struct avro_record_schema_t *record;
				record = avro_schema_to_record(schema);
				free(record->name);
				while (!STAILQ_EMPTY(&record->fields)) {
					struct avro_record_field_t *field;
					field = STAILQ_FIRST(&record->fields);
					STAILQ_REMOVE_HEAD(&record->fields,
							   fields);
					avro_schema_decref(field->type);
					free(field->name);
					free(field);
				}
				free(record);
			}
			break;

		case AVRO_ENUM:{
				struct avro_enum_schema_t *enump;
				enump = avro_schema_to_enum(schema);
				free(enump->name);
				while (!STAILQ_EMPTY(&enump->symbols)) {
					struct avro_enum_symbol_t *symbol;
					symbol = STAILQ_FIRST(&enump->symbols);
					STAILQ_REMOVE_HEAD(&enump->symbols,
							   symbols);
					free(symbol->symbol);
					free(symbol);
				}
				free(enump);
			}
			break;

		case AVRO_FIXED:{
				struct avro_fixed_schema_t *fixed;
				fixed = avro_schema_to_fixed(schema);
				free((char *)fixed->name);
				free(fixed);
			}
			break;

		case AVRO_MAP:{
				struct avro_map_schema_t *map;
				map = avro_schema_to_map(schema);
				avro_schema_decref(map->values);
				free(map);
			}
			break;

		case AVRO_ARRAY:{
				struct avro_array_schema_t *array;
				array = avro_schema_to_array(schema);
				avro_schema_decref(array->items);
				free(array);
			}
			break;
		case AVRO_UNION:{
				struct avro_union_schema_t *unionp;
				unionp = avro_schema_to_union(schema);
				while (!STAILQ_EMPTY(&unionp->branches)) {
					struct avro_union_branch_t *branch;
					branch =
					    STAILQ_FIRST(&unionp->branches);
					STAILQ_REMOVE_HEAD(&unionp->branches,
							   branches);
					avro_schema_decref(branch->schema);
					free(branch);
				}
				free(unionp);
			}
			break;

		case AVRO_LINK:{
				struct avro_link_schema_t *link;
				link = avro_schema_to_link(schema);
				avro_schema_decref(link->to);
				free(link);
			}
			break;
		}
	}
}

avro_schema_t avro_schema_incref(avro_schema_t schema)
{
	if (schema && schema->refcount != (unsigned int)-1) {
		++schema->refcount;
	}
	return schema;
}

void avro_schema_decref(avro_schema_t schema)
{
	if (schema && schema->refcount != (unsigned int)-1
	    && --schema->refcount == 0) {
		avro_schema_free(schema);
	}
}

avro_schema_t avro_schema_string(void)
{
	static struct avro_obj_t obj = {
		.type = AVRO_STRING,
		.class_type = AVRO_SCHEMA,
		.refcount = 1
	};
	return &obj;
}

avro_schema_t avro_schema_bytes(void)
{
	static struct avro_obj_t obj = {
		.type = AVRO_BYTES,
		.class_type = AVRO_SCHEMA,
		.refcount = 1
	};
	return &obj;
}

avro_schema_t avro_schema_int(void)
{
	static struct avro_obj_t obj = {
		.type = AVRO_INT32,
		.class_type = AVRO_SCHEMA,
		.refcount = 1
	};
	return &obj;
}

avro_schema_t avro_schema_long(void)
{
	static struct avro_obj_t obj = {
		.type = AVRO_INT64,
		.class_type = AVRO_SCHEMA,
		.refcount = 1
	};
	return &obj;
}

avro_schema_t avro_schema_float(void)
{
	static struct avro_obj_t obj = {
		.type = AVRO_FLOAT,
		.class_type = AVRO_SCHEMA,
		.refcount = 1
	};
	return &obj;
}

avro_schema_t avro_schema_double(void)
{
	static struct avro_obj_t obj = {
		.type = AVRO_DOUBLE,
		.class_type = AVRO_SCHEMA,
		.refcount = 1
	};
	return &obj;
}

avro_schema_t avro_schema_boolean(void)
{
	static struct avro_obj_t obj = {
		.type = AVRO_BOOLEAN,
		.class_type = AVRO_SCHEMA,
		.refcount = 1
	};
	return &obj;
}

avro_schema_t avro_schema_null(void)
{
	static struct avro_obj_t obj = {
		.type = AVRO_NULL,
		.class_type = AVRO_SCHEMA,
		.refcount = 1
	};
	return &obj;
}

avro_schema_t avro_schema_fixed(const char *name, const int64_t size)
{
	struct avro_fixed_schema_t *fixed =
	    malloc(sizeof(struct avro_fixed_schema_t));
	if (!fixed) {
		return NULL;
	}
	if (!is_avro_id(name)) {
		return NULL;
	}
	fixed->name = strdup(name);
	fixed->size = size;
	avro_schema_init(&fixed->obj, AVRO_FIXED);
	return &fixed->obj;
}

avro_schema_t avro_schema_union(void)
{
	struct avro_union_schema_t *schema =
	    malloc(sizeof(struct avro_union_schema_t));
	if (!schema) {
		return NULL;
	}
	STAILQ_INIT(&schema->branches);
	avro_schema_init(&schema->obj, AVRO_UNION);
	return &schema->obj;
}

int
avro_schema_union_append(const avro_schema_t union_schema,
			 const avro_schema_t schema)
{
	struct avro_union_schema_t *unionp;
	struct avro_union_branch_t *s;

	if (!union_schema || !schema || !is_avro_union(union_schema)) {
		return EINVAL;
	}
	unionp = avro_schema_to_union(union_schema);
	s = malloc(sizeof(struct avro_union_branch_t));
	if (!s) {
		return ENOMEM;
	}
	s->schema = avro_schema_incref(schema);
	STAILQ_INSERT_TAIL(&unionp->branches, s, branches);
	return 0;
}

avro_schema_t avro_schema_array(const avro_schema_t items)
{
	struct avro_array_schema_t *array =
	    malloc(sizeof(struct avro_array_schema_t));
	if (!array) {
		return NULL;
	}
	array->items = avro_schema_incref(items);
	avro_schema_init(&array->obj, AVRO_ARRAY);
	return &array->obj;
}

avro_schema_t avro_schema_map(const avro_schema_t values)
{
	struct avro_map_schema_t *map =
	    malloc(sizeof(struct avro_map_schema_t));
	if (!map) {
		return NULL;
	}
	map->values = avro_schema_incref(values);
	avro_schema_init(&map->obj, AVRO_MAP);
	return &map->obj;
}

avro_schema_t avro_schema_enum(const char *name)
{
	struct avro_enum_schema_t *enump =
	    malloc(sizeof(struct avro_enum_schema_t));
	if (!enump) {
		return NULL;
	}
	if (!is_avro_id(name)) {
		return NULL;
	}
	enump->name = strdup(name);
	if (!enump->name) {
		return NULL;
	}
	STAILQ_INIT(&enump->symbols);
	avro_schema_init(&enump->obj, AVRO_ENUM);
	return &enump->obj;
}

int
avro_schema_enum_symbol_append(const avro_schema_t enum_schema,
			       const char *symbol)
{
	struct avro_enum_schema_t *enump;
	struct avro_enum_symbol_t *enum_symbol;
	if (!enum_schema || !symbol || !is_avro_enum(enum_schema)) {
		return EINVAL;
	}
	enump = avro_schema_to_enum(enum_schema);
	enum_symbol = malloc(sizeof(struct avro_enum_symbol_t));
	if (!enum_symbol) {
		return ENOMEM;
	}
	enum_symbol->symbol = strdup(symbol);
	STAILQ_INSERT_TAIL(&enump->symbols, enum_symbol, symbols);
	return 0;
}

int
avro_schema_record_field_append(const avro_schema_t record_schema,
				const char *field_name,
				const avro_schema_t field_schema)
{
	struct avro_record_schema_t *record;
	struct avro_record_field_t *new_field;
	if (!field_name || !field_schema || !is_avro_schema(record_schema)
	    || !is_avro_record(record_schema) || record_schema == field_schema
	    || !is_avro_id(field_name)) {
		return EINVAL;
	}
	record = avro_schema_to_record(record_schema);
	new_field = malloc(sizeof(struct avro_record_field_t));
	if (!new_field) {
		return ENOMEM;
	}
	new_field->name = strdup(field_name);
	new_field->type = avro_schema_incref(field_schema);
	STAILQ_INSERT_TAIL(&record->fields, new_field, fields);
	return 0;
}

avro_schema_t avro_schema_record(const char *name)
{
	struct avro_record_schema_t *record =
	    malloc(sizeof(struct avro_record_schema_t));
	if (!record) {
		return NULL;
	}
	if (!is_avro_id(name)) {
		return NULL;
	}
	record->name = strdup(name);
	STAILQ_INIT(&record->fields);
	avro_schema_init(&record->obj, AVRO_RECORD);
	return &record->obj;
}

static int
save_named_schemas(const char *name, avro_schema_t schema,
		   avro_schema_error_t * error)
{
	st_table *st = (*error)->named_schemas;
	return st_insert(st, (st_data_t) name, (st_data_t) schema);
}

static avro_schema_t
find_named_schemas(const char *name, avro_schema_error_t * error)
{
	st_table *st = (*error)->named_schemas;
	union {
		avro_schema_t schema;
		st_data_t data;
	} val;
	if (st_lookup(st, (st_data_t) name, &(val.data))) {
		return val.schema;
	}
	return NULL;
};

avro_schema_t avro_schema_link(avro_schema_t to)
{
	struct avro_link_schema_t *link;
	if (!is_avro_named_type(to)) {
		return NULL;
	}
	link = malloc(sizeof(struct avro_link_schema_t));
	if (!link) {
		return NULL;
	}
	link->to = avro_schema_incref(to);
	avro_schema_init(&link->obj, AVRO_LINK);
	return &link->obj;
}

static int
avro_type_from_json_t(json_t * json, avro_type_t * type,
		      avro_schema_error_t * error, avro_schema_t * named_type)
{
	json_t *json_type;
	const char *type_str;

	if (json_is_array(json)) {
		*type = AVRO_UNION;
		return 0;
	} else if (json_is_object(json)) {
		json_type = json_object_get(json, "type");
	} else {
		json_type = json;
	}
	if (!json_is_string(json_type)) {
		return EINVAL;
	}
	type_str = json_string_value(json_type);
	if (!type_str) {
		return EINVAL;
	}
	/*
	 * TODO: gperf/re2c this 
	 */
	if (strcmp(type_str, "string") == 0) {
		*type = AVRO_STRING;
	} else if (strcmp(type_str, "bytes") == 0) {
		*type = AVRO_BYTES;
	} else if (strcmp(type_str, "int") == 0) {
		*type = AVRO_INT32;
	} else if (strcmp(type_str, "long") == 0) {
		*type = AVRO_INT64;
	} else if (strcmp(type_str, "float") == 0) {
		*type = AVRO_FLOAT;
	} else if (strcmp(type_str, "double") == 0) {
		*type = AVRO_DOUBLE;
	} else if (strcmp(type_str, "boolean") == 0) {
		*type = AVRO_BOOLEAN;
	} else if (strcmp(type_str, "null") == 0) {
		*type = AVRO_NULL;
	} else if (strcmp(type_str, "record") == 0) {
		*type = AVRO_RECORD;
	} else if (strcmp(type_str, "enum") == 0) {
		*type = AVRO_ENUM;
	} else if (strcmp(type_str, "array") == 0) {
		*type = AVRO_ARRAY;
	} else if (strcmp(type_str, "map") == 0) {
		*type = AVRO_MAP;
	} else if (strcmp(type_str, "fixed") == 0) {
		*type = AVRO_FIXED;
	} else if ((*named_type = find_named_schemas(type_str, error))) {
		*type = AVRO_LINK;
	} else {
		return EINVAL;
	}
	return 0;
}

static int
avro_schema_from_json_t(json_t * json, avro_schema_t * schema,
			avro_schema_error_t * error)
{
	avro_type_t type = 0;
	unsigned int i;
	avro_schema_t named_schemas = NULL;

	if (avro_type_from_json_t(json, &type, error, &named_schemas)) {
		return EINVAL;
	}

	switch (type) {
	case AVRO_LINK:
		*schema = avro_schema_link(named_schemas);
		break;

	case AVRO_STRING:
		*schema = avro_schema_string();
		break;

	case AVRO_BYTES:
		*schema = avro_schema_bytes();
		break;

	case AVRO_INT32:
		*schema = avro_schema_int();
		break;

	case AVRO_INT64:
		*schema = avro_schema_long();
		break;

	case AVRO_FLOAT:
		*schema = avro_schema_float();
		break;

	case AVRO_DOUBLE:
		*schema = avro_schema_double();
		break;

	case AVRO_BOOLEAN:
		*schema = avro_schema_boolean();
		break;

	case AVRO_NULL:
		*schema = avro_schema_null();
		break;

	case AVRO_RECORD:
		{
			json_t *json_name = json_object_get(json, "name");
			json_t *json_fields = json_object_get(json, "fields");
			unsigned int num_fields;
			const char *record_name;

			if (!json_is_string(json_name)) {
				return EINVAL;
			}
			if (!json_is_array(json_fields)) {
				return EINVAL;
			}
			num_fields = json_array_size(json_fields);
			if (num_fields == 0) {
				return EINVAL;
			}
			record_name = json_string_value(json_name);
			if (!record_name) {
				return EINVAL;
			}
			*schema = avro_schema_record(record_name);
			if (save_named_schemas(record_name, *schema, error)) {
				return ENOMEM;
			}
			for (i = 0; i < num_fields; i++) {
				json_t *json_field =
				    json_array_get(json_fields, i);
				json_t *json_field_name;
				json_t *json_field_type;
				avro_schema_t json_field_type_schema;
				int field_rval;

				if (!json_is_object(json_field)) {
					avro_schema_decref(*schema);
					return EINVAL;
				}
				json_field_name =
				    json_object_get(json_field, "name");
				if (!json_field_name) {
					avro_schema_decref(*schema);
					return EINVAL;
				}
				json_field_type =
				    json_object_get(json_field, "type");
				if (!json_field_type) {
					avro_schema_decref(*schema);
					return EINVAL;
				}
				field_rval =
				    avro_schema_from_json_t(json_field_type,
							    &json_field_type_schema,
							    error);
				if (field_rval) {
					avro_schema_decref(*schema);
					return field_rval;
				}
				field_rval =
				    avro_schema_record_field_append(*schema,
								    json_string_value
								    (json_field_name),
								    json_field_type_schema);
				if (field_rval != 0) {
					avro_schema_decref(*schema);
					return field_rval;
				}
			}
		}
		break;

	case AVRO_ENUM:
		{
			json_t *json_name = json_object_get(json, "name");
			json_t *json_symbols = json_object_get(json, "symbols");
			const char *name;
			unsigned int num_symbols;

			if (!json_is_string(json_name)) {
				return EINVAL;
			}
			if (!json_is_array(json_symbols)) {
				return EINVAL;
			}

			name = json_string_value(json_name);
			if (!name) {
				return EINVAL;
			}
			num_symbols = json_array_size(json_symbols);
			if (num_symbols == 0) {
				return EINVAL;
			}
			*schema = avro_schema_enum(name);
			if (save_named_schemas(name, *schema, error)) {
				return ENOMEM;
			}
			for (i = 0; i < num_symbols; i++) {
				int enum_rval;
				json_t *json_symbol =
				    json_array_get(json_symbols, i);
				const char *symbol;
				if (!json_is_string(json_symbol)) {
					avro_schema_decref(*schema);
					return EINVAL;
				}
				symbol = json_string_value(json_symbol);
				enum_rval =
				    avro_schema_enum_symbol_append(*schema,
								   symbol);
				if (enum_rval != 0) {
					avro_schema_decref(*schema);
					return enum_rval;
				}
			}
		}
		break;

	case AVRO_ARRAY:
		{
			int items_rval;
			json_t *json_items = json_object_get(json, "items");
			avro_schema_t items_schema;
			if (!json_items) {
				return EINVAL;
			}
			items_rval =
			    avro_schema_from_json_t(json_items, &items_schema,
						    error);
			if (items_rval) {
				return items_rval;
			}
			*schema = avro_schema_array(items_schema);
		}
		break;

	case AVRO_MAP:
		{
			int values_rval;
			json_t *json_values = json_object_get(json, "values");
			avro_schema_t values_schema;

			if (!json_values) {
				return EINVAL;
			}
			values_rval =
			    avro_schema_from_json_t(json_values, &values_schema,
						    error);
			if (values_rval) {
				return values_rval;
			}
			*schema = avro_schema_map(values_schema);
		}
		break;

	case AVRO_UNION:
		{
			unsigned int num_schemas = json_array_size(json);
			avro_schema_t s;
			if (num_schemas == 0) {
				return EINVAL;
			}
			*schema = avro_schema_union();
			for (i = 0; i < num_schemas; i++) {
				int schema_rval;
				json_t *schema_json = json_array_get(json, i);
				if (!schema_json) {
					return EINVAL;
				}
				schema_rval =
				    avro_schema_from_json_t(schema_json, &s,
							    error);
				if (schema_rval != 0) {
					avro_schema_decref(*schema);
					return schema_rval;
				}
				schema_rval =
				    avro_schema_union_append(*schema, s);
				if (schema_rval != 0) {
					avro_schema_decref(*schema);
					return schema_rval;
				}
			}
		}
		break;

	case AVRO_FIXED:
		{
			json_t *json_size = json_object_get(json, "size");
			json_t *json_name = json_object_get(json, "name");
			int size;
			const char *name;
			if (!json_is_integer(json_size)) {
				return EINVAL;
			}
			if (!json_is_string(json_name)) {
				return EINVAL;
			}
			size = json_integer_value(json_size);
			name = json_string_value(json_name);
			*schema = avro_schema_fixed(name, size);
			if (save_named_schemas(name, *schema, error)) {
				return ENOMEM;
			}
		}
		break;

	default:
		return EINVAL;
	}
	return 0;
}

int
avro_schema_from_json(const char *jsontext, const int32_t len,
		      avro_schema_t * schema, avro_schema_error_t * e)
{
	json_t *root;
	int rval = 0;
	struct avro_schema_error_t *error;

	if (!jsontext || !schema) {
		return EINVAL;
	}

	error = malloc(sizeof(struct avro_schema_error_t));
	if (!error) {
		return ENOMEM;
	}
	*e = error;

	error->named_schemas = st_init_strtable();
	if (!error->named_schemas) {
		free(error);
		return ENOMEM;
	}

	root = json_loads(jsontext, &error->json_error);
	if (!root) {
		st_free_table(error->named_schemas);
		free(error);
		return EINVAL;
	}

	/*
	 * json_dumpf(root, stderr, 0); 
	 */
	rval = avro_schema_from_json_t(root, schema, e);
	json_decref(root);
	st_free_table(error->named_schemas);
	if (rval == 0) {
		/* no need for an error return */
		free(error);
	}
	return rval;
}

avro_schema_t avro_schema_copy(avro_schema_t schema)
{
	avro_schema_t new_schema = NULL;
	if (!schema) {
		return NULL;
	}
	switch (avro_typeof(schema)) {
	case AVRO_STRING:
	case AVRO_BYTES:
	case AVRO_INT32:
	case AVRO_INT64:
	case AVRO_FLOAT:
	case AVRO_DOUBLE:
	case AVRO_BOOLEAN:
	case AVRO_NULL:
		/*
		 * No need to copy primitives since they're static 
		 */
		new_schema = schema;
		break;

	case AVRO_RECORD:
		{
			struct avro_record_schema_t *record_schema =
			    avro_schema_to_record(schema);
			struct avro_record_field_t *field;
			new_schema = avro_schema_record(record_schema->name);
			for (field = STAILQ_FIRST(&record_schema->fields);
			     field != NULL;
			     field = STAILQ_NEXT(field, fields)) {
				avro_schema_t field_copy =
				    avro_schema_copy(field->type);
				if (!field_copy) {
					avro_schema_decref(new_schema);
					return NULL;
				}
				if (avro_schema_record_field_append
				    (new_schema, field->name, field_copy)) {
					avro_schema_decref(new_schema);
					return NULL;
				}
			}
		}
		break;

	case AVRO_ENUM:
		{
			struct avro_enum_schema_t *enum_schema =
			    avro_schema_to_enum(schema);
			new_schema = avro_schema_enum(enum_schema->name);
			struct avro_enum_symbol_t *enum_symbol;
			for (enum_symbol = STAILQ_FIRST(&enum_schema->symbols);
			     enum_symbol != NULL;
			     enum_symbol = STAILQ_NEXT(enum_symbol, symbols)) {
				if (avro_schema_enum_symbol_append
				    (new_schema, enum_symbol->symbol)) {
					avro_schema_decref(new_schema);
					return NULL;
				}
			}
		}
		break;

	case AVRO_FIXED:
		{
			struct avro_fixed_schema_t *fixed_schema =
			    avro_schema_to_fixed(schema);
			new_schema =
			    avro_schema_fixed(fixed_schema->name,
					      fixed_schema->size);
		}
		break;

	case AVRO_MAP:
		{
			struct avro_map_schema_t *map_schema =
			    avro_schema_to_map(schema);
			avro_schema_t values_copy =
			    avro_schema_copy(map_schema->values);
			if (!values_copy) {
				return NULL;
			}
			new_schema = avro_schema_map(values_copy);
		}
		break;

	case AVRO_ARRAY:
		{
			struct avro_array_schema_t *array_schema =
			    avro_schema_to_array(schema);
			avro_schema_t items_copy =
			    avro_schema_copy(array_schema->items);
			if (!items_copy) {
				return NULL;
			}
			new_schema = avro_schema_array(items_copy);
		}
		break;

	case AVRO_UNION:
		{
			struct avro_union_schema_t *union_schema =
			    avro_schema_to_union(schema);
			struct avro_union_branch_t *s;

			new_schema = avro_schema_union();

			for (s = STAILQ_FIRST(&union_schema->branches);
			     s != NULL; s = STAILQ_NEXT(s, branches)) {
				avro_schema_t schema_copy =
				    avro_schema_copy(s->schema);
				if (avro_schema_union_append
				    (new_schema, schema_copy)) {
					avro_schema_decref(new_schema);
					return NULL;
				}
			}
		}
		break;

	case AVRO_LINK:
		{
			struct avro_link_schema_t *link_schema =
			    avro_schema_to_link(schema);
			/*
			 * TODO: use an avro_schema_copy of to instead of pointing to
			 * the same reference 
			 */
			avro_schema_incref(link_schema->to);
			new_schema = avro_schema_link(link_schema->to);
		}
		break;

	default:
		return NULL;
	}
	return new_schema;
}
