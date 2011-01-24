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

#include "avro_private.h"
#include "allocation.h"
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>

#include "jansson.h"
#include "st.h"
#include "schema.h"

#define DEFAULT_TABLE_SIZE 32

struct avro_schema_error_t_ {
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

static int record_free_foreach(int i, struct avro_record_field_t *field,
			       void *arg)
{
	AVRO_UNUSED(i);
	AVRO_UNUSED(arg);

	avro_str_free(field->name);
	avro_schema_decref(field->type);
	avro_freet(struct avro_record_field_t, field);
	return ST_DELETE;
}

static int enum_free_foreach(int i, char *sym, void *arg)
{
	AVRO_UNUSED(i);
	AVRO_UNUSED(arg);

	avro_str_free(sym);
	return ST_DELETE;
}

static int union_free_foreach(int i, avro_schema_t schema, void *arg)
{
	AVRO_UNUSED(i);
	AVRO_UNUSED(arg);

	avro_schema_decref(schema);
	return ST_DELETE;
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
				avro_str_free(record->name);
				if (record->space) {
					avro_str_free(record->space);
				}
				st_foreach(record->fields, record_free_foreach,
					   0);
				st_free_table(record->fields_byname);
				st_free_table(record->fields);
				avro_freet(struct avro_record_schema_t, record);
			}
			break;

		case AVRO_ENUM:{
				struct avro_enum_schema_t *enump;
				enump = avro_schema_to_enum(schema);
				avro_str_free(enump->name);
				st_foreach(enump->symbols, enum_free_foreach,
					   0);
				st_free_table(enump->symbols);
				st_free_table(enump->symbols_byname);
				avro_freet(struct avro_enum_schema_t, enump);
			}
			break;

		case AVRO_FIXED:{
				struct avro_fixed_schema_t *fixed;
				fixed = avro_schema_to_fixed(schema);
				avro_str_free((char *) fixed->name);
				avro_freet(struct avro_fixed_schema_t, fixed);
			}
			break;

		case AVRO_MAP:{
				struct avro_map_schema_t *map;
				map = avro_schema_to_map(schema);
				avro_schema_decref(map->values);
				avro_freet(struct avro_map_schema_t, map);
			}
			break;

		case AVRO_ARRAY:{
				struct avro_array_schema_t *array;
				array = avro_schema_to_array(schema);
				avro_schema_decref(array->items);
				avro_freet(struct avro_array_schema_t, array);
			}
			break;
		case AVRO_UNION:{
				struct avro_union_schema_t *unionp;
				unionp = avro_schema_to_union(schema);
				st_foreach(unionp->branches, union_free_foreach,
					   0);
				st_free_table(unionp->branches);
				st_free_table(unionp->branches_byname);
				avro_freet(struct avro_union_schema_t, unionp);
			}
			break;

		case AVRO_LINK:{
				struct avro_link_schema_t *link;
				link = avro_schema_to_link(schema);
				avro_schema_decref(link->to);
				avro_freet(struct avro_link_schema_t, link);
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
	return avro_schema_incref(&obj);
}

avro_schema_t avro_schema_bytes(void)
{
	static struct avro_obj_t obj = {
		.type = AVRO_BYTES,
		.class_type = AVRO_SCHEMA,
		.refcount = 1
	};
	return avro_schema_incref(&obj);
}

avro_schema_t avro_schema_int(void)
{
	static struct avro_obj_t obj = {
		.type = AVRO_INT32,
		.class_type = AVRO_SCHEMA,
		.refcount = 1
	};
	return avro_schema_incref(&obj);
}

avro_schema_t avro_schema_long(void)
{
	static struct avro_obj_t obj = {
		.type = AVRO_INT64,
		.class_type = AVRO_SCHEMA,
		.refcount = 1
	};
	return avro_schema_incref(&obj);
}

avro_schema_t avro_schema_float(void)
{
	static struct avro_obj_t obj = {
		.type = AVRO_FLOAT,
		.class_type = AVRO_SCHEMA,
		.refcount = 1
	};
	return avro_schema_incref(&obj);
}

avro_schema_t avro_schema_double(void)
{
	static struct avro_obj_t obj = {
		.type = AVRO_DOUBLE,
		.class_type = AVRO_SCHEMA,
		.refcount = 1
	};
	return avro_schema_incref(&obj);
}

avro_schema_t avro_schema_boolean(void)
{
	static struct avro_obj_t obj = {
		.type = AVRO_BOOLEAN,
		.class_type = AVRO_SCHEMA,
		.refcount = 1
	};
	return avro_schema_incref(&obj);
}

avro_schema_t avro_schema_null(void)
{
	static struct avro_obj_t obj = {
		.type = AVRO_NULL,
		.class_type = AVRO_SCHEMA,
		.refcount = 1
	};
	return avro_schema_incref(&obj);
}

avro_schema_t avro_schema_fixed(const char *name, const int64_t size)
{
	struct avro_fixed_schema_t *fixed =
	    avro_new(struct avro_fixed_schema_t);
	if (!fixed) {
		return NULL;
	}
	if (!is_avro_id(name)) {
		return NULL;
	}
	fixed->name = avro_strdup(name);
	fixed->size = size;
	avro_schema_init(&fixed->obj, AVRO_FIXED);
	return &fixed->obj;
}

int64_t avro_schema_fixed_size(const avro_schema_t fixed)
{
	return avro_schema_to_fixed(fixed)->size;
}

avro_schema_t avro_schema_union(void)
{
	struct avro_union_schema_t *schema =
	    avro_new(struct avro_union_schema_t);
	if (!schema) {
		return NULL;
	}
	schema->branches = st_init_numtable_with_size(DEFAULT_TABLE_SIZE);
	if (!schema->branches) {
		avro_freet(struct avro_union_schema_t, schema);
		return NULL;
	}
	schema->branches_byname =
	    st_init_strtable_with_size(DEFAULT_TABLE_SIZE);
	if (!schema->branches_byname) {
		st_free_table(schema->branches);
		avro_freet(struct avro_union_schema_t, schema);
		return NULL;
	}

	avro_schema_init(&schema->obj, AVRO_UNION);
	return &schema->obj;
}

int
avro_schema_union_append(const avro_schema_t union_schema,
			 const avro_schema_t schema)
{
	struct avro_union_schema_t *unionp;
	if (!union_schema || !schema || !is_avro_union(union_schema)) {
		return EINVAL;
	}
	unionp = avro_schema_to_union(union_schema);
	int  new_index = unionp->branches->num_entries;
	st_insert(unionp->branches, new_index, (st_data_t) schema);
	const char *name = avro_schema_type_name(schema);
	st_insert(unionp->branches_byname, (st_data_t) name,
		  (st_data_t) new_index);
	avro_schema_incref(schema);
	return 0;
}

avro_schema_t avro_schema_union_branch(avro_schema_t unionp,
				       int branch_index)
{
	union {
		st_data_t data;
		avro_schema_t schema;
	} val;
	st_lookup(avro_schema_to_union(unionp)->branches,
		  branch_index, &val.data);
	return val.schema;
}

avro_schema_t avro_schema_union_branch_by_name
(avro_schema_t unionp, int *branch_index, const char *name)
{
	union {
		st_data_t data;
		int  branch_index;
	} val;

	if (!st_lookup(avro_schema_to_union(unionp)->branches_byname,
		       (st_data_t) name, &val.data)) {
		return NULL;
	}

	if (branch_index != NULL) {
		*branch_index = val.branch_index;
	}
	return avro_schema_union_branch(unionp, val.branch_index);
}

avro_schema_t avro_schema_array(const avro_schema_t items)
{
	struct avro_array_schema_t *array =
	    avro_new(struct avro_array_schema_t);
	if (!array) {
		return NULL;
	}
	array->items = avro_schema_incref(items);
	avro_schema_init(&array->obj, AVRO_ARRAY);
	return &array->obj;
}

avro_schema_t avro_schema_array_items(avro_schema_t array)
{
	return avro_schema_to_array(array)->items;
}

avro_schema_t avro_schema_map(const avro_schema_t values)
{
	struct avro_map_schema_t *map =
	    avro_new(struct avro_map_schema_t);
	if (!map) {
		return NULL;
	}
	map->values = avro_schema_incref(values);
	avro_schema_init(&map->obj, AVRO_MAP);
	return &map->obj;
}

avro_schema_t avro_schema_map_values(avro_schema_t map)
{
	return avro_schema_to_map(map)->values;
}

avro_schema_t avro_schema_enum(const char *name)
{
	struct avro_enum_schema_t *enump;

	if (!is_avro_id(name)) {
		return NULL;
	}
	enump = avro_new(struct avro_enum_schema_t);
	if (!enump) {
		return NULL;
	}
	enump->name = avro_strdup(name);
	if (!enump->name) {
		avro_freet(struct avro_enum_schema_t, enump);
		return NULL;
	}
	enump->symbols = st_init_numtable_with_size(DEFAULT_TABLE_SIZE);
	if (!enump->symbols) {
		avro_str_free(enump->name);
		avro_freet(struct avro_enum_schema_t, enump);
		return NULL;
	}
	enump->symbols_byname = st_init_strtable_with_size(DEFAULT_TABLE_SIZE);
	if (!enump->symbols_byname) {
		st_free_table(enump->symbols);
		avro_str_free(enump->name);
		avro_freet(struct avro_enum_schema_t, enump);
		return NULL;
	}
	avro_schema_init(&enump->obj, AVRO_ENUM);
	return &enump->obj;
}

const char *avro_schema_enum_get(const avro_schema_t enump,
				 int index)
{
	union {
		st_data_t data;
		char *sym;
	} val;
	st_lookup(avro_schema_to_enum(enump)->symbols, index, &val.data);
	return val.sym;
}

int avro_schema_enum_get_by_name(const avro_schema_t enump,
				 const char *symbol_name)
{
	union {
		st_data_t data;
		long idx;
	} val;

	return
	    (st_lookup(avro_schema_to_enum(enump)->symbols_byname,
		       (st_data_t) symbol_name, &val.data))?
	    val.idx:
	    -1;
}

int
avro_schema_enum_symbol_append(const avro_schema_t enum_schema,
			       const char *symbol)
{
	struct avro_enum_schema_t *enump;
	char *sym;
	long idx;
	if (!enum_schema || !symbol || !is_avro_enum(enum_schema)) {
		return EINVAL;
	}
	enump = avro_schema_to_enum(enum_schema);
	sym = avro_strdup(symbol);
	if (!sym) {
		return ENOMEM;
	}
	idx = enump->symbols->num_entries;
	st_insert(enump->symbols, (st_data_t) idx, (st_data_t) sym);
	st_insert(enump->symbols_byname, (st_data_t) sym, (st_data_t) idx);
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
	new_field = avro_new(struct avro_record_field_t);
	if (!new_field) {
		return ENOMEM;
	}
	new_field->name = avro_strdup(field_name);
	new_field->type = avro_schema_incref(field_schema);
	st_insert(record->fields, record->fields->num_entries,
		  (st_data_t) new_field);
	st_insert(record->fields_byname, (st_data_t) new_field->name,
		  (st_data_t) new_field);
	return 0;
}

avro_schema_t avro_schema_record(const char *name, const char *space)
{
	struct avro_record_schema_t *record;
	if (!is_avro_id(name)) {
		return NULL;
	}
	record = avro_new(struct avro_record_schema_t);
	if (!record) {
		return NULL;
	}
	record->name = avro_strdup(name);
	if (!record->name) {
		avro_freet(struct avro_record_schema_t, record);
		return NULL;
	}
	record->space = space ? avro_strdup(space) : NULL;
	if (space && !record->space) {
		avro_str_free(record->name);
		avro_freet(struct avro_record_schema_t, record);
		return NULL;
	}
	record->fields = st_init_numtable_with_size(DEFAULT_TABLE_SIZE);
	if (!record->fields) {
		if (record->space) {
			avro_str_free(record->space);
		}
		avro_str_free(record->name);
		avro_freet(struct avro_record_schema_t, record);
		return NULL;
	}
	record->fields_byname = st_init_strtable_with_size(DEFAULT_TABLE_SIZE);
	if (!record->fields_byname) {
		st_free_table(record->fields);
		if (record->space) {
			avro_str_free(record->space);
		}
		avro_str_free(record->name);
		avro_freet(struct avro_record_schema_t, record);
		return NULL;
	}

	avro_schema_init(&record->obj, AVRO_RECORD);
	return &record->obj;
}

size_t avro_schema_record_size(const avro_schema_t record)
{
	return avro_schema_to_record(record)->fields->num_entries;
}

avro_schema_t avro_schema_record_field_get(const avro_schema_t
					   record, const char *field_name)
{
	union {
		st_data_t data;
		struct avro_record_field_t *field;
	} val;
	st_lookup(avro_schema_to_record(record)->fields_byname,
		  (st_data_t) field_name, &val.data);
	return val.field->type;
}

const char *avro_schema_record_field_name(const avro_schema_t schema, int index)
{
	union {
		st_data_t data;
		struct avro_record_field_t *field;
	} val;
	st_lookup(avro_schema_to_record(schema)->fields, index, &val.data);
	return val.field->name;
}

avro_schema_t avro_schema_record_field_get_by_index
(const avro_schema_t record, int index)
{
	union {
		st_data_t data;
		struct avro_record_field_t *field;
	} val;
	st_lookup(avro_schema_to_record(record)->fields, index, &val.data);
	return val.field->type;
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
	link = avro_new(struct avro_link_schema_t);
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
			json_t *json_namespace =
			    json_object_get(json, "namespace");
			json_t *json_fields = json_object_get(json, "fields");
			unsigned int num_fields;
			const char *record_name;
			const char *record_namespace;

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
			if (json_is_string(json_namespace)) {
				record_namespace =
				    json_string_value(json_namespace);
			} else {
				record_namespace = NULL;
			}
			*schema =
			    avro_schema_record(record_name, record_namespace);
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
				avro_schema_decref(json_field_type_schema);
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
			avro_schema_decref(items_schema);
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
			avro_schema_decref(values_schema);
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
				avro_schema_decref(s);
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
	avro_schema_error_t error;

	AVRO_UNUSED(len);

	if (!jsontext || !schema) {
		return EINVAL;
	}

	error = avro_new(struct avro_schema_error_t_);
	if (!error) {
		return ENOMEM;
	}
	*e = error;

	error->named_schemas = st_init_strtable_with_size(DEFAULT_TABLE_SIZE);
	if (!error->named_schemas) {
		avro_freet(struct avro_schema_error_t_, error);
		return ENOMEM;
	}

	root = json_loads(jsontext, &error->json_error);
	if (!root) {
		st_free_table(error->named_schemas);
		avro_freet(struct avro_schema_error_t_, error);
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
		avro_freet(struct avro_schema_error_t_, error);
	}
	return rval;
}

avro_schema_t avro_schema_copy(avro_schema_t schema)
{
	long i;
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
			new_schema =
			    avro_schema_record(record_schema->name,
					       record_schema->space);
			for (i = 0; i < record_schema->fields->num_entries; i++) {
				union {
					st_data_t data;
					struct avro_record_field_t *field;
				} val;
				st_lookup(record_schema->fields, i, &val.data);
				avro_schema_t type_copy =
				    avro_schema_copy(val.field->type);
				avro_schema_record_field_append(new_schema,
								val.field->name,
								type_copy);
			}
		}
		break;

	case AVRO_ENUM:
		{
			struct avro_enum_schema_t *enum_schema =
			    avro_schema_to_enum(schema);
			new_schema = avro_schema_enum(enum_schema->name);
			for (i = 0; i < enum_schema->symbols->num_entries; i++) {
				union {
					st_data_t data;
					char *sym;
				} val;
				st_lookup(enum_schema->symbols, i, &val.data);
				avro_schema_enum_symbol_append(new_schema,
							       val.sym);
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

			new_schema = avro_schema_union();
			for (i = 0; i < union_schema->branches->num_entries;
			     i++) {
				avro_schema_t schema_copy;
				union {
					st_data_t data;
					avro_schema_t schema;
				} val;
				st_lookup(union_schema->branches, i, &val.data);
				schema_copy = avro_schema_copy(val.schema);
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

avro_schema_t avro_schema_get_subschema(const avro_schema_t schema,
         const char *name)
{
 if (is_avro_record(schema)) {
   const struct avro_record_schema_t *rschema =
     avro_schema_to_record(schema);
   union {
     st_data_t data;
     struct avro_record_field_t *field;
   } field;

   if (st_lookup(rschema->fields_byname,
           (st_data_t) name, &field.data))
   {
     return field.field->type;
   }

   return NULL;
 } else if (is_avro_union(schema)) {
   const struct avro_union_schema_t *uschema =
     avro_schema_to_union(schema);
   long i;

   for (i = 0; i < uschema->branches->num_entries; i++) {
     union {
       st_data_t data;
       avro_schema_t schema;
     } val;
     st_lookup(uschema->branches, i, &val.data);
     if (strcmp(avro_schema_type_name(val.schema),
          name) == 0)
     {
       return val.schema;
     }
   }

   return NULL;
 } else if (is_avro_array(schema)) {
   if (strcmp(name, "[]") == 0) {
     const struct avro_array_schema_t *aschema =
       avro_schema_to_array(schema);
     return aschema->items;
   }

   return NULL;
 } else if (is_avro_map(schema)) {
   if (strcmp(name, "{}") == 0) {
     const struct avro_map_schema_t *mschema =
       avro_schema_to_map(schema);
     return mschema->values;
   }

   return NULL;
 }

 return NULL;
}

const char *avro_schema_name(const avro_schema_t schema)
{
	if (is_avro_record(schema)) {
		return (avro_schema_to_record(schema))->name;
	} else if (is_avro_enum(schema)) {
		return (avro_schema_to_enum(schema))->name;
	} else if (is_avro_fixed(schema)) {
		return (avro_schema_to_fixed(schema))->name;
	}
	return NULL;
}

const char *avro_schema_type_name(const avro_schema_t schema)
{
 if (is_avro_record(schema)) {
   return (avro_schema_to_record(schema))->name;
 } else if (is_avro_enum(schema)) {
   return (avro_schema_to_enum(schema))->name;
 } else if (is_avro_fixed(schema)) {
   return (avro_schema_to_fixed(schema))->name;
 } else if (is_avro_union(schema)) {
   return "union";
 } else if (is_avro_array(schema)) {
   return "array";
 } else if (is_avro_map(schema)) {
   return "map";
 } else if (is_avro_int32(schema)) {
   return "int32";
 } else if (is_avro_int64(schema)) {
   return "int64";
 } else if (is_avro_float(schema)) {
   return "float";
 } else if (is_avro_double(schema)) {
   return "double";
 } else if (is_avro_boolean(schema)) {
   return "boolean";
 } else if (is_avro_null(schema)) {
   return "null";
 } else if (is_avro_string(schema)) {
   return "string";
 } else if (is_avro_bytes(schema)) {
   return "bytes";
 }
 return NULL;
}

avro_datum_t avro_datum_from_schema(const avro_schema_t schema)
{
	if (!is_avro_schema(schema)) {
		return NULL;
	}

	switch (avro_typeof(schema)) {
		case AVRO_STRING:
			return avro_wrapstring("");

		case AVRO_BYTES:
			return avro_wrapbytes("", 0);

		case AVRO_INT32:
			return avro_int32(0);

		case AVRO_INT64:
			return avro_int64(0);

		case AVRO_FLOAT:
			return avro_float(0);

		case AVRO_DOUBLE:
			return avro_double(0);

		case AVRO_BOOLEAN:
			return avro_boolean(0);

		case AVRO_NULL:
			return avro_null();

		case AVRO_RECORD:
			{
				const struct avro_record_schema_t *record_schema =
				    avro_schema_to_record(schema);

				avro_datum_t  rec =
				    avro_record(record_schema->name,
						record_schema->space);

				int  i;
				for (i = 0; i < record_schema->fields->num_entries; i++) {
					union {
						st_data_t data;
						struct avro_record_field_t *field;
					} val;
					st_lookup(record_schema->fields, i, &val.data);

					avro_datum_t  field =
					    avro_datum_from_schema(val.field->type);
					avro_record_set(rec, val.field->name, field);
					avro_datum_decref(field);
				}

				return rec;
			}

		case AVRO_ENUM:
			{
				const struct avro_enum_schema_t *enum_schema =
				    avro_schema_to_enum(schema);
				return avro_enum(enum_schema->name, 0);
			}

		case AVRO_FIXED:
			{
				const struct avro_fixed_schema_t *fixed_schema =
				    avro_schema_to_fixed(schema);
				return avro_wrapfixed(fixed_schema->name, "", 0);
			}

		case AVRO_MAP:
			return avro_map();

		case AVRO_ARRAY:
			return avro_array();

		case AVRO_UNION:
			return avro_union(-1, NULL);

		case AVRO_LINK:
			{
				const struct avro_link_schema_t *link_schema =
				    avro_schema_to_link(schema);
				return avro_datum_from_schema(link_schema->to);
			}

		default:
			return NULL;
	}
}

/* simple helper for writing strings */
static int avro_write_str(avro_writer_t out, const char *str)
{
	return avro_write(out, (char *)str, strlen(str));
}

static int write_field(avro_writer_t out, const struct avro_record_field_t *field)
{
	int rval;
	check(rval, avro_write_str(out, "{\"name\":\""));
	check(rval, avro_write_str(out, field->name));
	check(rval, avro_write_str(out, "\",\"type\":"));
	check(rval, avro_schema_to_json(field->type, out));
	return avro_write_str(out, "}");
}

static int write_record(avro_writer_t out, const struct avro_record_schema_t *record)
{
	int rval;
	long i;

	check(rval, avro_write_str(out, "{\"type\":\"record\",\"name\":\""));
	check(rval, avro_write_str(out, record->name));
	check(rval, avro_write_str(out, "\","));
	if (record->space) {
		check(rval, avro_write_str(out, "\"namespace\":\""));
		check(rval, avro_write_str(out, record->space));
		check(rval, avro_write_str(out, "\","));
	}
	check(rval, avro_write_str(out, "\"fields\":["));
	for (i = 0; i < record->fields->num_entries; i++) {
		union {
			st_data_t data;
			struct avro_record_field_t *field;
		} val;
		st_lookup(record->fields, i, &val.data);
		if (i) {
			check(rval, avro_write_str(out, ","));
		}
		check(rval, write_field(out, val.field));
	}
	return avro_write_str(out, "]}");
}

static int write_enum(avro_writer_t out, const struct avro_enum_schema_t *enump)
{
	int rval;
	long i;
	check(rval, avro_write_str(out, "{\"type\":\"enum\",\"name\":\""));
	check(rval, avro_write_str(out, enump->name));
	check(rval, avro_write_str(out, "\",\"symbols\":["));

	for (i = 0; i < enump->symbols->num_entries; i++) {
		union {
			st_data_t data;
			char *sym;
		} val;
		st_lookup(enump->symbols, i, &val.data);
		if (i) {
			check(rval, avro_write_str(out, ","));
		}
		check(rval, avro_write_str(out, "\""));
		check(rval, avro_write_str(out, val.sym));
		check(rval, avro_write_str(out, "\""));
	}
	return avro_write_str(out, "]}");
}
static int write_fixed(avro_writer_t out, const struct avro_fixed_schema_t *fixed)
{
	int rval;
	char size[16];
	check(rval, avro_write_str(out, "{\"type\":\"fixed\",\"name\":\""));
	check(rval, avro_write_str(out, fixed->name));
	check(rval, avro_write_str(out, "\",\"size\":"));
	snprintf(size, sizeof(size), "%"PRId64, fixed->size);
	check(rval, avro_write_str(out, size));
	return avro_write_str(out, "}");
}
static int write_map(avro_writer_t out, const struct avro_map_schema_t *map)
{
	int rval;
	check(rval, avro_write_str(out, "{\"type\":\"map\",\"values\":"));
	check(rval, avro_schema_to_json(map->values, out));
	return avro_write_str(out, "}");
}
static int write_array(avro_writer_t out, const struct avro_array_schema_t *array)
{
	int rval;
	check(rval, avro_write_str(out, "{\"type\":\"array\",\"items\":"));
	check(rval, avro_schema_to_json(array->items, out));
	return avro_write_str(out, "}");
}
static int write_union(avro_writer_t out, const struct avro_union_schema_t *unionp)
{
	int rval;
	long i;
	check(rval, avro_write_str(out, "["));

	for (i = 0; i < unionp->branches->num_entries; i++) {
		union {
			st_data_t data;
			avro_schema_t schema;
		} val;
		st_lookup(unionp->branches, i, &val.data);
		if (i) {
			check(rval, avro_write_str(out, ","));
		}
		check(rval, avro_schema_to_json(val.schema, out));
	}
	return avro_write_str(out, "]");
}
static int write_link(avro_writer_t out, const struct avro_link_schema_t *link)
{
	int rval;
	check(rval, avro_write_str(out, "\""));
	check(rval, avro_write_str(out, avro_schema_name(link->to)));
	return avro_write_str(out, "\"");
}

int avro_schema_to_json(const avro_schema_t schema, avro_writer_t out)
{
	int rval;

	if (!is_avro_schema(schema) || !out) {
		return EINVAL;
	}

	if (is_avro_primitive(schema)) {
		check(rval, avro_write_str(out, "{\"type\":\""));
	}

	switch (avro_typeof(schema)) {
	case AVRO_STRING:
		check(rval, avro_write_str(out, "string"));
		break;
	case AVRO_BYTES:
		check(rval, avro_write_str(out, "bytes"));
		break;
	case AVRO_INT32:
		check(rval, avro_write_str(out, "int"));
		break;
	case AVRO_INT64:
		check(rval, avro_write_str(out, "long"));
		break;
	case AVRO_FLOAT:
		check(rval, avro_write_str(out, "float"));
		break;
	case AVRO_DOUBLE:
		check(rval, avro_write_str(out, "double"));
		break;
	case AVRO_BOOLEAN:
		check(rval, avro_write_str(out, "boolean"));
		break;
	case AVRO_NULL:
		check(rval, avro_write_str(out, "null"));
		break;
	case AVRO_RECORD:
		return write_record(out, avro_schema_to_record(schema));
	case AVRO_ENUM:
		return write_enum(out, avro_schema_to_enum(schema));
	case AVRO_FIXED:
		return write_fixed(out, avro_schema_to_fixed(schema));
	case AVRO_MAP:
		return write_map(out, avro_schema_to_map(schema));
	case AVRO_ARRAY:
		return write_array(out, avro_schema_to_array(schema));
	case AVRO_UNION:
		return write_union(out, avro_schema_to_union(schema));
	case AVRO_LINK:
		return write_link(out, avro_schema_to_link(schema));
	}

	if (is_avro_primitive(schema)) {
		return avro_write_str(out, "\"}");
	}
	return EINVAL;
}
