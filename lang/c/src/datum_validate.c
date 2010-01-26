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
#include <limits.h>
#include <errno.h>
#include <string.h>
#include "schema.h"
#include "datum.h"
#include "st.h"

struct validate_st {
	avro_schema_t expected_schema;
	int rval;
};

static int
schema_map_validate_foreach(char *key, avro_datum_t datum,
			    struct validate_st *vst)
{
	if (!avro_schema_datum_validate(vst->expected_schema, datum)) {
		vst->rval = 0;
		return ST_STOP;
	}
	return ST_CONTINUE;
}

int
avro_schema_datum_validate(avro_schema_t expected_schema, avro_datum_t datum)
{
	if (!is_avro_schema(expected_schema) || !is_avro_datum(datum)) {
		return EINVAL;
	}

	switch (avro_typeof(expected_schema)) {
	case AVRO_NULL:
		return is_avro_null(datum);

	case AVRO_BOOLEAN:
		return is_avro_boolean(datum);

	case AVRO_STRING:
		return is_avro_string(datum);

	case AVRO_BYTES:
		return is_avro_bytes(datum);

	case AVRO_INT32:
		return is_avro_int32(datum)
		    || (is_avro_int64(datum)
			&& (INT_MIN <= avro_datum_to_int64(datum)->i64
			    && avro_datum_to_int64(datum)->i64 <= INT_MAX));

	case AVRO_INT64:
		return is_avro_int32(datum) || is_avro_int64(datum);

	case AVRO_FLOAT:
		return is_avro_int32(datum) || is_avro_int64(datum)
		    || is_avro_float(datum);

	case AVRO_DOUBLE:
		return is_avro_int32(datum) || is_avro_int64(datum)
		    || is_avro_float(datum) || is_avro_double(datum);

	case AVRO_FIXED:
		return (is_avro_fixed(datum)
			&& (avro_schema_to_fixed(expected_schema)->size ==
			    avro_datum_to_fixed(datum)->size));

	case AVRO_ENUM:
		{
			struct avro_enum_schema_t *enump =
			    avro_schema_to_enum(expected_schema);
			struct avro_enum_symbol_t *symbol =
			    STAILQ_FIRST(&enump->symbols);
			while (symbol) {
				if (!strcmp
				    (symbol->symbol,
				     avro_datum_to_enum(datum)->symbol)) {
					return 1;
				}
				symbol = STAILQ_NEXT(symbol, symbols);
			}
			return 0;
		}
		break;

	case AVRO_ARRAY:
		{
			if (is_avro_array(datum)) {
				struct avro_array_datum_t *array =
				    avro_datum_to_array(datum);
				struct avro_array_element_t *el =
				    STAILQ_FIRST(&array->els);
				while (el) {
					if (!avro_schema_datum_validate
					    ((avro_schema_to_array
					      (expected_schema))->items,
					     el->datum)) {
						return 0;
					}
					el = STAILQ_NEXT(el, els);
				}
				return 1;
			}
			return 0;
		}
		break;

	case AVRO_MAP:
		if (is_avro_map(datum)) {
			struct validate_st vst =
			    { avro_schema_to_map(expected_schema)->values, 1 };
			st_foreach(avro_datum_to_map(datum)->map,
				   schema_map_validate_foreach,
				   (st_data_t) & vst);
			return vst.rval;
		}
		break;

	case AVRO_UNION:
		{
			struct avro_union_schema_t *union_schema =
			    avro_schema_to_union(expected_schema);
			struct avro_union_branch_t *branch;

			for (branch = STAILQ_FIRST(&union_schema->branches);
			     branch != NULL;
			     branch = STAILQ_NEXT(branch, branches)) {
				if (avro_schema_datum_validate
				    (branch->schema, datum)) {
					return 1;
				}
			}
			return 0;
		}
		break;

	case AVRO_RECORD:
		if (is_avro_record(datum)) {
			struct avro_record_schema_t *record_schema =
			    avro_schema_to_record(expected_schema);
			struct avro_record_field_t *field;
			for (field = STAILQ_FIRST(&record_schema->fields);
			     field != NULL;
			     field = STAILQ_NEXT(field, fields)) {
				int field_rval;
				avro_datum_t field_datum;
				field_rval =
				    avro_record_get(datum, field->name,
						    &field_datum);
				if (field_rval) {
					/*
					 * TODO: check for default values 
					 */
					return field_rval;
				}
				if (!avro_schema_datum_validate
				    (field->type, field_datum)) {
					return 0;
				}
			}
			return 1;
		}
		break;

	case AVRO_LINK:
		{
			return
			    avro_schema_datum_validate((avro_schema_to_link
							(expected_schema))->to,
						       datum);
		}
		break;
	}
	return 0;
}
