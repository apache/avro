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

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "avro/allocation.h"
#include "avro/data.h"
#include "avro/errors.h"
#include "avro/generic.h"
#include "avro/refcount.h"
#include "avro/schema.h"
#include "avro/value.h"
#include "avro_private.h"


#ifndef DEBUG_FIELD_OFFSETS
#define DEBUG_FIELD_OFFSETS 0
#endif


/*
 * For generic records, we need to store the value implementation for
 * each field.  We also need to store an offset for each field, since
 * we're going to store the contents of each field directly in the
 * record, rather than via pointers.
 */

typedef struct avro_generic_record_value_iface {
	avro_value_iface_t  iface;

	/** The reference count for this interface. */
	volatile int  refcount;

	/** The schema for this record. */
	avro_schema_t  schema;

	/** The total size of each value struct for this record. */
	size_t  instance_size;

	/** The number of fields in this record.  Yes, we could get this
	 * from schema, but this is easier. */
	size_t  field_count;

	/** The offset of each field within the record struct. */
	size_t  *field_offsets;

	/** The value implementation for each field. */
	avro_value_iface_t  **field_ifaces;
} avro_generic_record_value_iface_t;

typedef struct avro_generic_record {
	/* The rest of the struct is taken up by the inline storage
	 * needed for each field. */
} avro_generic_record_t;


/** Return a pointer to the given field within a record struct. */
#define avro_generic_record_field(iface, rec, index) \
	(((void *) (rec)) + (iface)->field_offsets[(index)])


static avro_value_iface_t *
avro_generic_record_incref(avro_value_iface_t *viface)
{
	avro_generic_record_value_iface_t  *iface =
	    (avro_generic_record_value_iface_t *) viface;
	avro_refcount_inc(&iface->refcount);
	return viface;
}

static void
avro_generic_record_decref(avro_value_iface_t *viface)
{
	avro_generic_record_value_iface_t  *iface =
	    (avro_generic_record_value_iface_t *) viface;

	if (avro_refcount_dec(&iface->refcount)) {
		size_t  i;
		for (i = 0; i < iface->field_count; i++) {
			avro_value_iface_decref(iface->field_ifaces[i]);
		}

		avro_schema_decref(iface->schema);
		avro_free(iface->field_offsets,
			  sizeof(size_t) * iface->field_count);
		avro_free(iface->field_ifaces,
			  sizeof(avro_value_iface_t *) * iface->field_count);

		avro_freet(avro_generic_record_value_iface_t, iface);
	}
}

static size_t
avro_generic_record_instance_size(const avro_value_iface_t *viface)
{
	const avro_generic_record_value_iface_t  *iface =
	    (avro_generic_record_value_iface_t *) viface;
	return iface->instance_size;
}

static int
avro_generic_record_init(const avro_value_iface_t *viface, void *vself)
{
	int  rval;
	const avro_generic_record_value_iface_t  *iface =
	    (const avro_generic_record_value_iface_t *) viface;
	avro_generic_record_t  *self = vself;

	/* Initialize each field */
	size_t  i;
	for (i = 0; i < iface->field_count; i++) {
		check(rval, avro_value_init
		      (iface->field_ifaces[i],
		       avro_generic_record_field(iface, self, i)));
	}

	return 0;
}

static void
avro_generic_record_done(const avro_value_iface_t *viface, void *vself)
{
	const avro_generic_record_value_iface_t  *iface =
	    (const avro_generic_record_value_iface_t *) viface;
	avro_generic_record_t  *self = vself;
	size_t  i;
	for (i = 0; i < iface->field_count; i++) {
		avro_value_t  value = {
			iface->field_ifaces[i],
			avro_generic_record_field(iface, self, i)
		};
		avro_value_done(&value);
	}
}

static int
avro_generic_record_reset(const avro_value_iface_t *viface, void *vself)
{
	const avro_generic_record_value_iface_t  *iface =
	    (const avro_generic_record_value_iface_t *) viface;
	int  rval;
	avro_generic_record_t  *self = vself;
	size_t  i;
	for (i = 0; i < iface->field_count; i++) {
		avro_value_t  value = {
			iface->field_ifaces[i],
			avro_generic_record_field(iface, self, i)
		};
		check(rval, avro_value_reset(&value));
	}
	return 0;
}

static avro_type_t
avro_generic_record_get_type(const avro_value_iface_t *viface, const void *vself)
{
	AVRO_UNUSED(viface);
	AVRO_UNUSED(vself);
	return AVRO_RECORD;
}

static avro_schema_t
avro_generic_record_get_schema(const avro_value_iface_t *viface, const void *vself)
{
	const avro_generic_record_value_iface_t  *iface =
	    (const avro_generic_record_value_iface_t *) viface;
	AVRO_UNUSED(vself);
	return iface->schema;
}

static int
avro_generic_record_get_size(const avro_value_iface_t *viface,
			     const void *vself, size_t *size)
{
	const avro_generic_record_value_iface_t  *iface =
	    (const avro_generic_record_value_iface_t *) viface;
	AVRO_UNUSED(vself);
	if (size != NULL) {
		*size = iface->field_count;
	}
	return 0;
}

static int
avro_generic_record_get_by_index(const avro_value_iface_t *viface,
				 const void *vself, size_t index,
				 avro_value_t *child, const char **name)
{
	const avro_generic_record_value_iface_t  *iface =
	    (const avro_generic_record_value_iface_t *) viface;
	const avro_generic_record_t  *self = vself;
	if (index >= iface->field_count) {
		avro_set_error("Field index %zu out of range", index);
		return EINVAL;
	}
	child->iface = iface->field_ifaces[index];
	child->self = avro_generic_record_field(iface, self, index);

	/*
	 * Grab the field name from the schema if asked for.
	 */
	if (name != NULL) {
		avro_schema_t  schema = iface->schema;
		*name = avro_schema_record_field_name(schema, index);
	}

	return 0;
}

static int
avro_generic_record_get_by_name(const avro_value_iface_t *viface,
				const void *vself, const char *name,
				avro_value_t *child, size_t *index_out)
{
	const avro_generic_record_value_iface_t  *iface =
	    (const avro_generic_record_value_iface_t *) viface;
	const avro_generic_record_t  *self = vself;

	avro_schema_t  schema = iface->schema;
	int  index = avro_schema_record_field_get_index(schema, name);
	if (index < 0) {
		avro_set_error("Unknown record field %s", name);
		return EINVAL;
	}

	child->iface = iface->field_ifaces[index];
	child->self = avro_generic_record_field(iface, self, index);
	if (index_out != NULL) {
		*index_out = index;
	}
	return 0;
}

static avro_value_iface_t  AVRO_GENERIC_RECORD_CLASS =
{
	/* "class" methods */
	avro_generic_record_incref,
	avro_generic_record_decref,
	avro_generic_record_instance_size,
	/* general "instance" methods */
	avro_generic_record_init,
	avro_generic_record_done,
	avro_generic_record_reset,
	avro_generic_record_get_type,
	avro_generic_record_get_schema,
	/* primitive getters */
	NULL, /* get_boolean */
	NULL, /* get_bytes */
	NULL, /* grab_bytes */
	NULL, /* get_double */
	NULL, /* get_float */
	NULL, /* get_int */
	NULL, /* get_long */
	NULL, /* get_null */
	NULL, /* get_string */
	NULL, /* grab_string */
	NULL, /* get_enum */
	NULL, /* get_fixed */
	NULL, /* grab_fixed */
	/* primitive setters */
	NULL, /* set_boolean */
	NULL, /* set_bytes */
	NULL, /* give_bytes */
	NULL, /* set_double */
	NULL, /* set_float */
	NULL, /* set_int */
	NULL, /* set_long */
	NULL, /* set_null */
	NULL, /* set_string */
	NULL, /* set_string_length */
	NULL, /* give_string_length */
	NULL, /* set_enum */
	NULL, /* set_fixed */
	NULL, /* give_fixed */
	/* compound getters */
	avro_generic_record_get_size,
	avro_generic_record_get_by_index,
	avro_generic_record_get_by_name,
	NULL, /* get_discriminant */
	NULL, /* get_current_branch */
	/* compound setters */
	NULL, /* append */
	NULL, /* add */
	NULL  /* set_branch */
};

avro_value_iface_t *
avro_generic_record_class(avro_schema_t schema,
			  avro_value_iface_creator_t creator,
			  void *user_data)
{
	if (!is_avro_record(schema)) {
		avro_set_error("Expected record schema");
		return NULL;
	}

	avro_generic_record_value_iface_t  *iface =
		avro_new(avro_generic_record_value_iface_t);
	if (iface == NULL) {
		return NULL;
	}

	memset(iface, 0, sizeof(avro_generic_record_value_iface_t));
	memcpy(&iface->iface, &AVRO_GENERIC_RECORD_CLASS,
	       sizeof(avro_value_iface_t));
	iface->refcount = 1;
	iface->schema = avro_schema_incref(schema);

	iface->field_count = avro_schema_record_size(schema);
	size_t  field_offsets_size =
		sizeof(size_t) * iface->field_count;
	size_t  field_ifaces_size =
		sizeof(avro_value_iface_t *) * iface->field_count;

	iface->field_offsets = avro_malloc(field_offsets_size);
	if (iface->field_offsets == NULL) {
		goto error;
	}

	iface->field_ifaces = avro_malloc(field_ifaces_size);
	if (iface->field_ifaces == NULL) {
		goto error;
	}

	size_t  next_offset = sizeof(avro_generic_record_t);
#if DEBUG_FIELD_OFFSETS
	fprintf(stderr, "  Record %s\n  Header: Offset 0, size %zu\n",
		avro_schema_type_name(schema),
		sizeof(avro_generic_record_t));
#endif
	size_t  i;
	for (i = 0; i < iface->field_count; i++) {
		avro_schema_t  field_schema =
		    avro_schema_record_field_get_by_index(schema, i);

		iface->field_offsets[i] = next_offset;

		iface->field_ifaces[i] = creator(field_schema, user_data);
		if (iface->field_ifaces[i] == NULL) {
			goto error;
		}

		size_t  field_size =
		    avro_value_instance_size(iface->field_ifaces[i]);
		if (field_size == 0) {
			avro_set_error("Record field class must provide instance_size");
			goto error;
		}

#if DEBUG_FIELD_OFFSETS
		fprintf(stderr, "  Field %zu (%s): Offset %zu, size %zu\n",
			i, avro_schema_type_name(field_schema),
			next_offset, field_size);
#endif
		next_offset += field_size;
	}

	iface->instance_size = next_offset;
#if DEBUG_FIELD_OFFSETS
	fprintf(stderr, "  TOTAL SIZE: %zu\n", next_offset);
#endif

	return &iface->iface;

error:
	avro_schema_decref(iface->schema);
	if (iface->field_offsets != NULL) {
		avro_free(iface->field_offsets, field_offsets_size);
	}
	if (iface->field_ifaces != NULL) {
		for (i = 0; i < iface->field_count; i++) {
			if (iface->field_ifaces[i] != NULL) {
				avro_value_iface_decref(iface->field_ifaces[i]);
			}
		}
		avro_free(iface->field_ifaces, field_ifaces_size);
	}
	avro_freet(avro_generic_record_value_iface_t, iface);
	return NULL;
}
