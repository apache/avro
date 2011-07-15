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


/*
 * For generic maps, we need to store the value implementation for the
 * map's elements.
 */

typedef struct avro_generic_map_value_iface {
	avro_value_iface_t  iface;
	volatile int  refcount;
	avro_schema_t  schema;
	avro_value_iface_t  *child_iface;
} avro_generic_map_value_iface_t;

typedef struct avro_generic_map {
	avro_raw_map_t  map;
} avro_generic_map_t;


static avro_value_iface_t *
avro_generic_map_incref(avro_value_iface_t *viface)
{
	avro_generic_map_value_iface_t  *iface =
	    (avro_generic_map_value_iface_t *) viface;
	avro_refcount_inc(&iface->refcount);
	return viface;
}

static void
avro_generic_map_decref(avro_value_iface_t *viface)
{
	avro_generic_map_value_iface_t  *iface =
	    (avro_generic_map_value_iface_t *) viface;
	if (avro_refcount_dec(&iface->refcount)) {
		avro_schema_decref(iface->schema);
		avro_value_iface_decref(iface->child_iface);
		avro_freet(avro_generic_map_value_iface_t, iface);
	}
}

static size_t
avro_generic_map_instance_size(const avro_value_iface_t *viface)
{
	AVRO_UNUSED(viface);
	return sizeof(avro_generic_map_t);
}

static int
avro_generic_map_init(const avro_value_iface_t *viface, void *vself)
{
	const avro_generic_map_value_iface_t  *iface =
	    (const avro_generic_map_value_iface_t *) viface;
	avro_generic_map_t  *self = vself;

	size_t  child_size = avro_value_instance_size(iface->child_iface);
	avro_raw_map_init(&self->map, child_size);
	return 0;
}

static void
avro_generic_map_free_elements(const avro_value_iface_t *child_iface,
			       avro_generic_map_t *self)
{
	size_t  i;
	for (i = 0; i < avro_raw_map_size(&self->map); i++) {
		avro_value_t  element;
		element.iface = child_iface;
		element.self = avro_raw_map_get_raw(&self->map, i);
		avro_value_done(&element);
	}
}

static void
avro_generic_map_done(const avro_value_iface_t *viface, void *vself)
{
	const avro_generic_map_value_iface_t  *iface =
	    (const avro_generic_map_value_iface_t *) viface;
	avro_generic_map_t  *self = vself;
	avro_generic_map_free_elements(iface->child_iface, self);
	avro_raw_map_done(&self->map);
}

static int
avro_generic_map_reset(const avro_value_iface_t *viface, void *vself)
{
	const avro_generic_map_value_iface_t  *iface =
	    (const avro_generic_map_value_iface_t *) viface;
	avro_generic_map_t  *self = vself;
	avro_generic_map_free_elements(iface->child_iface, self);
	avro_raw_map_clear(&self->map);
	return 0;
}

static avro_type_t
avro_generic_map_get_type(const avro_value_iface_t *viface, const void *vself)
{
	AVRO_UNUSED(viface);
	AVRO_UNUSED(vself);
	return AVRO_MAP;
}

static avro_schema_t
avro_generic_map_get_schema(const avro_value_iface_t *viface, const void *vself)
{
	const avro_generic_map_value_iface_t  *iface =
	    (const avro_generic_map_value_iface_t *) viface;
	AVRO_UNUSED(vself);
	return iface->schema;
}

static int
avro_generic_map_get_size(const avro_value_iface_t *viface,
			  const void *vself, size_t *size)
{
	AVRO_UNUSED(viface);
	const avro_generic_map_t  *self = vself;
	if (size != NULL) {
		*size = avro_raw_map_size(&self->map);
	}
	return 0;
}

static int
avro_generic_map_get_by_index(const avro_value_iface_t *viface,
			      const void *vself, size_t index,
			      avro_value_t *child, const char **name)
{
	const avro_generic_map_value_iface_t  *iface =
	    (const avro_generic_map_value_iface_t *) viface;
	const avro_generic_map_t  *self = vself;
	if (index >= avro_raw_map_size(&self->map)) {
		avro_set_error("Map index %zu out of range", index);
		return EINVAL;
	}
	child->iface = iface->child_iface;
	child->self = avro_raw_map_get_raw(&self->map, index);
	if (name != NULL) {
		*name = avro_raw_map_get_key(&self->map, index);
	}
	return 0;
}

static int
avro_generic_map_get_by_name(const avro_value_iface_t *viface,
			     const void *vself, const char *name,
			     avro_value_t *child, size_t *index)
{
	const avro_generic_map_value_iface_t  *iface =
	    (const avro_generic_map_value_iface_t *) viface;
	const avro_generic_map_t  *self = vself;
	child->iface = iface->child_iface;
	child->self = avro_raw_map_get(&self->map, name, index);
	return 0;
}

static int
avro_generic_map_add(const avro_value_iface_t *viface,
		     void *vself, const char *key,
		     avro_value_t *child, size_t *index, int *is_new)
{
	const avro_generic_map_value_iface_t  *iface =
	    (const avro_generic_map_value_iface_t *) viface;
	int  rval;
	avro_generic_map_t  *self = vself;
	child->iface = iface->child_iface;
	rval = avro_raw_map_get_or_create(&self->map, key,
					  &child->self, index);
	if (rval < 0) {
		return -rval;
	}
	if (is_new != NULL) {
		*is_new = rval;
	}
	return 0;
}

static avro_value_iface_t  AVRO_GENERIC_MAP_CLASS =
{
	/* "class" methods */
	avro_generic_map_incref,
	avro_generic_map_decref,
	avro_generic_map_instance_size,
	/* general "instance" methods */
	avro_generic_map_init,
	avro_generic_map_done,
	avro_generic_map_reset,
	avro_generic_map_get_type,
	avro_generic_map_get_schema,
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
	avro_generic_map_get_size,
	avro_generic_map_get_by_index,
	avro_generic_map_get_by_name,
	NULL, /* get_discriminant */
	NULL, /* get_current_branch */
	/* compound setters */
	NULL, /* append */
	avro_generic_map_add,
	NULL  /* set_branch */
};

avro_value_iface_t *
avro_generic_map_class(avro_schema_t schema,
		       avro_value_iface_creator_t creator,
		       void *user_data)
{
	if (!is_avro_map(schema)) {
		avro_set_error("Expected map schema");
		return NULL;
	}

	avro_schema_t  child_schema = avro_schema_array_items(schema);
	avro_value_iface_t  *child_iface = creator(child_schema, user_data);
	if (child_iface == NULL) {
		return NULL;
	}

	size_t  child_size = avro_value_instance_size(child_iface);
	if (child_size == 0) {
		avro_set_error("Map value class must provide instance_size");
		avro_value_iface_decref(child_iface);
		return NULL;
	}

	avro_generic_map_value_iface_t  *iface =
		avro_new(avro_generic_map_value_iface_t);
	if (iface == NULL) {
		return NULL;
	}

	/*
	 * TODO: Maybe check that schema.items matches
	 * child_iface.get_schema?
	 */

	memcpy(&iface->iface, &AVRO_GENERIC_MAP_CLASS,
	       sizeof(avro_value_iface_t));
	iface->refcount = 1;
	iface->schema = avro_schema_incref(schema);
	iface->child_iface = child_iface;
	return &iface->iface;
}
