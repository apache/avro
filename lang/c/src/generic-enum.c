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
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "avro/allocation.h"
#include "avro/errors.h"
#include "avro/generic.h"
#include "avro/refcount.h"
#include "avro/schema.h"
#include "avro/value.h"
#include "avro_private.h"


/*-----------------------------------------------------------------------
 * enum
 */

typedef struct avro_generic_enum_value_iface {
	avro_value_iface_t  iface;
	volatile int  refcount;
	avro_schema_t  schema;
} avro_generic_enum_value_iface_t;


static avro_value_iface_t *
avro_generic_enum_incref(avro_value_iface_t *viface)
{
	avro_generic_enum_value_iface_t  *iface =
	    (avro_generic_enum_value_iface_t *) viface;
	avro_refcount_inc(&iface->refcount);
	return viface;
}

static void
avro_generic_enum_decref(avro_value_iface_t *viface)
{
	avro_generic_enum_value_iface_t  *iface =
	    (avro_generic_enum_value_iface_t *) viface;
	if (avro_refcount_dec(&iface->refcount)) {
		avro_schema_decref(iface->schema);
		avro_freet(avro_generic_enum_value_iface_t, iface);
	}
}

static size_t
avro_generic_enum_instance_size(const avro_value_iface_t *viface)
{
	AVRO_UNUSED(viface);
	return sizeof(int);
}

static int
avro_generic_enum_init(const avro_value_iface_t *viface, void *vself)
{
	AVRO_UNUSED(viface);
	int  *self = vself;
	*self = 0;
	return 0;
}

static void
avro_generic_enum_done(const avro_value_iface_t *viface, void *vself)
{
	AVRO_UNUSED(viface);
	AVRO_UNUSED(vself);
}

static int
avro_generic_enum_reset(const avro_value_iface_t *viface, void *vself)
{
	AVRO_UNUSED(viface);
	int  *self = vself;
	*self = 0;
	return 0;
}

static avro_type_t
avro_generic_enum_get_type(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return AVRO_ENUM;
}

static avro_schema_t
avro_generic_enum_get_schema(const avro_value_iface_t *viface, const void *vself)
{
	const avro_generic_enum_value_iface_t  *iface =
	    (const avro_generic_enum_value_iface_t *) viface;
	AVRO_UNUSED(vself);
	return iface->schema;
}

static int
avro_generic_enum_get(const avro_value_iface_t *viface,
		      const void *vself, int *out)
{
	AVRO_UNUSED(viface);
	const int  *self = vself;
	*out = *self;
	return 0;
}

static int
avro_generic_enum_set(const avro_value_iface_t *viface,
		      void *vself, int val)
{
	AVRO_UNUSED(viface);
	int  *self = vself;
	*self = val;
	return 0;
}

static avro_value_iface_t  AVRO_GENERIC_ENUM_CLASS =
{
	/* "class" methods */
	avro_generic_enum_incref,
	avro_generic_enum_decref,
	avro_generic_enum_instance_size,
	/* general "instance" methods */
	avro_generic_enum_init,
	avro_generic_enum_done,
	avro_generic_enum_reset,
	avro_generic_enum_get_type,
	avro_generic_enum_get_schema,
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
	avro_generic_enum_get,
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
	avro_generic_enum_set,
	NULL, /* set_fixed */
	NULL, /* give_fixed */
	/* compound getters */
	NULL, /* get_size */
	NULL, /* get_by_index */
	NULL, /* get_by_name */
	NULL, /* get_discriminant */
	NULL, /* get_current_branch */
	/* compound setters */
	NULL, /* append */
	NULL, /* add */
	NULL  /* set_branch */
};

avro_value_iface_t *avro_generic_enum_class(avro_schema_t schema)
{
	if (!is_avro_enum(schema)) {
		avro_set_error("Expected enum schema");
		return NULL;
	}

	avro_generic_enum_value_iface_t  *iface =
		avro_new(avro_generic_enum_value_iface_t);
	if (iface == NULL) {
		return NULL;
	}

	memcpy(&iface->iface, &AVRO_GENERIC_ENUM_CLASS,
	       sizeof(avro_value_iface_t));
	iface->refcount = 1;
	iface->schema = avro_schema_incref(schema);
	return &iface->iface;
}

int avro_generic_enum_new(avro_value_t *value, bool val)
{
	int  rval;
	check(rval, avro_value_new(&AVRO_GENERIC_ENUM_CLASS, value));
	return avro_generic_enum_set(value->iface, value->self, val);
}
