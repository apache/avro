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


#ifndef DEBUG_BRANCHES
#define DEBUG_BRANCHES 0
#endif


/*
 * For generic unions, we need to store the value implementation for
 * each branch, just like for generic records.  However, for unions, we
 * can only have one branch active at a time, so we can reuse the space
 * in the union struct, just like is done with C unions.
 */

typedef struct avro_generic_union_value_iface {
	avro_value_iface_t  iface;

	/** The reference count for this interface. */
	volatile int  refcount;

	/** The schema for this union. */
	avro_schema_t  schema;

	/** The total size of each value struct for this union. */
	size_t  instance_size;

	/** The number of branches in this union.  Yes, we could get
	 * this from schema, but this is easier. */
	size_t  branch_count;

	/** The value implementation for each branch. */
	avro_value_iface_t  **branch_ifaces;
} avro_generic_union_value_iface_t;

typedef struct avro_generic_union {
	/** The currently active branch of the union.  -1 if no branch
	 * is selected. */
	int  discriminant;

	/* The rest of the struct is taken up by the inline storage
	 * needed for the active branch. */
} avro_generic_union_t;


/** Return the child interface for the active branch. */
#define avro_generic_union_branch_iface(iface, _union) \
	((iface)->branch_ifaces[(_union)->discriminant])

/** Return a pointer to the active branch within a union struct. */
#define avro_generic_union_branch(_union) \
	(((void *) (_union)) + sizeof(avro_generic_union_t))


static avro_value_iface_t *
avro_generic_union_incref(avro_value_iface_t *viface)
{
	avro_generic_union_value_iface_t  *iface =
	    (avro_generic_union_value_iface_t *) viface;
	avro_refcount_inc(&iface->refcount);
	return viface;
}

static void
avro_generic_union_decref(avro_value_iface_t *viface)
{
	avro_generic_union_value_iface_t  *iface =
	    (avro_generic_union_value_iface_t *) viface;

	if (avro_refcount_dec(&iface->refcount)) {
		size_t  i;
		for (i = 0; i < iface->branch_count; i++) {
			avro_value_iface_decref(iface->branch_ifaces[i]);
		}

		avro_schema_decref(iface->schema);
		avro_free(iface->branch_ifaces,
			  sizeof(avro_value_iface_t *) * iface->branch_count);

		avro_freet(avro_generic_union_value_iface_t, iface);
	}
}

static size_t
avro_generic_union_instance_size(const avro_value_iface_t *viface)
{
	const avro_generic_union_value_iface_t  *iface =
	    (const avro_generic_union_value_iface_t *) viface;
	return iface->instance_size;
}

static int
avro_generic_union_init(const avro_value_iface_t *viface, void *vself)
{
	AVRO_UNUSED(viface);
	avro_generic_union_t  *self = vself;
	self->discriminant = -1;
	return 0;
}

static void
avro_generic_union_done(const avro_value_iface_t *viface, void *vself)
{
	const avro_generic_union_value_iface_t  *iface =
	    (const avro_generic_union_value_iface_t *) viface;
	avro_generic_union_t  *self = vself;
	if (self->discriminant >= 0) {
#if DEBUG_BRANCHES
		fprintf(stderr, "Finalizing branch %d\n",
			self->discriminant);
#endif
		avro_value_t  value = {
			avro_generic_union_branch_iface(iface, self),
			avro_generic_union_branch(self)
		};
		avro_value_done(&value);
		self->discriminant = -1;
	}
}

static int
avro_generic_union_reset(const avro_value_iface_t *viface, void *vself)
{
	const avro_generic_union_value_iface_t  *iface =
	    (const avro_generic_union_value_iface_t *) viface;
	avro_generic_union_t  *self = vself;
	/* Keep the same branch selected, for the common case that we're
	 * about to reuse it. */
	if (self->discriminant >= 0) {
#if DEBUG_BRANCHES
		fprintf(stderr, "Resetting branch %d\n",
			self->discriminant);
#endif
		avro_value_t  value = {
			avro_generic_union_branch_iface(iface, self),
			avro_generic_union_branch(self)
		};
		return avro_value_reset(&value);
	}
	return 0;
}

static avro_type_t
avro_generic_union_get_type(const avro_value_iface_t *viface, const void *vself)
{
	AVRO_UNUSED(viface);
	AVRO_UNUSED(vself);
	return AVRO_UNION;
}

static avro_schema_t
avro_generic_union_get_schema(const avro_value_iface_t *viface, const void *vself)
{
	const avro_generic_union_value_iface_t  *iface =
	    (const avro_generic_union_value_iface_t *) viface;
	AVRO_UNUSED(vself);
	return iface->schema;
}

static int
avro_generic_union_get_discriminant(const avro_value_iface_t *viface,
				    const void *vself, int *out)
{
	AVRO_UNUSED(viface);
	const avro_generic_union_t  *self = vself;
	*out = self->discriminant;
	return 0;
}

static int
avro_generic_union_get_current_branch(const avro_value_iface_t *viface,
				      const void *vself, avro_value_t *branch)
{
	const avro_generic_union_value_iface_t  *iface =
	    (const avro_generic_union_value_iface_t *) viface;
	const avro_generic_union_t  *self = vself;
	if (self->discriminant < 0) {
		avro_set_error("Union has no selected branch");
		return EINVAL;
	}
	branch->iface = avro_generic_union_branch_iface(iface, self);
	branch->self = avro_generic_union_branch(self);
	return 0;
}

static int
avro_generic_union_set_branch(const avro_value_iface_t *viface,
			      void *vself, int discriminant,
			      avro_value_t *branch)
{
	const avro_generic_union_value_iface_t  *iface =
	    (const avro_generic_union_value_iface_t *) viface;
	int  rval;
	avro_generic_union_t  *self = vself;

#if DEBUG_BRANCHES
	fprintf(stderr, "Selecting branch %d (was %d)\n",
		discriminant, self->discriminant);
#endif

	/*
	 * If the new desired branch is different than the currently
	 * active one, then finalize the old branch and initialize the
	 * new one.
	 */
	if (self->discriminant != discriminant) {
		if (self->discriminant >= 0) {
#if DEBUG_BRANCHES
			fprintf(stderr, "Finalizing branch %d\n",
				self->discriminant);
#endif
			avro_value_t  value = {
				avro_generic_union_branch_iface(iface, self),
				avro_generic_union_branch(self)
			};
			avro_value_done(&value);
		}
		self->discriminant = discriminant;
		if (discriminant >= 0) {
#if DEBUG_BRANCHES
			fprintf(stderr, "Initializing branch %d\n",
				self->discriminant);
#endif
			check(rval, avro_value_init
			      (avro_generic_union_branch_iface(iface, self),
			       avro_generic_union_branch(self)));
		}
	}

	if (branch != NULL) {
		branch->iface = avro_generic_union_branch_iface(iface, self);
		branch->self = avro_generic_union_branch(self);
	}

	return 0;
}

static avro_value_iface_t  AVRO_GENERIC_UNION_CLASS =
{
	/* "class" methods */
	avro_generic_union_incref,
	avro_generic_union_decref,
	avro_generic_union_instance_size,
	/* general "instance" methods */
	avro_generic_union_init,
	avro_generic_union_done,
	avro_generic_union_reset,
	avro_generic_union_get_type,
	avro_generic_union_get_schema,
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
	NULL, /* get_size */
	NULL, /* get_by_index */
	NULL, /* get_by_name */
	avro_generic_union_get_discriminant,
	avro_generic_union_get_current_branch,
	/* compound setters */
	NULL, /* append */
	NULL, /* add */
	avro_generic_union_set_branch
};

avro_value_iface_t *
avro_generic_union_class(avro_schema_t schema,
			 avro_value_iface_creator_t creator,
			 void *user_data)
{
	if (!is_avro_union(schema)) {
		avro_set_error("Expected union schema");
		return NULL;
	}

	avro_generic_union_value_iface_t  *iface =
		avro_new(avro_generic_union_value_iface_t);
	if (iface == NULL) {
		return NULL;
	}

	memset(iface, 0, sizeof(avro_generic_union_value_iface_t));
	memcpy(&iface->iface, &AVRO_GENERIC_UNION_CLASS,
	       sizeof(avro_value_iface_t));
	iface->refcount = 1;
	iface->schema = avro_schema_incref(schema);

	iface->branch_count = avro_schema_union_size(schema);
	size_t  branch_ifaces_size =
		sizeof(avro_value_iface_t *) * iface->branch_count;

	iface->branch_ifaces = avro_malloc(branch_ifaces_size);
	if (iface->branch_ifaces == NULL) {
		goto error;
	}

	size_t  max_branch_size = 0;
	size_t  i;
	for (i = 0; i < iface->branch_count; i++) {
		avro_schema_t  branch_schema =
		    avro_schema_union_branch(schema, i);

		iface->branch_ifaces[i] = creator(branch_schema, user_data);
		if (iface->branch_ifaces[i] == NULL) {
			goto error;
		}

		size_t  branch_size =
		    avro_value_instance_size(iface->branch_ifaces[i]);
		if (branch_size == 0) {
			avro_set_error("Union branch class must provide instance_size");
			goto error;
		}

#if DEBUG_BRANCHES
		fprintf(stderr, "Branch %zu, size %zu\n",
			i, branch_size);
#endif

		if (branch_size > max_branch_size) {
			max_branch_size = branch_size;
		}
	}

	iface->instance_size =
		sizeof(avro_generic_union_t) + max_branch_size;
#if DEBUG_BRANCHES
	fprintf(stderr, "MAX BRANCH SIZE: %zu\n", max_branch_size);
#endif

	return &iface->iface;

error:
	avro_schema_decref(iface->schema);
	if (iface->branch_ifaces != NULL) {
		for (i = 0; i < iface->branch_count; i++) {
			if (iface->branch_ifaces[i] != NULL) {
				avro_value_iface_decref(iface->branch_ifaces[i]);
			}
		}
		avro_free(iface->branch_ifaces, branch_ifaces_size);
	}
	avro_freet(avro_generic_union_value_iface_t, iface);
	return NULL;
}
