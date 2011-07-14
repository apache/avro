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

#include <stdbool.h>
#include <stdint.h>
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

/*-----------------------------------------------------------------------
 * recursive schemas
 */

/*
 * Recursive schemas are handled specially; the value implementation for
 * an AVRO_LINK schema is simply a wrapper around the value
 * implementation for the link's target schema.  The value methods all
 * delegate to the wrapped implementation.
 *
 * We don't set the target_iface pointer when the link implementation is
 * first created, since we might not have finished creating the
 * implementation for the target schema.  (We create the implementations
 * for child schemas depth-first, so the target schema's implementation
 * won't be done until all of its descendants — including the link
 * schema — have been instantiated.)
 *
 * So anyway, we set the target_iface pointer to NULL at first.  And
 * then in a fix-up stage, once all of the non-link schemas have been
 * instantiated, we go through and set the target_iface pointers for any
 * link schemas we encountered.
 */

typedef struct avro_generic_link_value_iface {
	avro_value_iface_t  iface;

	/** The reference count for this interface. */
	volatile int  refcount;

	/** The schema for this interface. */
	avro_schema_t  schema;

	/** The target's implementation. */
	avro_value_iface_t  *target_iface;
} avro_generic_link_value_iface_t;


static avro_value_iface_t *
avro_generic_link_incref(avro_value_iface_t *viface)
{
	avro_generic_link_value_iface_t  *iface =
	    (avro_generic_link_value_iface_t *) viface;
	avro_refcount_inc(&iface->refcount);
	return viface;
}

static void
avro_generic_link_decref(avro_value_iface_t *viface)
{
	avro_generic_link_value_iface_t  *iface =
	    (avro_generic_link_value_iface_t *) viface;

	if (avro_refcount_dec(&iface->refcount)) {
		avro_value_iface_decref(iface->target_iface);
		avro_freet(avro_generic_link_value_iface_t, iface);
	}
}

static size_t
avro_generic_link_instance_size(const avro_value_iface_t *viface)
{
	AVRO_UNUSED(viface);
	return sizeof(avro_value_t);
}

static int
avro_generic_link_init(const avro_value_iface_t *viface, void *vself)
{
	avro_generic_link_value_iface_t  *iface =
	    (avro_generic_link_value_iface_t *) viface;
	avro_value_t  *self = vself;

	return avro_value_new(iface->target_iface, self);
}

static void
avro_generic_link_done(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = vself;
	avro_value_free(self);
}

static int
avro_generic_link_reset(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = vself;
	return avro_value_reset(self);
}

static avro_type_t
avro_generic_link_get_type(const avro_value_iface_t *viface, const void *vself)
{
	AVRO_UNUSED(viface);
	const avro_value_t  *self = vself;
	return avro_value_get_type(self);
}

static avro_schema_t
avro_generic_link_get_schema(const avro_value_iface_t *viface, const void *vself)
{
	AVRO_UNUSED(viface);
	const avro_value_t  *self = vself;
	return avro_value_get_schema(self);
}

static int
avro_generic_link_get_boolean(const avro_value_iface_t *iface,
			      const void *vself, bool *out)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = vself;
	return avro_value_get_boolean(self, out);
}

static int
avro_generic_link_get_bytes(const avro_value_iface_t *iface,
			    const void *vself, const void **buf, size_t *size)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = vself;
	return avro_value_get_bytes(self, buf, size);
}

static int
avro_generic_link_grab_bytes(const avro_value_iface_t *iface,
			     const void *vself, avro_wrapped_buffer_t *dest)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = vself;
	return avro_value_grab_bytes(self, dest);
}

static int
avro_generic_link_get_double(const avro_value_iface_t *iface,
			     const void *vself, double *out)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = vself;
	return avro_value_get_double(self, out);
}

static int
avro_generic_link_get_float(const avro_value_iface_t *iface,
			    const void *vself, float *out)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = vself;
	return avro_value_get_float(self, out);
}

static int
avro_generic_link_get_int(const avro_value_iface_t *iface,
			  const void *vself, int32_t *out)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = vself;
	return avro_value_get_int(self, out);
}

static int
avro_generic_link_get_long(const avro_value_iface_t *iface,
			   const void *vself, int64_t *out)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = vself;
	return avro_value_get_long(self, out);
}

static int
avro_generic_link_get_null(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = vself;
	return avro_value_get_null(self);
}

static int
avro_generic_link_get_string(const avro_value_iface_t *iface,
			     const void *vself, const char **str, size_t *size)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = vself;
	return avro_value_get_string(self, str, size);
}

static int
avro_generic_link_grab_string(const avro_value_iface_t *iface,
			      const void *vself, avro_wrapped_buffer_t *dest)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = vself;
	return avro_value_grab_string(self, dest);
}

static int
avro_generic_link_get_enum(const avro_value_iface_t *iface,
			   const void *vself, int *out)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = vself;
	return avro_value_get_enum(self, out);
}

static int
avro_generic_link_get_fixed(const avro_value_iface_t *iface,
			    const void *vself, const void **buf, size_t *size)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = vself;
	return avro_value_get_fixed(self, buf, size);
}

static int
avro_generic_link_grab_fixed(const avro_value_iface_t *iface,
			     const void *vself, avro_wrapped_buffer_t *dest)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = vself;
	return avro_value_grab_fixed(self, dest);
}

static int
avro_generic_link_set_boolean(const avro_value_iface_t *iface,
			      void *vself, bool val)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = vself;
	return avro_value_set_boolean(self, val);
}

static int
avro_generic_link_set_bytes(const avro_value_iface_t *iface,
			    void *vself, void *buf, size_t size)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = vself;
	return avro_value_set_bytes(self, buf, size);
}

static int
avro_generic_link_give_bytes(const avro_value_iface_t *iface,
			     void *vself, avro_wrapped_buffer_t *buf)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = vself;
	return avro_value_give_bytes(self, buf);
}

static int
avro_generic_link_set_double(const avro_value_iface_t *iface,
			     void *vself, double val)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = vself;
	return avro_value_set_double(self, val);
}

static int
avro_generic_link_set_float(const avro_value_iface_t *iface,
			    void *vself, float val)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = vself;
	return avro_value_set_float(self, val);
}

static int
avro_generic_link_set_int(const avro_value_iface_t *iface,
			  void *vself, int32_t val)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = vself;
	return avro_value_set_int(self, val);
}

static int
avro_generic_link_set_long(const avro_value_iface_t *iface,
			   void *vself, int64_t val)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = vself;
	return avro_value_set_long(self, val);
}

static int
avro_generic_link_set_null(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = vself;
	return avro_value_set_null(self);
}

static int
avro_generic_link_set_string(const avro_value_iface_t *iface,
			     void *vself, char *str)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = vself;
	return avro_value_set_string(self, str);
}

static int
avro_generic_link_set_string_len(const avro_value_iface_t *iface,
				 void *vself, char *str, size_t size)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = vself;
	return avro_value_set_string_len(self, str, size);
}

static int
avro_generic_link_give_string_len(const avro_value_iface_t *iface,
				  void *vself, avro_wrapped_buffer_t *buf)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = vself;
	return avro_value_give_string_len(self, buf);
}

static int
avro_generic_link_set_enum(const avro_value_iface_t *iface,
			   void *vself, int val)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = vself;
	return avro_value_set_enum(self, val);
}

static int
avro_generic_link_set_fixed(const avro_value_iface_t *iface,
			    void *vself, void *buf, size_t size)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = vself;
	return avro_value_set_fixed(self, buf, size);
}

static int
avro_generic_link_give_fixed(const avro_value_iface_t *iface,
			     void *vself, avro_wrapped_buffer_t *buf)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = vself;
	return avro_value_give_fixed(self, buf);
}

static int
avro_generic_link_get_size(const avro_value_iface_t *iface,
			   const void *vself, size_t *size)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = vself;
	return avro_value_get_size(self, size);
}

static int
avro_generic_link_get_by_index(const avro_value_iface_t *iface,
			       const void *vself, size_t index,
			       avro_value_t *child, const char **name)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = vself;
	return avro_value_get_by_index(self, index, child, name);
}

static int
avro_generic_link_get_by_name(const avro_value_iface_t *iface,
			      const void *vself, const char *name,
			      avro_value_t *child, size_t *index)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = vself;
	return avro_value_get_by_name(self, name, child, index);
}

static int
avro_generic_link_get_discriminant(const avro_value_iface_t *iface,
				   const void *vself, int *out)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = vself;
	return avro_value_get_discriminant(self, out);
}

static int
avro_generic_link_get_current_branch(const avro_value_iface_t *iface,
				     const void *vself, avro_value_t *branch)
{
	AVRO_UNUSED(iface);
	const avro_value_t  *self = vself;
	return avro_value_get_current_branch(self, branch);
}

static int
avro_generic_link_append(const avro_value_iface_t *iface,
			 void *vself, avro_value_t *child_out,
			 size_t *new_index)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = vself;
	return avro_value_append(self, child_out, new_index);
}

static int
avro_generic_link_add(const avro_value_iface_t *iface,
		      void *vself, const char *key,
		      avro_value_t *child, size_t *index, bool *is_new)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = vself;
	return avro_value_add(self, key, child, index, is_new);
}

static int
avro_generic_link_set_branch(const avro_value_iface_t *iface,
			     void *vself, int discriminant,
			     avro_value_t *branch)
{
	AVRO_UNUSED(iface);
	avro_value_t  *self = vself;
	return avro_value_set_branch(self, discriminant, branch);
}

static avro_value_iface_t  AVRO_GENERIC_LINK_CLASS =
{
	/* "class" methods */
	avro_generic_link_incref,
	avro_generic_link_decref,
	avro_generic_link_instance_size,
	/* general "instance" methods */
	avro_generic_link_init,
	avro_generic_link_done,
	avro_generic_link_reset,
	avro_generic_link_get_type,
	avro_generic_link_get_schema,
	/* primitive getters */
	avro_generic_link_get_boolean,
	avro_generic_link_get_bytes,
	avro_generic_link_grab_bytes,
	avro_generic_link_get_double,
	avro_generic_link_get_float,
	avro_generic_link_get_int,
	avro_generic_link_get_long,
	avro_generic_link_get_null,
	avro_generic_link_get_string,
	avro_generic_link_grab_string,
	avro_generic_link_get_enum,
	avro_generic_link_get_fixed,
	avro_generic_link_grab_fixed,
	/* primitive setters */
	avro_generic_link_set_boolean,
	avro_generic_link_set_bytes,
	avro_generic_link_give_bytes,
	avro_generic_link_set_double,
	avro_generic_link_set_float,
	avro_generic_link_set_int,
	avro_generic_link_set_long,
	avro_generic_link_set_null,
	avro_generic_link_set_string,
	avro_generic_link_set_string_len,
	avro_generic_link_give_string_len,
	avro_generic_link_set_enum,
	avro_generic_link_set_fixed,
	avro_generic_link_give_fixed,
	/* compound getters */
	avro_generic_link_get_size,
	avro_generic_link_get_by_index,
	avro_generic_link_get_by_name,
	avro_generic_link_get_discriminant,
	avro_generic_link_get_current_branch,
	/* compound setters */
	avro_generic_link_append,
	avro_generic_link_add,
	avro_generic_link_set_branch
};

static avro_value_iface_t *
avro_generic_link_class(avro_schema_t schema)
{
	if (!is_avro_link(schema)) {
		avro_set_error("Expected link schema");
		return NULL;
	}

	avro_generic_link_value_iface_t  *iface =
		avro_new(avro_generic_link_value_iface_t);
	if (iface == NULL) {
		return NULL;
	}

	memcpy(&iface->iface, &AVRO_GENERIC_LINK_CLASS,
	       sizeof(avro_value_iface_t));
	iface->refcount = 1;
	iface->schema = avro_schema_incref(schema);
	return &iface->iface;
}


/*-----------------------------------------------------------------------
 * schema type dispatcher
 */

typedef struct memoize_state_t {
	avro_memoize_t  mem;
	avro_raw_array_t  links;
} memoize_state_t;

static avro_value_iface_t *
avro_generic_class_from_schema_memoized(avro_schema_t schema,
					void *user_data)
{
	memoize_state_t  *state = user_data;

	/*
	 * If we've already instantiated a value class for this schema,
	 * just return it.
	 */

	avro_value_iface_t  *result = NULL;
	if (avro_memoize_get(&state->mem, schema, NULL, (void **) &result)) {
		return avro_value_iface_incref(result);
	}

	/*
	 * Otherwise instantiate the value class based on the schema
	 * type.
	 */

	switch (schema->type) {
	case AVRO_BOOLEAN:
		result = avro_generic_boolean_class();
		break;
	case AVRO_BYTES:
		result = avro_generic_bytes_class();
		break;
	case AVRO_DOUBLE:
		result = avro_generic_double_class();
		break;
	case AVRO_FLOAT:
		result = avro_generic_float_class();
		break;
	case AVRO_INT32:
		result = avro_generic_int_class();
		break;
	case AVRO_INT64:
		result = avro_generic_long_class();
		break;
	case AVRO_NULL:
		result = avro_generic_null_class();
		break;
	case AVRO_STRING:
		result = avro_generic_string_class();
		break;

	case AVRO_ARRAY:
		result = avro_generic_array_class
		    (schema, avro_generic_class_from_schema_memoized, state);
		break;
	case AVRO_ENUM:
		result = avro_generic_enum_class(schema);
		break;
	case AVRO_FIXED:
		result = avro_generic_fixed_class(schema);
		break;
	case AVRO_MAP:
		result = avro_generic_map_class
		    (schema, avro_generic_class_from_schema_memoized, state);
		break;
	case AVRO_RECORD:
		result = avro_generic_record_class
		    (schema, avro_generic_class_from_schema_memoized, state);
		break;
	case AVRO_UNION:
		result = avro_generic_union_class
		    (schema, avro_generic_class_from_schema_memoized, state);
		break;

	case AVRO_LINK:
		{
			avro_value_iface_t  **link_ptr =
			    avro_raw_array_append(&state->links);
			result = avro_generic_link_class(schema);
			*link_ptr = result;
			break;
		}

	default:
		avro_set_error("Unknown schema type");
		return NULL;
	}

	/*
	 * Add the new value implementation to the memoized state before
	 * we return.
	 */

	avro_memoize_set(&state->mem, schema, NULL, result);
	return result;
}

avro_value_iface_t *
avro_generic_class_from_schema(avro_schema_t schema)
{
	/*
	 * Create a state to keep track of the value implementations
	 * that we create for each subschema.
	 */

	memoize_state_t  state;
	avro_memoize_init(&state.mem);
	avro_raw_array_init(&state.links, sizeof(avro_value_iface_t *));

	/*
	 * Create the value implementations.
	 */

	avro_value_iface_t  *result =
	    avro_generic_class_from_schema_memoized(schema, &state);

	/*
	 * Fix up any link schemas so that their value implementations
	 * point to their target schemas' implementations.
	 */

	size_t  i;
	for (i = 0; i < avro_raw_array_size(&state.links); i++) {
		avro_generic_link_value_iface_t  *link_iface =
		    avro_raw_array_get
		    (&state.links, avro_generic_link_value_iface_t *, i);
		avro_schema_t  target_schema =
		    avro_schema_link_target(link_iface->schema);

		avro_value_iface_t  *target_iface = NULL;
		if (!avro_memoize_get(&state.mem, target_schema, NULL,
				      (void **) &target_iface)) {
			avro_set_error("Never created a value implementation for %s",
				       avro_schema_type_name(target_schema));
			return NULL;
		}

		link_iface->target_iface = avro_value_iface_incref(target_iface);
	}

	/*
	 * And now we can return.
	 */

	avro_memoize_done(&state.mem);
	avro_raw_array_done(&state.links);
	return result;
}
