/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	 You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

#include "avro.h"
#include "avro/consumer.h"
#include "avro_errors.h"
#include "avro_private.h"
#include "allocation.h"


#if !defined(DEBUG_RESOLVER)
#define DEBUG_RESOLVER 0
#endif

#if DEBUG_RESOLVER
#include <stdio.h>
#define debug(...) { fprintf(stderr, __VA_ARGS__); fprintf(stderr, "\n"); }
#else
#define debug(...) /* no debug output */
#endif


typedef struct avro_resolver_t  avro_resolver_t;

struct avro_resolver_t {
	avro_consumer_t  parent;

	/* The reader schema for this resolver. */
	avro_schema_t  rschema;

	/* An array of any child resolvers needed for the subschemas of
	 * wschema */
	avro_consumer_t  **child_resolvers;

	/* If the reader and writer schemas are records, this field
	 * contains a mapping from writer field indices to reader field
	 * indices. */
	int  *index_mapping;

	/* The number of elements in the child_resolvers and
	 * index_mapping arrays. */
	size_t  num_children;

	/* If the reader schema is a union, but the writer schema is
	 * not, this field indicates which branch of the reader union
	 * should be selected. */
	int  reader_union_branch;
};


static void
avro_resolver_free(avro_consumer_t *consumer)
{
	avro_resolver_t  *resolver = (avro_resolver_t *) consumer;
	avro_schema_decref(resolver->parent.schema);
	avro_schema_decref(resolver->rschema);
	if (resolver->child_resolvers) {
		unsigned int  i;
		for (i = 0; i < resolver->num_children; i++) {
			avro_consumer_t  *child = resolver->child_resolvers[i];
			if (child) {
				avro_consumer_free(child);
			}
		}
		avro_free(resolver->child_resolvers,
			  sizeof(avro_resolver_t *) * resolver->num_children);
	}
	if (resolver->index_mapping) {
		avro_free(resolver->index_mapping,
			  sizeof(int) * resolver->num_children);
	}
	avro_freet(avro_resolver_t, resolver);
}


/**
 * Create a new avro_resolver_t instance.  You must fill in the callback
 * pointers that are appropriate for the writer schema after this
 * function returns.
 */

static avro_resolver_t *
avro_resolver_create(avro_schema_t wschema,
		     avro_schema_t rschema)
{
	avro_resolver_t  *resolver = avro_new(avro_resolver_t);
	memset(resolver, 0, sizeof(avro_resolver_t));

	resolver->parent.free = avro_resolver_free;
	resolver->parent.schema = avro_schema_incref(wschema);
	resolver->rschema = avro_schema_incref(rschema);
	resolver->reader_union_branch = -1;
	return resolver;
}


static avro_datum_t
avro_resolver_get_real_dest(avro_resolver_t *resolver, avro_datum_t dest)
{
	if (resolver->reader_union_branch < 0) {
		/*
		 * The reader schema isn't a union, so use the dest
		 * field as-is.
		 */

		return dest;
	}

	debug("Retrieving union branch %d for %s value",
	      resolver->reader_union_branch,
	      avro_schema_type_name(resolver->parent.schema));

	avro_datum_t  branch = NULL;
	avro_union_set_discriminant
	    (dest, resolver->reader_union_branch, &branch);
	return branch;
}


/*-----------------------------------------------------------------------
 * Reader unions
 */

/*
 * For each Avro type, we have to check whether the reader schema on its
 * own is compatible, and whether the reader is a union that contains a
 * compatible type.  The macros in this section help us perform both of
 * these checks with less code.
 */


/**
 * A helper macro that handles the case where neither writer nor reader
 * are unions.  Uses @ref check_func to see if the two schemas are
 * compatible.
 */

#define check_non_union(wschema, rschema, check_func)		\
{								\
	avro_resolver_t  *self = NULL;				\
	int  rc = check_func(&self, wschema, rschema,		\
			     rschema);				\
	if (self) {						\
		debug("Non-union schemas %s (writer) "		\
		      "and %s (reader) match",			\
		      avro_schema_type_name(wschema),		\
		      avro_schema_type_name(rschema));		\
								\
		self->reader_union_branch = -1;			\
		return &self->parent;				\
        }							\
								\
        if (rc) {						\
		return NULL;					\
	}							\
    }


/**
 * Helper macro that handles the case where the reader is a union, and
 * the writer is not.  Checks each branch of the reader union schema,
 * looking for the first branch that is compatible with the writer
 * schema.  The @ref check_func argument should be a function that can
 * check the compatiblity of each branch schema.
 */

#define check_reader_union(wschema, rschema, check_func)		\
{									\
	if (!is_avro_union(rschema)) {					\
		break;							\
	}								\
									\
	debug("Checking reader union schema");				\
	size_t  num_branches = avro_schema_union_size(rschema);		\
	unsigned int  i;						\
									\
	for (i = 0; i < num_branches; i++) {				\
		avro_schema_t  branch_schema =				\
		    avro_schema_union_branch(rschema, i);		\
		avro_resolver_t  *self = NULL;				\
		int  rc = check_func(&self, wschema, branch_schema,	\
				     rschema);				\
		if (self) {						\
			debug("Reader union branch %d (%s) "		\
			      "and writer %s match",			\
			      i, avro_schema_type_name(branch_schema),	\
			      avro_schema_type_name(wschema));		\
			self->reader_union_branch = i;			\
			return &self->parent;				\
		}							\
									\
		if (rc) {						\
			return NULL;					\
		}							\
	}								\
									\
	debug("No reader union branches match");			\
}

/**
 * A helper macro that defines wraps together check_non_union and
 * check_reader_union for a simple (non-union) writer schema type.
 */

#define check_simple_writer(wschema, rschema, type_name)		\
{									\
	check_non_union(wschema, rschema, try_##type_name);		\
	check_reader_union(wschema, rschema, try_##type_name);		\
	debug("Writer %s doesn't match reader %s",			\
	      avro_schema_type_name(wschema),				\
	      avro_schema_type_name(rschema));				\
	avro_set_error("Cannot store a " #type_name " into schema %s",	\
		       avro_schema_type_name(rschema));			\
	return NULL;							\
}


/*-----------------------------------------------------------------------
 * primitives
 */

static int
avro_resolver_boolean_value(avro_consumer_t *consumer, int value,
			    void *user_data)
{
	avro_resolver_t  *resolver = (avro_resolver_t *) consumer;
	avro_datum_t  ud_dest = user_data;
	avro_datum_t  dest = avro_resolver_get_real_dest(resolver, ud_dest);
	debug("Storing %s into %p", value? "TRUE": "FALSE", dest);
	return avro_boolean_set(dest, value);
}

static int
try_boolean(avro_resolver_t **resolver,
	    avro_schema_t wschema, avro_schema_t rschema,
	    avro_schema_t root_rschema)
{
	if (is_avro_boolean(rschema)) {
		*resolver = avro_resolver_create(wschema, root_rschema);
		(*resolver)->parent.boolean_value = avro_resolver_boolean_value;
	}
	return 0;
}


static void
free_bytes(void *ptr, size_t sz)
{
	/*
	 * The binary encoder class allocates bytes values with an extra
	 * byte, so that they're NUL terminated.
	 */
	avro_free(ptr, sz+1);
}

static int
avro_resolver_bytes_value(avro_consumer_t *consumer,
			  const void *value, size_t value_len,
			  void *user_data)
{
	avro_resolver_t  *resolver = (avro_resolver_t *) consumer;
	avro_datum_t  ud_dest = user_data;
	avro_datum_t  dest = avro_resolver_get_real_dest(resolver, ud_dest);
	debug("Storing %zu bytes into %p", value_len, dest);
	return avro_givebytes_set(dest, value, value_len, free_bytes);
}

static int
try_bytes(avro_resolver_t **resolver,
	  avro_schema_t wschema, avro_schema_t rschema,
	  avro_schema_t root_rschema)
{
	if (is_avro_bytes(rschema)) {
		*resolver = avro_resolver_create(wschema, root_rschema);
		(*resolver)->parent.bytes_value = avro_resolver_bytes_value;
	}
	return 0;
}


static int
avro_resolver_double_value(avro_consumer_t *consumer, double value,
			   void *user_data)
{
	avro_resolver_t  *resolver = (avro_resolver_t *) consumer;
	avro_datum_t  ud_dest = user_data;
	avro_datum_t  dest = avro_resolver_get_real_dest(resolver, ud_dest);
	debug("Storing %le into %p", value, dest);
	return avro_double_set(dest, value);
}

static int
try_double(avro_resolver_t **resolver,
	   avro_schema_t wschema, avro_schema_t rschema,
	   avro_schema_t root_rschema)
{
	if (is_avro_double(rschema)) {
		*resolver = avro_resolver_create(wschema, root_rschema);
		(*resolver)->parent.double_value = avro_resolver_double_value;
	}
	return 0;
}


static int
avro_resolver_float_value(avro_consumer_t *consumer, float value,
			  void *user_data)
{
	avro_resolver_t  *resolver = (avro_resolver_t *) consumer;
	avro_datum_t  ud_dest = user_data;
	avro_datum_t  dest = avro_resolver_get_real_dest(resolver, ud_dest);
	debug("Storing %e into %p", value, dest);
	return avro_float_set(dest, value);
}

static int
try_float(avro_resolver_t **resolver,
	  avro_schema_t wschema, avro_schema_t rschema,
	  avro_schema_t root_rschema)
{
	if (is_avro_float(rschema)) {
		*resolver = avro_resolver_create(wschema, root_rschema);
		(*resolver)->parent.float_value = avro_resolver_float_value;
	}
	return 0;
}


static int
avro_resolver_int_value(avro_consumer_t *consumer, int32_t value,
			void *user_data)
{
	avro_resolver_t  *resolver = (avro_resolver_t *) consumer;
	avro_datum_t  ud_dest = user_data;
	avro_datum_t  dest = avro_resolver_get_real_dest(resolver, ud_dest);
	debug("Storing %" PRId32 " into %p", value, dest);
	return avro_int32_set(dest, value);
}

static int
try_int(avro_resolver_t **resolver,
	avro_schema_t wschema, avro_schema_t rschema,
	avro_schema_t root_rschema)
{
	if (is_avro_int32(rschema)) {
		*resolver = avro_resolver_create(wschema, root_rschema);
		(*resolver)->parent.int_value = avro_resolver_int_value;
	}
	return 0;
}


static int
avro_resolver_long_value(avro_consumer_t *consumer, int64_t value,
			 void *user_data)
{
	avro_resolver_t  *resolver = (avro_resolver_t *) consumer;
	avro_datum_t  ud_dest = user_data;
	avro_datum_t  dest = avro_resolver_get_real_dest(resolver, ud_dest);
	debug("Storing %" PRId64 " into %p", value, dest);
	return avro_int64_set(dest, value);
}

static int
try_long(avro_resolver_t **resolver,
	 avro_schema_t wschema, avro_schema_t rschema,
	 avro_schema_t root_rschema)
{
	if (is_avro_int64(rschema)) {
		*resolver = avro_resolver_create(wschema, root_rschema);
		(*resolver)->parent.long_value = avro_resolver_long_value;
	}
	return 0;
}


static int
avro_resolver_null_value(avro_consumer_t *consumer, void *user_data)
{
#if DEBUG_RESOLVER
	avro_resolver_t  *resolver = (avro_resolver_t *) consumer;
	avro_datum_t  ud_dest = user_data;
	avro_datum_t  dest = avro_resolver_get_real_dest(resolver, ud_dest);
	debug("Storing null into %p", dest);
#else
	AVRO_UNUSED(consumer);
	AVRO_UNUSED(user_data);
#endif
	return 0;
}

static int
try_null(avro_resolver_t **resolver,
	 avro_schema_t wschema, avro_schema_t rschema,
	 avro_schema_t root_rschema)
{
	if (is_avro_null(rschema)) {
		*resolver = avro_resolver_create(wschema, root_rschema);
		(*resolver)->parent.null_value = avro_resolver_null_value;
	}
	return 0;
}


static int
avro_resolver_string_value(avro_consumer_t *consumer,
			   const void *value, size_t value_len,
			   void *user_data)
{
	AVRO_UNUSED(value_len);
	avro_resolver_t  *resolver = (avro_resolver_t *) consumer;
	avro_datum_t  ud_dest = user_data;
	avro_datum_t  dest = avro_resolver_get_real_dest(resolver, ud_dest);
	debug("Storing \"%s\" into %p", (const char *) value, dest);
	return avro_givestring_set(dest, value, avro_alloc_free);
}

static int
try_string(avro_resolver_t **resolver,
	   avro_schema_t wschema, avro_schema_t rschema,
	   avro_schema_t root_rschema)
{
	if (is_avro_string(rschema)) {
		*resolver = avro_resolver_create(wschema, root_rschema);
		(*resolver)->parent.string_value = avro_resolver_string_value;
	}
	return 0;
}


/*-----------------------------------------------------------------------
 * arrays
 */

static int
avro_resolver_array_start_block(avro_consumer_t *consumer,
				int is_first_block,
				unsigned int block_count,
				void *user_data)
{
#if DEBUG_RESOLVER
	if (is_first_block) {
		avro_resolver_t  *resolver = (avro_resolver_t *) consumer;
		avro_datum_t  ud_dest = user_data;
		avro_datum_t  dest = avro_resolver_get_real_dest(resolver, ud_dest);
		debug("Starting array %p", dest);
	}
#else
	AVRO_UNUSED(consumer);
	AVRO_UNUSED(user_data);
	AVRO_UNUSED(is_first_block);
#endif

	AVRO_UNUSED(block_count);
	return 0;
}

static int
avro_resolver_array_element(avro_consumer_t *consumer,
			    unsigned int index,
			    avro_consumer_t **element_consumer,
			    void **element_user_data,
			    void *user_data)
{
	AVRO_UNUSED(index);

	avro_resolver_t  *resolver = (avro_resolver_t *) consumer;
	avro_datum_t  ud_dest = user_data;
	avro_datum_t  dest = avro_resolver_get_real_dest(resolver, ud_dest);
	debug("Adding element to array %p", dest);

	/*
	 * Allocate a new element datum and add it to the array.
	 */
	avro_schema_t  array_schema = avro_datum_get_schema(dest);
	avro_schema_t  item_schema = avro_schema_array_items(array_schema);
	avro_datum_t  element = avro_datum_from_schema(item_schema);
	avro_array_append_datum(dest, element);
	avro_datum_decref(element);

	/*
	 * Return the consumer that we allocated to process the array's
	 * children.
	 */
	*element_consumer = resolver->child_resolvers[0];
	*element_user_data = element;
	return 0;
}

static int
try_array(avro_resolver_t **resolver,
	  avro_schema_t wschema, avro_schema_t rschema,
	  avro_schema_t root_rschema)
{
	/*
	 * First verify that the reader is an array.
	 */

	if (!is_avro_array(rschema)) {
		return 0;
	}

	/*
	 * Array schemas have to have compatible element schemas to be
	 * compatible themselves.  Try to create an avro_resolver_t to
	 * check the compatibility.
	 */

	avro_schema_t  witems = avro_schema_array_items(wschema);
	avro_schema_t  ritems = avro_schema_array_items(rschema);

	avro_consumer_t  *item_consumer = avro_resolver_new(witems, ritems);
	if (!item_consumer) {
		avro_prefix_error("Array values aren't compatible: ");
		return EINVAL;
	}

	/*
	 * The two schemas are compatible, so go ahead and create a
	 * GavroResolver for the array.  Store the item schema's
	 * resolver into the child_resolvers field.
	 */

	*resolver = avro_resolver_create(wschema, root_rschema);
	(*resolver)->num_children = 1;
	(*resolver)->child_resolvers = avro_calloc(1, sizeof(avro_consumer_t *));
	(*resolver)->child_resolvers[0] = item_consumer;
	(*resolver)->parent.array_start_block = avro_resolver_array_start_block;
	(*resolver)->parent.array_element = avro_resolver_array_element;

	return 0;
}


/*-----------------------------------------------------------------------
 * enums
 */

static int
avro_resolver_enum_value(avro_consumer_t *consumer, int value,
			 void *user_data)
{
	AVRO_UNUSED(value);

	avro_resolver_t  *resolver = (avro_resolver_t *) consumer;
	avro_datum_t  ud_dest = user_data;
	avro_datum_t  dest = avro_resolver_get_real_dest(resolver, ud_dest);

	const char  *symbol_name = avro_schema_enum_get(resolver->parent.schema, value);
	debug("Storing symbol %s into %p", symbol_name, dest);
	return avro_enum_set_name(dest, symbol_name);
}

static int
try_enum(avro_resolver_t **resolver,
	 avro_schema_t wschema, avro_schema_t rschema,
	 avro_schema_t root_rschema)
{
	/*
	 * Enum schemas have to have the same name — but not the same
	 * list of symbols — to be compatible.
	 */

	if (is_avro_enum(rschema)) {
		const char  *wname = avro_schema_name(wschema);
		const char  *rname = avro_schema_name(rschema);

		if (!strcmp(wname, rname)) {
			*resolver = avro_resolver_create(wschema, root_rschema);
			(*resolver)->parent.enum_value = avro_resolver_enum_value;
		}
	}
	return 0;
}


/*-----------------------------------------------------------------------
 * fixed
 */

static int
avro_resolver_fixed_value(avro_consumer_t *consumer,
			  const void *value, size_t value_len,
			  void *user_data)
{
	avro_resolver_t  *resolver = (avro_resolver_t *) consumer;
	avro_datum_t  ud_dest = user_data;
	avro_datum_t  dest = avro_resolver_get_real_dest(resolver, ud_dest);
	debug("Storing (fixed) %zu bytes into %p", value_len, dest);
	return avro_givefixed_set(dest, value, value_len, avro_alloc_free);
}

static int
try_fixed(avro_resolver_t **resolver,
	  avro_schema_t wschema, avro_schema_t rschema,
	  avro_schema_t root_rschema)
{
	/*
	 * Fixed schemas need the same name and size to be compatible.
	 */

	if (avro_schema_equal(wschema, rschema)) {
		*resolver = avro_resolver_create(wschema, root_rschema);
		(*resolver)->parent.fixed_value = avro_resolver_fixed_value;
	}
	return 0;
}


/*-----------------------------------------------------------------------
 * maps
 */

static int
avro_resolver_map_start_block(avro_consumer_t *consumer,
			      int is_first_block,
			      unsigned int block_count,
			      void *user_data)
{
#if DEBUG_RESOLVER
	if (is_first_block) {
		avro_resolver_t  *resolver = (avro_resolver_t *) consumer;
		avro_datum_t  ud_dest = user_data;
		avro_datum_t  dest = avro_resolver_get_real_dest(resolver, ud_dest);
		debug("Starting map %p", dest);
	}
#else
	AVRO_UNUSED(consumer);
	AVRO_UNUSED(user_data);
	AVRO_UNUSED(is_first_block);
#endif

	AVRO_UNUSED(block_count);
	return 0;
}

static int
avro_resolver_map_element(avro_consumer_t *consumer,
			  unsigned int index,
			  const char *key,
			  avro_consumer_t **value_consumer,
			  void **value_user_data,
			  void *user_data)
{
	AVRO_UNUSED(index);

	avro_resolver_t  *resolver = (avro_resolver_t *) consumer;
	avro_datum_t  ud_dest = user_data;
	avro_datum_t  dest = avro_resolver_get_real_dest(resolver, ud_dest);
	debug("Adding element to map %p", dest);

	/*
	 * Allocate a new element datum and add it to the map.
	 */

	avro_schema_t  map_schema = avro_datum_get_schema(dest);
	avro_schema_t  value_schema = avro_schema_map_values(map_schema);
	avro_datum_t  value = avro_datum_from_schema(value_schema);
	avro_map_set(dest, key, value);
	avro_datum_decref(value);

	/*
	 * Return the consumer that we allocated to process the map's
	 * children.
	 */

	*value_consumer = resolver->child_resolvers[0];
	*value_user_data = value;
	return 0;
}

static int
try_map(avro_resolver_t **resolver,
	avro_schema_t wschema, avro_schema_t rschema,
	avro_schema_t root_rschema)
{
	/*
	 * First verify that the reader is an map.
	 */

	if (!is_avro_map(rschema)) {
		return 0;
	}

	/*
	 * Array schemas have to have compatible element schemas to be
	 * compatible themselves.  Try to create an avro_resolver_t to
	 * check the compatibility.
	 */

	avro_schema_t  wvalues = avro_schema_map_values(wschema);
	avro_schema_t  rvalues = avro_schema_map_values(rschema);

	avro_consumer_t  *value_consumer = avro_resolver_new(wvalues, rvalues);
	if (!value_consumer) {
		avro_prefix_error("Map values aren't compatible: ");
		return EINVAL;
	}

	/*
	 * The two schemas are compatible, so go ahead and create a
	 * GavroResolver for the map.  Store the value schema's
	 * resolver into the child_resolvers field.
	 */

	*resolver = avro_resolver_create(wschema, root_rschema);
	(*resolver)->num_children = 1;
	(*resolver)->child_resolvers = avro_calloc(1, sizeof(avro_consumer_t *));
	(*resolver)->child_resolvers[0] = value_consumer;
	(*resolver)->parent.map_start_block = avro_resolver_map_start_block;
	(*resolver)->parent.map_element = avro_resolver_map_element;

	return 0;
}


/*-----------------------------------------------------------------------
 * records
 */

static int
avro_resolver_record_start(avro_consumer_t *consumer,
			   void *user_data)
{
#if DEBUG_RESOLVER
	avro_resolver_t  *resolver = (avro_resolver_t *) consumer;
	avro_datum_t  ud_dest = user_data;
	avro_datum_t  dest = avro_resolver_get_real_dest(resolver, ud_dest);
	debug("Starting record at %p", dest);
#else
	AVRO_UNUSED(consumer);
	AVRO_UNUSED(user_data);
#endif

	/*
	 * TODO: Eventually, we'll fill in default values for the extra
	 * reader fields here.
	 */

	return 0;
}

static int
avro_resolver_record_field(avro_consumer_t *consumer,
			   unsigned int index,
			   avro_consumer_t **field_consumer,
			   void **field_user_data,
			   void *user_data)
{
	avro_resolver_t  *resolver = (avro_resolver_t *) consumer;
	avro_datum_t  ud_dest = user_data;
	avro_datum_t  dest = avro_resolver_get_real_dest(resolver, ud_dest);

	const char  *field_name =
	    avro_schema_record_field_name(consumer->schema, index);

	/*
	 * Grab the resolver for this field of the writer record.  If
	 * it's NULL, this this field doesn't exist in the reader
	 * record, and should be skipped.
	 */

	debug("Retrieving resolver for writer field %i (%s)",
	      index, field_name);

	if (!resolver->child_resolvers[index]) {
		debug("Reader doesn't have field %s, skipping", field_name);
		return 0;
	}

	/*
	 * TODO: Once we can retrieve record fields by index (quickly),
	 * use the index_mapping.
	 */

	avro_datum_t  field = NULL;
	avro_record_get(dest, field_name, &field);

	*field_consumer = resolver->child_resolvers[index];
	*field_user_data = field;
	return 0;
}

static int
try_record(avro_resolver_t **resolver,
	   avro_schema_t wschema, avro_schema_t rschema,
	   avro_schema_t root_rschema)
{
	/*
	 * First verify that the reader is also a record, and has the
	 * same name as the writer.
	 */

	if (!is_avro_record(rschema)) {
		return 0;
	}

	const char  *wname = avro_schema_name(wschema);
	const char  *rname = avro_schema_name(rschema);

	if (strcmp(wname, rname)) {
		return 0;
	}

	/*
	 * Categorize the fields in the record schemas.  Fields that are
	 * only in the writer are ignored.  Fields that are only in the
	 * reader raise a schema mismatch error, unless the field has a
	 * default value.  Fields that are in both are resolved
	 * recursively.
	 *
	 * The child_resolver array will contain an avro_resolver_t for
	 * each field in the writer schema.  To build this array, we
	 * loop through the fields of the reader schema.  If that field
	 * is also in the writer schema, we resolve them recursively,
	 * and store the resolver into the array.  If the field isn't in
	 * the writer schema, we raise an error.  (TODO: Eventually,
	 * we'll handle default values here.)  After this loop finishes,
	 * any NULLs in the child_resolver array will represent fields
	 * in the writer but not the reader; these fields will be
	 * skipped when processing the input.
	 */

	size_t  wfields = avro_schema_record_size(wschema);
	size_t  rfields = avro_schema_record_size(rschema);

	debug("Checking writer record schema %s", wname);

	avro_consumer_t  **child_resolvers =
	    avro_calloc(wfields, sizeof(avro_consumer_t *));
	int  *index_mapping = avro_calloc(wfields, sizeof(int));

	unsigned int  ri;
	for (ri = 0; ri < rfields; ri++) {
		avro_schema_t  rfield =
		    avro_schema_record_field_get_by_index(rschema, ri);
		const char  *field_name =
		    avro_schema_record_field_name(rschema, ri);

		debug("Resolving reader record field %u (%s)", ri, field_name);

		/*
		 * See if this field is also in the writer schema.
		 */

		int  wi = avro_schema_record_field_get_index(wschema, field_name);

		if (wi == -1) {
			/*
			 * This field isn't in the writer schema —
			 * that's an error!  TODO: Handle default
			 * values!
			 */

			debug("Field %s isn't in writer", field_name);
			avro_set_error("Reader field %s doesn't appear in writer",
				       field_name);
			goto error;
		}

		/*
		 * Try to recursively resolve the schemas for this
		 * field.  If they're not compatible, that's an error.
		 */

		avro_schema_t  wfield =
		    avro_schema_record_field_get_by_index(wschema, wi);
		avro_consumer_t  *field_resolver = avro_resolver_new(wfield, rfield);

		if (!field_resolver) {
			avro_prefix_error("Field %s isn't compatible: ", field_name);
			goto error;
		}

		/*
		 * Save the details for this field.
		 */

		debug("Found match for field %s (%u in reader, %d in writer)",
		      field_name, ri, wi);
		child_resolvers[wi] = field_resolver;
		index_mapping[wi] = ri;
	}

	/*
	 * We might not have found matches for all of the writer fields,
	 * but that's okay — any extras will be ignored.
	 */

	*resolver = avro_resolver_create(wschema, root_rschema);
	(*resolver)->num_children = wfields;
	(*resolver)->child_resolvers = child_resolvers;
	(*resolver)->index_mapping = index_mapping;
	(*resolver)->parent.record_start = avro_resolver_record_start;
	(*resolver)->parent.record_field = avro_resolver_record_field;
	return 0;

error:
	/*
	 * Clean up any consumer we might have already created.
	 */

	{
		unsigned int  i;
		for (i = 0; i < wfields; i++) {
			if (child_resolvers[i]) {
				avro_consumer_free(child_resolvers[i]);
			}
		}
	}

	avro_free(child_resolvers, wfields * sizeof(avro_consumer_t *));
	avro_free(index_mapping, wfields * sizeof(int));
	return EINVAL;
}


/*-----------------------------------------------------------------------
 * union
 */

static int
avro_resolver_union_branch(avro_consumer_t *consumer,
			   unsigned int discriminant,
			   avro_consumer_t **branch_consumer,
			   void **branch_user_data,
			   void *user_data)
{
	avro_resolver_t  *resolver = (avro_resolver_t *) consumer;

	/*
	 * Grab the resolver for this branch of the writer union.  If
	 * it's NULL, then this branch is incompatible with the reader.
	 */

	debug("Retrieving resolver for writer branch %u", discriminant);

	if (!resolver->child_resolvers[discriminant]) {
		avro_set_error("Writer union branch %u is incompatible "
			       "with reader schema \"%s\"",
			       discriminant, avro_schema_type_name(resolver->rschema));
		return EINVAL;
	}

	/*
	 * Return the branch's resolver.
	 */

	*branch_consumer = resolver->child_resolvers[discriminant];
	*branch_user_data = user_data;
	return 0;
}

static avro_consumer_t *
try_union(avro_schema_t wschema, avro_schema_t rschema)
{
	/*
	 * For a writer union, we recursively try to resolve each branch
	 * against the reader schema.  This will work correctly whether
	 * or not the reader is also a union — if the reader is a union,
	 * then we'll resolve each (non-union) writer branch against the
	 * reader union, which will be checked in our calls to
	 * check_simple_writer below.  The net result is that we might
	 * end up trying every combination of writer and reader
	 * branches, when looking for compatible schemas.
	 *
	 * Regardless of what the reader schema is, for each writer
	 * branch, we stash away the recursive avro_resolver_t into the
	 * child_resolvers array.  A NULL entry in this array means that
	 * that branch isn't compatible with the reader.  This isn't an
	 * immediate schema resolution error, since we allow
	 * incompatible branches in the types as long as that branch
	 * never appears in the actual data.  We only return an error if
	 * there are *no* branches that are compatible.
	 */

	size_t  num_branches = avro_schema_union_size(wschema);
	debug("Checking %zu-branch writer union schema", num_branches);

	avro_consumer_t  **child_resolvers =
	    avro_calloc(num_branches, sizeof(avro_consumer_t *));
	int  some_branch_compatible = 0;

	unsigned int  i;
	for (i = 0; i < num_branches; i++) {
		avro_schema_t  branch_schema =
		    avro_schema_union_branch(wschema, i);

		debug("Resolving writer union branch %u (%s)",
		      i, avro_schema_type_name(branch_schema));

		/*
		 * Try to recursively resolve this branch of the writer
		 * union.  Don't raise an error if this fails — it's
		 * okay for some of the branches to not be compatible
		 * with the reader, as long as those branches never
		 * appear in the input.
		 */

		child_resolvers[i] = avro_resolver_new(branch_schema, rschema);
		if (child_resolvers[i]) {
			debug("Found match for writer union branch %u", i);
			some_branch_compatible = 1;
		} else {
			debug("No match for writer union branch %u", i);
		}
	}

	/*
	 * As long as there's at least one branch that's compatible with
	 * the reader, then we consider this schema resolution a
	 * success.
	 */

	if (!some_branch_compatible) {
		debug("No writer union branches match");
		avro_set_error("No branches in the writer are compatible "
			       "with reader schema %s",
			       avro_schema_type_name(rschema));
		goto error;
	}

	avro_resolver_t  *resolver = avro_resolver_create(wschema, rschema);
	resolver->num_children = num_branches;
	resolver->child_resolvers = child_resolvers;
	resolver->parent.union_branch = avro_resolver_union_branch;
	return &resolver->parent;

error:
	/*
	 * Clean up any consumer we might have already created.
	 */

	for (i = 0; i < num_branches; i++) {
		if (child_resolvers[i]) {
			avro_consumer_free(child_resolvers[i]);
		}
	}

	avro_free(child_resolvers, num_branches * sizeof(avro_consumer_t *));
	return NULL;
}


/*-----------------------------------------------------------------------
 * schema type dispatcher
 */

avro_consumer_t *
avro_resolver_new(avro_schema_t wschema, avro_schema_t rschema)
{
	check_param(NULL, is_avro_schema(wschema), "writer schema");
	check_param(NULL, is_avro_schema(rschema), "reader schema");

	switch (avro_typeof(wschema))
	{
		case AVRO_BOOLEAN:
			check_simple_writer(wschema, rschema, boolean);
			return NULL;

		case AVRO_BYTES:
			check_simple_writer(wschema, rschema, bytes);
			return NULL;

		case AVRO_DOUBLE:
			check_simple_writer(wschema, rschema, double);
			return NULL;

		case AVRO_FLOAT:
			check_simple_writer(wschema, rschema, float);
			return NULL;

		case AVRO_INT32:
			check_simple_writer(wschema, rschema, int);
			return NULL;

		case AVRO_INT64:
			check_simple_writer(wschema, rschema, long);
			return NULL;

		case AVRO_NULL:
			check_simple_writer(wschema, rschema, null);
			return NULL;

		case AVRO_STRING:
			check_simple_writer(wschema, rschema, string);
			return NULL;

		case AVRO_ARRAY:
			check_simple_writer(wschema, rschema, array);
			return NULL;

		case AVRO_ENUM:
			check_simple_writer(wschema, rschema, enum);
			return NULL;

		case AVRO_FIXED:
			check_simple_writer(wschema, rschema, fixed);
			return NULL;

		case AVRO_MAP:
			check_simple_writer(wschema, rschema, map);
			return NULL;

		case AVRO_RECORD:
			check_simple_writer(wschema, rschema, record);
			return NULL;

		case AVRO_UNION:
			return try_union(wschema, rschema);

		default:
			avro_set_error("Unknown schema type");
			return NULL;
	}

	return NULL;
}
