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

#include "avro/allocation.h"
#include "avro/data.h"
#include "avro/generic.h"
#include "avro/schema.h"
#include "avro/value.h"
#include "avro_private.h"


/*-----------------------------------------------------------------------
 * boolean
 */

static size_t
avro_generic_boolean_instance_size(const avro_value_iface_t *iface)
{
	AVRO_UNUSED(iface);
	return sizeof(int);
}

static int
avro_generic_boolean_init(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	int  *self = vself;
	*self = 0;
	return 0;
}

static void
avro_generic_boolean_done(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
}

static int
avro_generic_boolean_reset(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	int  *self = vself;
	*self = 0;
	return 0;
}

static avro_type_t
avro_generic_boolean_get_type(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return AVRO_BOOLEAN;
}

static avro_schema_t
avro_generic_boolean_get_schema(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return avro_schema_boolean();
}

static int
avro_generic_boolean_get(const avro_value_iface_t *iface,
			 const void *vself, int *out)
{
	AVRO_UNUSED(iface);
	const int  *self = vself;
	*out = *self;
	return 0;
}

static int
avro_generic_boolean_set(const avro_value_iface_t *iface,
			 void *vself, int val)
{
	AVRO_UNUSED(iface);
	int  *self = vself;
	*self = val;
	return 0;
}

static avro_value_iface_t  AVRO_GENERIC_BOOLEAN_CLASS =
{
	/* "class" methods */
	NULL, /* incref */
	NULL, /* decref */
	avro_generic_boolean_instance_size,
	/* general "instance" methods */
	avro_generic_boolean_init,
	avro_generic_boolean_done,
	avro_generic_boolean_reset,
	avro_generic_boolean_get_type,
	avro_generic_boolean_get_schema,
	/* primitive getters */
	avro_generic_boolean_get,
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
	avro_generic_boolean_set,
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
	NULL, /* get_discriminant */
	NULL, /* get_current_branch */
	/* compound setters */
	NULL, /* append */
	NULL, /* add */
	NULL  /* set_branch */
};

avro_value_iface_t *avro_generic_boolean_class(void)
{
	return &AVRO_GENERIC_BOOLEAN_CLASS;
}

int avro_generic_boolean_new(avro_value_t *value, int val)
{
	int  rval;
	check(rval, avro_value_new(&AVRO_GENERIC_BOOLEAN_CLASS, value));
	return avro_generic_boolean_set(value->iface, value->self, val);
}

/*-----------------------------------------------------------------------
 * bytes
 */

static size_t
avro_generic_bytes_instance_size(const avro_value_iface_t *iface)
{
	AVRO_UNUSED(iface);
	return sizeof(avro_raw_string_t);
}

static int
avro_generic_bytes_init(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	avro_raw_string_t  *self = vself;
	avro_raw_string_init(self);
	return 0;
}

static void
avro_generic_bytes_done(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	avro_raw_string_t  *self = vself;
	avro_raw_string_done(self);
}

static int
avro_generic_bytes_reset(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	avro_raw_string_t  *self = vself;
	avro_raw_string_clear(self);
	return 0;
}

static avro_type_t
avro_generic_bytes_get_type(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return AVRO_BYTES;
}

static avro_schema_t
avro_generic_bytes_get_schema(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return avro_schema_bytes();
}

static int
avro_generic_bytes_get(const avro_value_iface_t *iface,
		       const void *vself, const void **buf, size_t *size)
{
	AVRO_UNUSED(iface);
	const avro_raw_string_t  *self = vself;
	if (buf != NULL) {
		*buf = avro_raw_string_get(self);
	}
	if (size != NULL) {
		*size = avro_raw_string_length(self);
	}
	return 0;
}

static int
avro_generic_bytes_grab(const avro_value_iface_t *iface,
			const void *vself, avro_wrapped_buffer_t *dest)
{
	AVRO_UNUSED(iface);
	const avro_raw_string_t  *self = vself;
	return avro_raw_string_grab(self, dest);
}

static int
avro_generic_bytes_set(const avro_value_iface_t *iface,
		       void *vself, void *buf, size_t size)
{
	AVRO_UNUSED(iface);
	check_param(EINVAL, buf != NULL, "bytes contents");
	avro_raw_string_t  *self = vself;
	avro_raw_string_set_length(self, buf, size);
	return 0;
}

static int
avro_generic_bytes_give(const avro_value_iface_t *iface,
			void *vself, avro_wrapped_buffer_t *buf)
{
	AVRO_UNUSED(iface);
	avro_raw_string_t  *self = vself;
	avro_raw_string_give(self, buf);
	return 0;
}

static avro_value_iface_t  AVRO_GENERIC_BYTES_CLASS =
{
	/* "class" methods */
	NULL, /* incref */
	NULL, /* decref */
	avro_generic_bytes_instance_size,
	/* general "instance" methods */
	avro_generic_bytes_init,
	avro_generic_bytes_done,
	avro_generic_bytes_reset,
	avro_generic_bytes_get_type,
	avro_generic_bytes_get_schema,
	/* primitive getters */
	NULL, /* get_boolean */
	avro_generic_bytes_get,
	avro_generic_bytes_grab,
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
	avro_generic_bytes_set,
	avro_generic_bytes_give,
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
	NULL, /* get_discriminant */
	NULL, /* get_current_branch */
	/* compound setters */
	NULL, /* append */
	NULL, /* add */
	NULL  /* set_branch */
};

avro_value_iface_t *avro_generic_bytes_class(void)
{
	return &AVRO_GENERIC_BYTES_CLASS;
}

int avro_generic_bytes_new(avro_value_t *value, void *buf, size_t size)
{
	int  rval;
	check(rval, avro_value_new(&AVRO_GENERIC_BYTES_CLASS, value));
	return avro_generic_bytes_set(value->iface, value->self, buf, size);
}

/*-----------------------------------------------------------------------
 * double
 */

static size_t
avro_generic_double_instance_size(const avro_value_iface_t *iface)
{
	AVRO_UNUSED(iface);
	return sizeof(double);
}

static int
avro_generic_double_init(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	double  *self = vself;
	*self = 0.0;
	return 0;
}

static void
avro_generic_double_done(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
}

static int
avro_generic_double_reset(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	double  *self = vself;
	*self = 0.0;
	return 0;
}

static avro_type_t
avro_generic_double_get_type(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return AVRO_DOUBLE;
}

static avro_schema_t
avro_generic_double_get_schema(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return avro_schema_double();
}

static int
avro_generic_double_get(const avro_value_iface_t *iface,
			const void *vself, double *out)
{
	AVRO_UNUSED(iface);
	const double  *self = vself;
	*out = *self;
	return 0;
}

static int
avro_generic_double_set(const avro_value_iface_t *iface,
			void *vself, double val)
{
	AVRO_UNUSED(iface);
	double  *self = vself;
	*self = val;
	return 0;
}

static avro_value_iface_t  AVRO_GENERIC_DOUBLE_CLASS =
{
	/* "class" methods */
	NULL, /* incref */
	NULL, /* decref */
	avro_generic_double_instance_size,
	/* general "instance" methods */
	avro_generic_double_init,
	avro_generic_double_done,
	avro_generic_double_reset,
	avro_generic_double_get_type,
	avro_generic_double_get_schema,
	/* primitive getters */
	NULL, /* get_boolean */
	NULL, /* get_bytes */
	NULL, /* grab_bytes */
	avro_generic_double_get,
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
	avro_generic_double_set,
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
	NULL, /* get_discriminant */
	NULL, /* get_current_branch */
	/* compound setters */
	NULL, /* append */
	NULL, /* add */
	NULL  /* set_branch */
};

avro_value_iface_t *avro_generic_double_class(void)
{
	return &AVRO_GENERIC_DOUBLE_CLASS;
}

int avro_generic_double_new(avro_value_t *value, double val)
{
	int  rval;
	check(rval, avro_value_new(&AVRO_GENERIC_DOUBLE_CLASS, value));
	return avro_generic_double_set(value->iface, value->self, val);
}

/*-----------------------------------------------------------------------
 * float
 */

static size_t
avro_generic_float_instance_size(const avro_value_iface_t *iface)
{
	AVRO_UNUSED(iface);
	return sizeof(float);
}

static int
avro_generic_float_init(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	float  *self = vself;
	*self = 0.0f;
	return 0;
}

static void
avro_generic_float_done(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
}

static int
avro_generic_float_reset(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	float  *self = vself;
	*self = 0.0f;
	return 0;
}

static avro_type_t
avro_generic_float_get_type(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return AVRO_FLOAT;
}

static avro_schema_t
avro_generic_float_get_schema(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return avro_schema_float();
}

static int
avro_generic_float_get(const avro_value_iface_t *iface,
		       const void *vself, float *out)
{
	AVRO_UNUSED(iface);
	const float  *self = vself;
	*out = *self;
	return 0;
}

static int
avro_generic_float_set(const avro_value_iface_t *iface,
		       void *vself, float val)
{
	AVRO_UNUSED(iface);
	float  *self = vself;
	*self = val;
	return 0;
}

static avro_value_iface_t  AVRO_GENERIC_FLOAT_CLASS =
{
	/* "class" methods */
	NULL, /* incref */
	NULL, /* decref */
	avro_generic_float_instance_size,
	/* general "instance" methods */
	avro_generic_float_init,
	avro_generic_float_done,
	avro_generic_float_reset,
	avro_generic_float_get_type,
	avro_generic_float_get_schema,
	/* primitive getters */
	NULL, /* get_boolean */
	NULL, /* get_bytes */
	NULL, /* grab_bytes */
	NULL, /* get_double */
	avro_generic_float_get,
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
	avro_generic_float_set,
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
	NULL, /* get_discriminant */
	NULL, /* get_current_branch */
	/* compound setters */
	NULL, /* append */
	NULL, /* add */
	NULL  /* set_branch */
};

avro_value_iface_t *avro_generic_float_class(void)
{
	return &AVRO_GENERIC_FLOAT_CLASS;
}

int avro_generic_float_new(avro_value_t *value, float val)
{
	int  rval;
	check(rval, avro_value_new(&AVRO_GENERIC_FLOAT_CLASS, value));
	return avro_generic_float_set(value->iface, value->self, val);
}

/*-----------------------------------------------------------------------
 * int
 */

static size_t
avro_generic_int_instance_size(const avro_value_iface_t *iface)
{
	AVRO_UNUSED(iface);
	return sizeof(int32_t);
}

static int
avro_generic_int_init(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	int32_t  *self = vself;
	*self = 0;
	return 0;
}

static void
avro_generic_int_done(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
}

static int
avro_generic_int_reset(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	int32_t  *self = vself;
	*self = 0;
	return 0;
}

static avro_type_t
avro_generic_int_get_type(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return AVRO_INT32;
}

static avro_schema_t
avro_generic_int_get_schema(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return avro_schema_int();
}

static int
avro_generic_int_get(const avro_value_iface_t *iface,
		     const void *vself, int32_t *out)
{
	AVRO_UNUSED(iface);
	const int32_t  *self = vself;
	*out = *self;
	return 0;
}

static int
avro_generic_int_set(const avro_value_iface_t *iface,
		     void *vself, int32_t val)
{
	AVRO_UNUSED(iface);
	int32_t  *self = vself;
	*self = val;
	return 0;
}

static avro_value_iface_t  AVRO_GENERIC_INT_CLASS =
{
	/* "class" methods */
	NULL, /* incref */
	NULL, /* decref */
	avro_generic_int_instance_size,
	/* general "instance" methods */
	avro_generic_int_init,
	avro_generic_int_done,
	avro_generic_int_reset,
	avro_generic_int_get_type,
	avro_generic_int_get_schema,
	/* primitive getters */
	NULL, /* get_boolean */
	NULL, /* get_bytes */
	NULL, /* grab_bytes */
	NULL, /* get_double */
	NULL, /* get_float */
	avro_generic_int_get,
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
	avro_generic_int_set,
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
	NULL, /* get_discriminant */
	NULL, /* get_current_branch */
	/* compound setters */
	NULL, /* append */
	NULL, /* add */
	NULL  /* set_branch */
};

avro_value_iface_t *avro_generic_int_class(void)
{
	return &AVRO_GENERIC_INT_CLASS;
}

int avro_generic_int_new(avro_value_t *value, int32_t val)
{
	int  rval;
	check(rval, avro_value_new(&AVRO_GENERIC_INT_CLASS, value));
	return avro_generic_int_set(value->iface, value->self, val);
}

/*-----------------------------------------------------------------------
 * long
 */

static size_t
avro_generic_long_instance_size(const avro_value_iface_t *iface)
{
	AVRO_UNUSED(iface);
	return sizeof(int64_t);
}

static int
avro_generic_long_init(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	int64_t  *self = vself;
	*self = 0;
	return 0;
}

static void
avro_generic_long_done(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
}

static int
avro_generic_long_reset(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	int64_t  *self = vself;
	*self = 0;
	return 0;
}

static avro_type_t
avro_generic_long_get_type(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return AVRO_INT64;
}

static avro_schema_t
avro_generic_long_get_schema(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return avro_schema_long();
}

static int
avro_generic_long_get(const avro_value_iface_t *iface,
		      const void *vself, int64_t *out)
{
	AVRO_UNUSED(iface);
	const int64_t  *self = vself;
	*out = *self;
	return 0;
}

static int
avro_generic_long_set(const avro_value_iface_t *iface,
		      void *vself, int64_t val)
{
	AVRO_UNUSED(iface);
	int64_t  *self = vself;
	*self = val;
	return 0;
}

static avro_value_iface_t  AVRO_GENERIC_LONG_CLASS =
{
	/* "class" methods */
	NULL, /* incref */
	NULL, /* decref */
	avro_generic_long_instance_size,
	/* general "instance" methods */
	avro_generic_long_init,
	avro_generic_long_done,
	avro_generic_long_reset,
	avro_generic_long_get_type,
	avro_generic_long_get_schema,
	/* primitive getters */
	NULL, /* get_boolean */
	NULL, /* get_bytes */
	NULL, /* grab_bytes */
	NULL, /* get_double */
	NULL, /* get_float */
	NULL, /* get_int */
	avro_generic_long_get,
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
	avro_generic_long_set,
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
	NULL, /* get_discriminant */
	NULL, /* get_current_branch */
	/* compound setters */
	NULL, /* append */
	NULL, /* add */
	NULL  /* set_branch */
};

avro_value_iface_t *avro_generic_long_class(void)
{
	return &AVRO_GENERIC_LONG_CLASS;
}

int avro_generic_long_new(avro_value_t *value, int64_t val)
{
	int  rval;
	check(rval, avro_value_new(&AVRO_GENERIC_LONG_CLASS, value));
	return avro_generic_long_set(value->iface, value->self, val);
}

/*-----------------------------------------------------------------------
 * null
 */

static size_t
avro_generic_null_instance_size(const avro_value_iface_t *iface)
{
	AVRO_UNUSED(iface);
	return sizeof(int);
}

static int
avro_generic_null_init(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	int  *self = vself;
	*self = 0;
	return 0;
}

static void
avro_generic_null_done(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
}

static int
avro_generic_null_reset(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	int  *self = vself;
	*self = 0;
	return 0;
}

static avro_type_t
avro_generic_null_get_type(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return AVRO_NULL;
}

static avro_schema_t
avro_generic_null_get_schema(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return avro_schema_null();
}

static int
avro_generic_null_get(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return 0;
}

static int
avro_generic_null_set(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return 0;
}

static avro_value_iface_t  AVRO_GENERIC_NULL_CLASS =
{
	/* "class" methods */
	NULL, /* incref */
	NULL, /* decref */
	avro_generic_null_instance_size,
	/* general "instance" methods */
	avro_generic_null_init,
	avro_generic_null_done,
	avro_generic_null_reset,
	avro_generic_null_get_type,
	avro_generic_null_get_schema,
	/* primitive getters */
	NULL, /* get_boolean */
	NULL, /* get_bytes */
	NULL, /* grab_bytes */
	NULL, /* get_double */
	NULL, /* get_float */
	NULL, /* get_int */
	NULL, /* get_long */
	avro_generic_null_get,
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
	avro_generic_null_set,
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
	NULL, /* get_discriminant */
	NULL, /* get_current_branch */
	/* compound setters */
	NULL, /* append */
	NULL, /* add */
	NULL  /* set_branch */
};

avro_value_iface_t *avro_generic_null_class(void)
{
	return &AVRO_GENERIC_NULL_CLASS;
}

int avro_generic_null_new(avro_value_t *value)
{
	return avro_value_new(&AVRO_GENERIC_NULL_CLASS, value);
}

/*-----------------------------------------------------------------------
 * string
 */

static size_t
avro_generic_string_instance_size(const avro_value_iface_t *iface)
{
	AVRO_UNUSED(iface);
	return sizeof(avro_raw_string_t);
}

static int
avro_generic_string_init(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	avro_raw_string_t  *self = vself;
	avro_raw_string_init(self);
	return 0;
}

static void
avro_generic_string_done(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	avro_raw_string_t  *self = vself;
	avro_raw_string_done(self);
}

static int
avro_generic_string_reset(const avro_value_iface_t *iface, void *vself)
{
	AVRO_UNUSED(iface);
	avro_raw_string_t  *self = vself;
	avro_raw_string_clear(self);
	return 0;
}

static avro_type_t
avro_generic_string_get_type(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return AVRO_STRING;
}

static avro_schema_t
avro_generic_string_get_schema(const avro_value_iface_t *iface, const void *vself)
{
	AVRO_UNUSED(iface);
	AVRO_UNUSED(vself);
	return avro_schema_string();
}

static int
avro_generic_string_get(const avro_value_iface_t *iface,
			const void *vself, const char **str, size_t *size)
{
	AVRO_UNUSED(iface);
	const avro_raw_string_t  *self = vself;
	const char  *contents = avro_raw_string_get(self);

	if (str != NULL) {
		/*
		 * We can't return a NULL string, we have to return an
		 * *empty* string
		 */

		*str = (contents == NULL)? "": contents;
	}
	if (size != NULL) {
		/* raw_string's length includes the NUL terminator,
		 * unless it's empty */
		*size = (contents == NULL)? 1: avro_raw_string_length(self);
	}
	return 0;
}

static int
avro_generic_string_grab(const avro_value_iface_t *iface,
			 const void *vself, avro_wrapped_buffer_t *dest)
{
	AVRO_UNUSED(iface);
	const avro_raw_string_t  *self = vself;
	const char  *contents = avro_raw_string_get(self);

	if (contents == NULL) {
		return avro_wrapped_buffer_new(dest, "", 1);
	} else {
		return avro_raw_string_grab(self, dest);
	}
}

static int
avro_generic_string_set(const avro_value_iface_t *iface,
			void *vself, char *val)
{
	AVRO_UNUSED(iface);
	check_param(EINVAL, val != NULL, "string contents");

	/*
	 * This raw_string method ensures that we copy the NUL
	 * terminator from val, and will include the NUL terminator in
	 * the raw_string's length, which is what we want.
	 */
	avro_raw_string_t  *self = vself;
	avro_raw_string_set(self, val);
	return 0;
}

static int
avro_generic_string_set_length(const avro_value_iface_t *iface,
			       void *vself, char *val, size_t size)
{
	AVRO_UNUSED(iface);
	check_param(EINVAL, val != NULL, "string contents");
	avro_raw_string_t  *self = vself;
	avro_raw_string_set_length(self, val, size);
	return 0;
}

static int
avro_generic_string_give_length(const avro_value_iface_t *iface,
				void *vself, avro_wrapped_buffer_t *buf)
{
	AVRO_UNUSED(iface);
	avro_raw_string_t  *self = vself;
	avro_raw_string_give(self, buf);
	return 0;
}

static avro_value_iface_t  AVRO_GENERIC_STRING_CLASS =
{
	/* "class" methods */
	NULL, /* incref */
	NULL, /* decref */
	avro_generic_string_instance_size,
	/* general "instance" methods */
	avro_generic_string_init,
	avro_generic_string_done,
	avro_generic_string_reset,
	avro_generic_string_get_type,
	avro_generic_string_get_schema,
	/* primitive getters */
	NULL, /* get_boolean */
	NULL, /* get_bytes */
	NULL, /* grab_bytes */
	NULL, /* get_double */
	NULL, /* get_float */
	NULL, /* get_int */
	NULL, /* get_long */
	NULL, /* get_null */
	avro_generic_string_get,
	avro_generic_string_grab,
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
	avro_generic_string_set,
	avro_generic_string_set_length,
	avro_generic_string_give_length,
	NULL, /* set_enum */
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

avro_value_iface_t *avro_generic_string_class(void)
{
	return &AVRO_GENERIC_STRING_CLASS;
}

int avro_generic_string_new(avro_value_t *value, char *str)
{
	int  rval;
	check(rval, avro_value_new(&AVRO_GENERIC_STRING_CLASS, value));
	return avro_generic_string_set(value->iface, value->self, str);
}

int avro_generic_string_new_length(avro_value_t *value, char *str, size_t size)
{
	int  rval;
	check(rval, avro_value_new(&AVRO_GENERIC_STRING_CLASS, value));
	return avro_generic_string_set_length(value->iface, value->self, str, size);
}
