/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

#ifndef AVRO_H
#define AVRO_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <wchar.h>

/**
@mainpage

The C implementation of the Avro Spec is currently <em>incomplete</em> without a public API.  It
is not ready for production use. 
 

*/


/**
@file avro.h
@brief AVRO API
*/

/**
@defgroup AVRO Avro C API
@{
*/

/**
Avro status enum.
Enum used by Avro functions to return state.
@todo expand the number of states
*/
enum avro_status
{
  AVRO_OK = 0, /**< Function success */
  AVRO_FAILURE = 1 /**< Function failure */
};
typedef enum avro_status avro_status_t;

/** Initialize the AVRO library 
@return The Avro status
*/
avro_status_t avro_initialize(void);

/**
* @defgroup generic_routines AVRO Generic API
* @ingroup AVRO
* @{
*/

/**
Avro types.
*/
enum avro_type
{
   AVRO_STRING, /**< string primitive */
   AVRO_BYTES,  /**< bytes primitive */
   AVRO_INT,    /**< int primitive */
   AVRO_LONG,   /**< long primitive */
   AVRO_FLOAT,  /**< float primitive */
   AVRO_DOUBLE, /**< double primitive */
   AVRO_BOOLEAN,/**< boolean primitive */
   AVRO_NULL,   /**< null primitive */
   AVRO_RECORD, /**< complex record */
   AVRO_FIELD,  /**< complex record field */
   AVRO_ENUM,   /**< complex enum */
   AVRO_FIXED,  /**< complex fixed value */
   AVRO_MAP,    /**< complex map */
   AVRO_ARRAY,  /**< complex array */
   AVRO_UNION   /**< complex union */
};
typedef enum avro_type avro_type_t;

#ifdef __cplusplus
}
#endif

#endif /* ifdef AVRO_H */
