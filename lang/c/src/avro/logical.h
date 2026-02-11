/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

#ifndef AVRO_LOGICAL_H
#define AVRO_LOGICAL_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

enum avro_logical_type_t {
  AVRO_LOGICAL_TYPE_NONE,
  AVRO_DECIMAL,
  AVRO_DATE,
  AVRO_TIME_MILLIS,
  AVRO_TIME_MICROS,
  AVRO_TIMESTAMP_MILLIS,
  AVRO_TIMESTAMP_MICROS,
  AVRO_DURATION
};
typedef enum avro_logical_type_t avro_logical_type_t;

typedef struct {
  avro_logical_type_t type;
  int precision;
  int scale; 
} avro_logical_schema_t;

CLOSE_EXTERN
#endif
