/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.logicaltypes;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * This interface defines what information an Avro data type should provide and
 * defines default conversion routines.
 *
 */
public interface AvroDatatype {

  /**
   * The primary use case for this function is for writing record values to the
   * console output for debugging
   * 
   * @param buffer is the StringBuffer the object value's string representation
   *               should be added
   * @param value  for this field
   */
  void toString(StringBuffer buffer, Object value);

  /**
   * Converts an input value into the object used by the backing data type. For
   * example the data type is a timestamp-millis, which is backed by a Long. This
   * method will convert any suitable input, e.g. Long, Instant,... into a long
   * value required by the writer.
   * 
   * @param value is the provided input
   * @return an object in the correct backing data type
   */
  Object convertToRawType(Object value);

  /**
   * @return the Avro schema type used for storing this logical type
   */
  Type getBackingType();

  /**
   * @return a default schema for this data type
   */
  Schema getRecommendedSchema();

  /**
   * @return the entry in the AvroType list of types
   */
  AvroType getAvroType();

  /**
   * Every data type has a best matching Java object, e.g. timestamp-millis is
   * best represented by a Java Instant. This function hence takes the value in
   * the backing data type and converts it to the Java object value.
   * 
   * @param value is the backing type value
   * @return the default Java object for this data type
   */
  Object convertToLogicalType(Object value);

  /**
   * @return the class the convertToLogicalType will return
   */
  Class<?> getConvertedType();
}
