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

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.commons.text.StringEscapeUtils;

/**
 * ENUM with the list of all Avro data types
 *
 */
public enum AvroType {
  /**
   * A 8bit signed integer
   */
  AVROBYTE,
  /**
   * ASCII text of large size, comparison and sorting is binary
   */
  AVROCLOB,
  /**
   * Unicode text of large size, comparison and sorting is binary
   */
  AVRONCLOB,
  /**
   * Unicode text up t n chars long, comparison and sorting is binary
   */
  AVRONVARCHAR,
  /**
   * A 16bit signed integer
   */
  AVROSHORT,
  /**
   * A Spatial data type in WKT representation
   */
  AVROSTGEOMETRY,
  /**
   * A Spatial data type in WKT representation
   */
  AVROSTPOINT,
  /**
   * A string as URI
   */
  AVROURI,
  /**
   * An ASCII string of n chars length, comparison and sorting is binary
   */
  AVROVARCHAR,
  /**
   * A date without time information
   */
  AVRODATE,
  /**
   * A numeric value with precision and scale
   */
  AVRODECIMAL,
  /**
   * A time information down to milliseconds
   */
  AVROTIMEMILLIS,
  /**
   * A time information down to microseconds
   */
  AVROTIMEMICROS,
  /**
   * A timestamp down to milliseconds in UTC
   */
  AVROTIMESTAMPMILLIS,
  /**
   * A timestamp down to microseconds in UTC
   */
  AVROTIMESTAMPMICROS,
  /**
   * A timestamp down to milliseconds without time zone info
   */
  AVROLOCALTIMESTAMPMILLIS,
  /**
   * A timestamp down to microseconds without time zone info
   */
  AVROLOCALTIMESTAMPMICROS,
  /**
   * Boolean
   */
  AVROBOOLEAN,
  /**
   * A 32bit signed integer value
   */
  AVROINT,
  /**
   * A 64bit signed integer value
   */
  AVROLONG,
  /**
   * A 32bit floating point number
   */
  AVROFLOAT,
  /**
   * A 64bit floating point number
   */
  AVRODOUBLE,
  /**
   * Binary data of any length
   */
  AVROBYTES,
  /**
   * A unbounded unicode text - prefer using nvarchar or nclob instead to indicate
   * its normal length, comparison and sorting is binary
   */
  AVROSTRING,
  /**
   * A binary object with an upper size limit
   */
  AVROFIXED,
  /**
   * A unicode string with a list of allowed values - one of enum(), comparison
   * and sorting is binary
   */
  AVROENUM,
  /**
   * A unicode string array with a list of allowed values - many of map(),
   * comparison and sorting is binary
   */
  AVROMAP,
  /**
   * An ASCII string formatted as UUID, comparison and sorting is binary
   */
  AVROUUID,
  /**
   * An array of elements
   */
  AVROARRAY,
  /**
   * A Record of its own
   */
  AVRORECORD;

  /**
   * In case this schema is a union of null and something else, it returns the
   * _something else_
   * 
   * @param schema of the input
   * @return schema without the union of null, in case it is just that. Does
   *         return an union in all other cases.
   */
  public static Schema getBaseSchema(Schema schema) {
    if (schema == null) {
      return null;
    } else if (schema.getType() == Type.UNION) {
      List<Schema> types = schema.getTypes();
      if (types.size() == 2 && types.get(0).getType() == Type.NULL) {
        return types.get(1);
      } else {
        return schema;
      }
    } else {
      return schema;
    }

  }

  public static String encodeJson(String text) {
    /*
     * Backspace is replaced with \b Form feed is replaced with \f Newline is
     * replaced with \n Carriage return is replaced with \r Tab is replaced with \t
     * Double quote is replaced with \" Backslash is replaced with \\
     */
    return StringEscapeUtils.escapeJson(text);
  }

}
