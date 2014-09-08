/*
 * Copyright 2014 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.joda;

import java.util.Collections;
import java.util.Set;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.ReadablePartial;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public  class IsoDate extends LogicalType {
    private static final Set<String> RESERVED = Collections.EMPTY_SET;
    
    private IsoDate(int precision, int scale) {
      super(RESERVED, "isodate");
    }

    public IsoDate(JsonNode node) {
      super(RESERVED, "isodate");
    }

    @Override
    public void validate(Schema schema) {
      // validate the type
      if (schema.getType() != Schema.Type.LONG &&
          schema.getType() != Schema.Type.STRING) {
        throw new IllegalArgumentException(
            "Logical type " + this + " must be backed by long or string");
      }
    }

    @Override
    public Set<String> reserved() {
      return RESERVED;
    }

    @Override
    public Class<?> getLogicalJavaType() {
        return LocalDate.class;
    }

    private static final DateTimeFormatter FMT = ISODateTimeFormat.date();
    
    @Override
    public Object deserialize(Schema.Type type, Object object) {
                  switch (type) {
              case STRING:
                  return FMT.parseLocalDate(((CharSequence) object).toString());
              case LONG:
                  return new DateTime((Long) object, DateTimeZone.UTC).toLocalDate();
              default:
                  throw new UnsupportedOperationException();
          }
    }

    @Override
    public Object serialize(Schema.Type type, Object object) {
                  switch (type) {
              case STRING:
                  return FMT.print((ReadablePartial) object);
              case LONG:
                  return ((LocalDate) object).toDateTimeAtStartOfDay(DateTimeZone.UTC).getMillis();                
              default:
                  throw new UnsupportedOperationException();
          }
    }

  }

