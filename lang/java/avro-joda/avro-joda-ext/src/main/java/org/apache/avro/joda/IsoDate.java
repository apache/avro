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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Collections;
import java.util.Set;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.ReadablePartial;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public  class IsoDate extends LogicalType {
    private static final Set<String> RESERVED = Collections.EMPTY_SET;
    
    private IsoDate() {
      super(RESERVED, "isodate");
    }

    public IsoDate(JsonNode node) {
      super(RESERVED, "isodate");
    }

    @Override
    public void validate(Schema schema) {
      // validate the type
      if (schema.getType() != Schema.Type.INT && schema.getType() != Schema.Type.LONG &&
          schema.getType() != Schema.Type.STRING) {
        throw new IllegalArgumentException(
            "Logical type " + this + " must be backed by long or int or string");
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

    public static final DateTimeFormatter FMT = ISODateTimeFormat.date();
    
    // This can be anything really, the number serialized will be the number of days between this date and the date.
    // this serialized number will be negative for anything before 1970-01-01.
    private static final LocalDate EPOCH = new LocalDate(0L, DateTimeZone.UTC);
    
    private static final LoadingCache<LocalDate, String> D2S_CONV_CACHE = CacheBuilder.newBuilder()
            .maximumSize(2048)
            .build(new CacheLoader<LocalDate, String> () {

      @Override
      public String load(final LocalDate key) throws Exception {
        return FMT.print(key);
      }
    });
    
    
    private static final LoadingCache<String, LocalDate> S2D_CONV_CACHE = CacheBuilder.newBuilder()
            .maximumSize(2048)
            .build(new CacheLoader<String, LocalDate> () {

      @Override
      public LocalDate load(final String key) throws Exception {
        return FMT.parseLocalDate(key);
      }
    });
        
    
    @Override
    public Object deserialize(Schema.Type type, Object object) {
                  switch (type) {
              case STRING:
                  return S2D_CONV_CACHE.getUnchecked(object.toString());
              case LONG: // start of day millis
                  return new DateTime((Long) object, DateTimeZone.UTC).toLocalDate();
              case INT: // nr of days since epoch                  
                return EPOCH.plusDays((Integer) object);
              default:
                  throw new UnsupportedOperationException();
          }
    }

    @Override
    public Object serialize(Schema.Type type, Object object) {
                  switch (type) {
              case STRING:
                  D2S_CONV_CACHE.getUnchecked((LocalDate) object);
              case LONG:
                  return ((LocalDate) object).toDateTimeAtStartOfDay(DateTimeZone.UTC).getMillis();                
              case INT:
                  return Days.daysBetween(EPOCH, (ReadablePartial) object).getDays();
              default:
                  throw new UnsupportedOperationException();
          }
    }

  }

