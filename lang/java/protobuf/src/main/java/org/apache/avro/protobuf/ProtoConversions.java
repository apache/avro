/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.protobuf;

import com.google.protobuf.Timestamp;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

public class ProtoConversions {

  private static final int THOUSAND = 1000;
  private static final int MILLION = 1000000;

  public static class TimestampMillisConversion extends Conversion<Timestamp> {
    @Override
    public Class<Timestamp> getConvertedType() {
      return Timestamp.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "timestamp-millis";
    }

    @Override
    public Timestamp fromLong(Long millisFromEpoch, Schema schema, LogicalType type) {
      long seconds = millisFromEpoch / THOUSAND;
      int nanos = (int) (millisFromEpoch - seconds * THOUSAND) * MILLION;
      return Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();
    }

    @Override
    public Long toLong(Timestamp value, Schema schema, LogicalType type) {
      return ProtoConversions.toLong(value);
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    }
  }

  public static class TimestampMicrosConversion extends Conversion<Timestamp> {
    @Override
    public Class<Timestamp> getConvertedType() {
      return Timestamp.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "timestamp-micros";
    }

    @Override
    public Timestamp fromLong(Long microsFromEpoch, Schema schema, LogicalType type) {
      long seconds = microsFromEpoch / MILLION;
      int nanos = (int) (microsFromEpoch - seconds * MILLION) * THOUSAND;
      return Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();
    }

    @Override
    public Long toLong(Timestamp value, Schema schema, LogicalType type) {
      if (value.getNanos() - (value.getNanos() / MILLION * MILLION) > 0) {
        throw new UnsupportedOperationException("micros-precise is unsupported");
      }

      return ProtoConversions.toLong(value);
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    }
  }

  private static long toLong(Timestamp value) {
    return value.getSeconds() * THOUSAND + value.getNanos() / MILLION;
  }
}
