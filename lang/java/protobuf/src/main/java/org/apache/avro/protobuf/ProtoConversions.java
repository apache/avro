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

  public static class TimestampConversion extends Conversion<Timestamp> {
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
      long seconds = millisFromEpoch / 1000;
      int nanos = (int) (millisFromEpoch - seconds * 1000) * 1000000;
      return Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();
    }

    @Override
    public Long toLong(Timestamp value, Schema schema, LogicalType type) {
      return value.getSeconds() * 1000 + value.getNanos() / 1000000;
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
      long seconds = microsFromEpoch / 1000000;
      int nanos = (int) (microsFromEpoch - seconds * 1000000) * 1000;
      return Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();
    }

    @Override
    public Long toLong(Timestamp value, Schema schema, LogicalType type) {
      if (value.getNanos() - (value.getNanos() / 1000000 * 1000000) > 0) {
        throw new UnsupportedOperationException("micros-precise is unsupported");
      }

      return value.getSeconds() * 1000 + value.getNanos() / 1000000;
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    }
  }

}
