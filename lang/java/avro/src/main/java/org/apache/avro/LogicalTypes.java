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

package org.apache.avro;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.logicaltypes.AvroByte;
import org.apache.avro.logicaltypes.AvroCLOB;
import org.apache.avro.logicaltypes.AvroDate;
import org.apache.avro.logicaltypes.AvroDecimal;
import org.apache.avro.logicaltypes.AvroLocalTimestampMicros;
import org.apache.avro.logicaltypes.AvroLocalTimestampMillis;
import org.apache.avro.logicaltypes.AvroNCLOB;
import org.apache.avro.logicaltypes.AvroNVarchar;
import org.apache.avro.logicaltypes.AvroSTGeometry;
import org.apache.avro.logicaltypes.AvroSTPoint;
import org.apache.avro.logicaltypes.AvroShort;
import org.apache.avro.logicaltypes.AvroTimeMicros;
import org.apache.avro.logicaltypes.AvroTimeMillis;
import org.apache.avro.logicaltypes.AvroTimestampMicros;
import org.apache.avro.logicaltypes.AvroTimestampMillis;
import org.apache.avro.logicaltypes.AvroUUID;
import org.apache.avro.logicaltypes.AvroUri;
import org.apache.avro.logicaltypes.AvroVarchar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogicalTypes {

  private static final Logger LOG = LoggerFactory.getLogger(LogicalTypes.class);

  public interface LogicalTypeFactory {
    LogicalType fromSchema(Schema schema);

    default String getTypeName() {
      throw new UnsupportedOperationException();
    }
  }

  private static final Map<String, LogicalTypeFactory> REGISTERED_TYPES = new ConcurrentHashMap<>();

  /**
   * Register a logical type.
   *
   * @param logicalTypeName The logical type name
   * @param factory         The logical type factory
   *
   * @throws NullPointerException if {@code logicalTypeName} or {@code factory} is
   *                              {@code null}
   */
  public static void register(String logicalTypeName, LogicalTypeFactory factory) {
    Objects.requireNonNull(logicalTypeName, "Logical type name cannot be null");
    Objects.requireNonNull(factory, "Logical type factory cannot be null");
    REGISTERED_TYPES.put(logicalTypeName, factory);
  }

  /**
   * Returns the {@link LogicalType} from the schema, if one is present.
   */
  public static LogicalType fromSchema(Schema schema) {
    return fromSchemaImpl(schema, true);
  }

  public static LogicalType fromSchemaIgnoreInvalid(Schema schema) {
    return fromSchemaImpl(schema, false);
  }

  private static LogicalType fromSchemaImpl(Schema schema, boolean throwErrors) {
    final LogicalType logicalType;
    final String typeName = schema.getProp(LogicalType.LOGICAL_TYPE_PROP);

    if (typeName == null) {
      return null;
    }

    try {
      switch (typeName) {
      case AvroByte.TYPENAME:
        logicalType = AvroByte.create();
        break;
      case AvroCLOB.TYPENAME:
        logicalType = AvroCLOB.create();
        break;
      case AvroDate.TYPENAME:
        logicalType = AvroDate.create();
        break;
      case AvroDecimal.TYPENAME:
        logicalType = new AvroDecimal(schema);
        break;
      case AvroNCLOB.TYPENAME:
        logicalType = AvroNCLOB.create();
        break;
      case AvroNVarchar.TYPENAME:
        logicalType = new AvroNVarchar(schema);
        break;
      case AvroShort.TYPENAME:
        logicalType = AvroShort.create();
        break;
      case AvroSTGeometry.TYPENAME:
        logicalType = AvroSTGeometry.create();
        break;
      case AvroSTPoint.TYPENAME:
        logicalType = AvroSTPoint.create();
        break;
      case AvroTimeMicros.TYPENAME:
        logicalType = AvroTimeMicros.create();
        break;
      case AvroTimeMillis.TYPENAME:
        logicalType = AvroTimeMillis.create();
        break;
      case AvroTimestampMicros.TYPENAME:
        logicalType = AvroTimestampMicros.create();
        break;
      case AvroTimestampMillis.TYPENAME:
        logicalType = AvroTimestampMillis.create();
        break;
      case AvroUri.TYPENAME:
        logicalType = AvroUri.create();
        break;
      case AvroUUID.TYPENAME:
        logicalType = AvroUUID.create();
        break;
      case AvroVarchar.TYPENAME:
        logicalType = new AvroVarchar(schema);
        break;
      case AvroLocalTimestampMicros.TYPENAME:
        logicalType = AvroLocalTimestampMicros.create();
        break;
      case AvroLocalTimestampMillis.TYPENAME:
        logicalType = AvroLocalTimestampMillis.create();
        break;
      default:
        final LogicalTypeFactory typeFactory = REGISTERED_TYPES.get(typeName);
        logicalType = (typeFactory == null) ? null : typeFactory.fromSchema(schema);
        break;
      }

      // make sure the type is valid before returning it
      if (logicalType != null) {
        logicalType.validate(schema);
      }
    } catch (RuntimeException e) {
      LOG.debug("Invalid logical type found", e);
      if (throwErrors) {
        throw e;
      }
      LOG.warn("Ignoring invalid logical type for name: {}", typeName);
      // ignore invalid types
      return null;
    }

    return logicalType;
  }

  public static final String DECIMAL = "decimal";
  public static final String UUID = "uuid";
  public static final String DATE = "date";
  public static final String TIME_MILLIS = "time-millis";
  public static final String TIME_MICROS = "time-micros";
  public static final String TIMESTAMP_MILLIS = "timestamp-millis";
  public static final String TIMESTAMP_MICROS = "timestamp-micros";
  public static final String LOCAL_TIMESTAMP_MILLIS = "local-timestamp-millis";
  public static final String LOCAL_TIMESTAMP_MICROS = "local-timestamp-micros";

  /** Create a Decimal LogicalType with the given precision and scale 0 */
  public static AvroDecimal decimal(int precision) {
    return decimal(precision, 0);
  }

  /** Create a Decimal LogicalType with the given precision and scale */
  public static AvroDecimal decimal(int precision, int scale) {
    return new AvroDecimal(precision, scale);
  }

  private static final LogicalType UUID_TYPE = AvroUUID.create();

  public static LogicalType uuid() {
    return UUID_TYPE;
  }

  private static final Date DATE_TYPE = AvroDate.create();

  public static Date date() {
    return DATE_TYPE;
  }

  private static final TimeMillis TIME_MILLIS_TYPE = AvroTimeMillis.create();

  public static TimeMillis timeMillis() {
    return TIME_MILLIS_TYPE;
  }

  private static final TimeMicros TIME_MICROS_TYPE = AvroTimeMicros.create();

  public static TimeMicros timeMicros() {
    return TIME_MICROS_TYPE;
  }

  private static final TimestampMillis TIMESTAMP_MILLIS_TYPE = AvroTimestampMillis.create();

  public static TimestampMillis timestampMillis() {
    return TIMESTAMP_MILLIS_TYPE;
  }

  private static final TimestampMicros TIMESTAMP_MICROS_TYPE = AvroTimestampMicros.create();

  public static TimestampMicros timestampMicros() {
    return TIMESTAMP_MICROS_TYPE;
  }

  private static final LocalTimestampMillis LOCAL_TIMESTAMP_MILLIS_TYPE = AvroLocalTimestampMillis.create();

  public static LocalTimestampMillis localTimestampMillis() {
    return LOCAL_TIMESTAMP_MILLIS_TYPE;
  }

  private static final LocalTimestampMicros LOCAL_TIMESTAMP_MICROS_TYPE = AvroLocalTimestampMicros.create();

  public static LocalTimestampMicros localTimestampMicros() {
    return LOCAL_TIMESTAMP_MICROS_TYPE;
  }

  /** Decimal represents arbitrary-precision fixed-scale decimal numbers */
  public static class Decimal extends LogicalType {
    private static final String PRECISION_PROP = "precision";
    private static final String SCALE_PROP = "scale";

    private int precision;
    private int scale;

    protected Decimal(int precision, int scale) {
      super(DECIMAL);
      this.precision = precision;
      this.scale = scale;
    }

    protected Decimal(Schema schema) {
      super(DECIMAL);
      if (!hasProperty(schema, PRECISION_PROP)) {
        throw new IllegalArgumentException("Invalid decimal: missing precision");
      }

      this.precision = getInt(schema, PRECISION_PROP);

      if (hasProperty(schema, SCALE_PROP)) {
        this.scale = getInt(schema, SCALE_PROP);
      } else {
        this.scale = 0;
      }
    }

    protected Decimal(String text) {
      super(DECIMAL);
      String[] parts = text.split("[\\(\\)\\,]");
      this.precision = 28;
      this.scale = 7;
      if (parts.length > 1) {
        this.precision = Integer.parseInt(parts[1]);
      }
      if (parts.length > 2) {
        this.scale = Integer.parseInt(parts[2]);
      }
    }

    @Override
    public Schema addToSchema(Schema schema) {
      super.addToSchema(schema);
      schema.addProp(PRECISION_PROP, precision);
      schema.addProp(SCALE_PROP, scale);
      return schema;
    }

    public int getPrecision() {
      return precision;
    }

    public int getScale() {
      return scale;
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      // validate the type
      if (schema.getType() != Schema.Type.FIXED && schema.getType() != Schema.Type.BYTES) {
        throw new IllegalArgumentException("Logical type decimal must be backed by fixed or bytes");
      }
      if (precision <= 0) {
        throw new IllegalArgumentException("Invalid decimal precision: " + precision + " (must be positive)");
      } else if (precision > maxPrecision(schema)) {
        throw new IllegalArgumentException("fixed(" + schema.getFixedSize() + ") cannot store " + precision
            + " digits (max " + maxPrecision(schema) + ")");
      }
      if (scale < 0) {
        throw new IllegalArgumentException("Invalid decimal scale: " + scale + " (must be positive)");
      } else if (scale > precision) {
        throw new IllegalArgumentException(
            "Invalid decimal scale: " + scale + " (greater than precision: " + precision + ")");
      }
    }

    private long maxPrecision(Schema schema) {
      if (schema.getType() == Schema.Type.BYTES) {
        // not bounded
        return Integer.MAX_VALUE;
      } else if (schema.getType() == Schema.Type.FIXED) {
        int size = schema.getFixedSize();
        return Math.round(Math.floor(Math.log10(2) * (8 * size - 1)));
      } else {
        // not valid for any other type
        return 0;
      }
    }

    private boolean hasProperty(Schema schema, String name) {
      return (schema.getObjectProp(name) != null);
    }

    private int getInt(Schema schema, String name) {
      Object obj = schema.getObjectProp(name);
      if (obj instanceof Integer) {
        return (Integer) obj;
      }
      throw new IllegalArgumentException(
          "Expected int " + name + ": " + (obj == null ? "null" : obj + ":" + obj.getClass().getSimpleName()));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      Decimal decimal = (Decimal) o;

      if (precision != decimal.precision)
        return false;
      return scale == decimal.scale;
    }

    @Override
    public int hashCode() {
      int result = precision;
      result = 31 * result + scale;
      return result;
    }
  }

  /** Date represents a date without a time */
  public static class Date extends LogicalType {
    protected Date() {
      super(DATE);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.INT) {
        throw new IllegalArgumentException("Date can only be used with an underlying int type");
      }
    }
  }

  /** TimeMillis represents a time in milliseconds without a date */
  public static class TimeMillis extends LogicalType {
    protected TimeMillis() {
      super(TIME_MILLIS);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.INT) {
        throw new IllegalArgumentException("Time (millis) can only be used with an underlying int type");
      }
    }
  }

  /** TimeMicros represents a time in microseconds without a date */
  public static class TimeMicros extends LogicalType {
    protected TimeMicros() {
      super(TIME_MICROS);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.LONG) {
        throw new IllegalArgumentException("Time (micros) can only be used with an underlying long type");
      }
    }
  }

  /** TimestampMillis represents a date and time in milliseconds */
  public static class TimestampMillis extends LogicalType {
    protected TimestampMillis() {
      super(TIMESTAMP_MILLIS);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.LONG) {
        throw new IllegalArgumentException("Timestamp (millis) can only be used with an underlying long type");
      }
    }
  }

  /** TimestampMicros represents a date and time in microseconds */
  public static class TimestampMicros extends LogicalType {
    protected TimestampMicros() {
      super(TIMESTAMP_MICROS);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.LONG) {
        throw new IllegalArgumentException("Timestamp (micros) can only be used with an underlying long type");
      }
    }
  }

  public static class LocalTimestampMillis extends LogicalType {
    protected LocalTimestampMillis() {
      super(LOCAL_TIMESTAMP_MILLIS);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.LONG) {
        throw new IllegalArgumentException("Local timestamp (millis) can only be used with an underlying long type");
      }
    }
  }

  public static class LocalTimestampMicros extends LogicalType {
    protected LocalTimestampMicros() {
      super(LOCAL_TIMESTAMP_MICROS);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.LONG) {
        throw new IllegalArgumentException("Local timestamp (micros) can only be used with an underlying long type");
      }
    }
  }

}
