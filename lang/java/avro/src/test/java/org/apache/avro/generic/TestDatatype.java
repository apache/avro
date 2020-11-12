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
package org.apache.avro.generic;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.data.TimeConversions.DateConversion;
import org.apache.avro.data.TimeConversions.LocalTimestampMicrosConversion;
import org.apache.avro.data.TimeConversions.LocalTimestampMillisConversion;
import org.apache.avro.data.TimeConversions.TimeMicrosConversion;
import org.apache.avro.data.TimeConversions.TimeMillisConversion;
import org.apache.avro.data.TimeConversions.TimestampMicrosConversion;
import org.apache.avro.data.TimeConversions.TimestampMillisConversion;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.logicaltypes.AvroBoolean;
import org.apache.avro.logicaltypes.AvroBytes;
import org.apache.avro.logicaltypes.AvroDatatype;
import org.apache.avro.logicaltypes.AvroDate;
import org.apache.avro.logicaltypes.AvroDecimal;
import org.apache.avro.logicaltypes.AvroDouble;
import org.apache.avro.logicaltypes.AvroFloat;
import org.apache.avro.logicaltypes.AvroInt;
import org.apache.avro.logicaltypes.AvroLocalTimestampMicros;
import org.apache.avro.logicaltypes.AvroLocalTimestampMillis;
import org.apache.avro.logicaltypes.AvroLong;
import org.apache.avro.logicaltypes.AvroString;
import org.apache.avro.logicaltypes.AvroTimeMicros;
import org.apache.avro.logicaltypes.AvroTimeMillis;
import org.apache.avro.logicaltypes.AvroTimestampMicros;
import org.apache.avro.logicaltypes.AvroTimestampMillis;
import org.junit.Test;

public class TestDatatype {
  String fortyTwo = "42";
  Integer fortyTwoInt = Integer.valueOf(42);
  Long fortyTwoLong = Long.valueOf(42);
  Byte fortyTwoByte = Byte.valueOf((byte) 42);
  byte[] fortyTwoBytes1 = { Byte.valueOf((byte) 42) };
  Float fortyTwoFloat = Float.valueOf((float) 42.0);
  Double fortyTwoDouble = Double.valueOf(42.0);
  byte[] fortyTwoByteArray = { 0x04, 0x02 };
  List<String> enumValues = new ArrayList<>();
  Schema arraySchema = Schema.createArray(Schema.create(Type.STRING));
  GenericArray<String> array = new GenericData.Array<>(1, arraySchema);
  Map<String, String> map = new HashMap<>();
  Schema recordSchema = SchemaBuilder.record("aRecord").fields().requiredString("myString").endRecord();
  GenericRecord subRecord = new GenericData.Record(recordSchema);

  @Test
  public void testDatatypeForPrimitives() throws IOException {
    Map<Field, Object> fields = new HashMap<>();
    fields.put(new Field("string1", Schema.create(Type.STRING), null, null), fortyTwo);
    fields.put(new Field("string2", AvroString.create().getRecommendedSchema(), null, null), fortyTwoInt);

    fields.put(new Field("bytes1", Schema.create(Type.BYTES), null, null), ByteBuffer.wrap(fortyTwoByteArray));
    fields.put(new Field("bytes2", AvroBytes.create().getRecommendedSchema(), null, null), fortyTwoByte);

    fields.put(new Field("int1", Schema.create(Type.INT), null, null), fortyTwoInt);
    fields.put(new Field("int2", AvroInt.create().getRecommendedSchema(), null, null), fortyTwo);

    fields.put(new Field("long1", Schema.create(Type.LONG), null, null), fortyTwoLong);
    fields.put(new Field("long2", AvroLong.create().getRecommendedSchema(), null, null), fortyTwo);

    fields.put(new Field("float1", Schema.create(Type.FLOAT), null, null), fortyTwoFloat);
    fields.put(new Field("float2", AvroFloat.create().getRecommendedSchema(), null, null), fortyTwoInt);

    fields.put(new Field("double1", Schema.create(Type.DOUBLE), null, null), fortyTwoDouble);
    fields.put(new Field("double2", AvroDouble.create().getRecommendedSchema(), null, null), fortyTwoInt);

    fields.put(new Field("boolean1", Schema.create(Type.BOOLEAN), null, null), true);
    fields.put(new Field("boolean2", AvroBoolean.create().getRecommendedSchema(), null, null), "True");

    Schema fixedSchema = Schema.createFixed("fixed", null, null, 2);
    fields.put(new Field("fixed1", fixedSchema, null, null), new GenericData.Fixed(fixedSchema, fortyTwoByteArray));
    fields.put(new Field("fixed2", fixedSchema), fortyTwoByteArray);

    enumValues.add("One");
    enumValues.add("Two");
    Schema enumSchema = Schema.createEnum("myEnum", null, null, enumValues);
    fields.put(new Field("enum1", enumSchema, null, null), new GenericData.EnumSymbol(enumSchema, enumValues.get(0)));
    fields.put(new Field("enum2", enumSchema, null, null), enumValues.get(0));

    array.add(fortyTwo);
    fields.put(new Field("array1", arraySchema, null, null), array);
    fields.put(new Field("array2", arraySchema, null, null), array);

    Schema mapSchema = Schema.createMap(Schema.create(Type.STRING));
    map.put(fortyTwo, fortyTwo);
    fields.put(new Field("map1", mapSchema, null, null), map);
    fields.put(new Field("map2", mapSchema, null, null), map);

    subRecord.put("myString", "42");
    fields.put(new Field("record1", recordSchema, null, null), subRecord);
    fields.put(new Field("record2", recordSchema, null, null), subRecord);

    Schema schema = Schema.createRecord("Foo", "test", "mytest", false);
    List<Field> fieldlist = new ArrayList<Field>();
    fieldlist.addAll(fields.keySet());

    schema.setFields(fieldlist);

    Record testRecord = new Record(schema);

    for (Field f : fields.keySet()) {
      Object value = fields.get(f);
      testRecord.put(f.name(), f.getDataType().convertToRawType(value));
    }

    validateRecord(testRecord, fields);

    /*
     * It might be that putting values does not change the object thus reading the
     * same object and comparing is kind of a no-operation. Thus the data is put
     * through a DatumWriter/Reader to make sure all objects are recreated for sure.
     */

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().validatingEncoder(schema,
        EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null));
    datumWriter.write(testRecord, encoder);
    encoder.flush();

    Decoder decoder = DecoderFactory.get().validatingDecoder(schema,
        DecoderFactory.get().binaryDecoder(byteArrayOutputStream.toByteArray(), null));
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    GenericRecord testRecord2 = datumReader.read(null, decoder);

    validateRecord(testRecord2, fields);
  }

  String fortwodatestring = "2004-02-01";
  String fortwotimestring = "00:04:02.00";
  Instant fortwoinstant = Instant.parse(fortwodatestring + "T" + fortwotimestring + "Z");
  String forcommatwostring = "4.2";
  BigDecimal forcommatwo = BigDecimal.valueOf(4.2);
  static DateConversion DATE_CONVERTER = new DateConversion();
  static final DecimalConversion DECIMAL_CONVERTER = new DecimalConversion();
  static final TimestampMicrosConversion TSMICRO_CONVERTER = new TimestampMicrosConversion();
  static final TimestampMillisConversion TSMILLI_CONVERTER = new TimestampMillisConversion();
  static final LocalTimestampMicrosConversion LOCALTSMICRO_CONVERTER = new LocalTimestampMicrosConversion();
  static final LocalTimestampMillisConversion LOCALTSMILLI_CONVERTER = new LocalTimestampMillisConversion();
  static final TimeMicrosConversion TIMEMICRO_CONVERTER = new TimeMicrosConversion();
  static final TimeMillisConversion TIMEMILLI_CONVERTER = new TimeMillisConversion();

  @Test
  public void testDatatypeForLogicalType1() throws IOException {
    Map<Field, Object> fields = new HashMap<>();

    Schema dateschema = LogicalTypes.date().addToSchema(Schema.create(Type.INT));
    fields.put(new Field("date1", AvroDate.create().getRecommendedSchema(), null, null), "2004-02-01");
    fields.put(new Field("date2", AvroDate.create().getRecommendedSchema(), null, null), LocalDate.parse("2004-02-01"));
    fields.put(new Field("date3", AvroDate.create().getRecommendedSchema(), null, null),
        LocalDate.parse("2004-02-01").toEpochDay());
    fields.put(new Field("date4", AvroDate.create().getRecommendedSchema(), null, null), Date.from(fortwoinstant));
    fields.put(new Field("date5", AvroDate.create().getRecommendedSchema(), null, null),
        ZonedDateTime.ofInstant(fortwoinstant, ZoneId.of("UTC")));
    fields.put(new Field("date6", AvroDate.create().getRecommendedSchema(), null, null), fortwoinstant);
    fields.put(new Field("dateC", dateschema, null, null),
        DATE_CONVERTER.toInt(fortwoinstant.atOffset(ZoneOffset.UTC).toLocalDate(), dateschema, LogicalTypes.date()));

    fields.put(new Field("decimal1", new AvroDecimal(27, 7).getRecommendedSchema(), null, null), forcommatwo);
    fields.put(new Field("decimal2", new AvroDecimal(27, 7).getRecommendedSchema(), null, null),
        forcommatwo.floatValue());
    fields.put(new Field("decimal3", new AvroDecimal(27, 7).getRecommendedSchema(), null, null), forcommatwostring);
    LogicalType decimaltype = LogicalTypes.decimal(27, 7);
    Schema decimalschema = decimaltype.addToSchema(Schema.create(Type.BYTES));
    fields.put(new Field("decimalC", decimalschema, null, null),
        DECIMAL_CONVERTER.toBytes(forcommatwo, decimalschema, decimaltype));

    Schema timestampmicroschema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Type.LONG));
    fields.put(new Field("tsMicros1", AvroTimestampMicros.create().getRecommendedSchema(), null, null),
        fortwodatestring + "T" + fortwotimestring + "Z");
    fields.put(new Field("tsMicros2", AvroTimestampMicros.create().getRecommendedSchema(), null, null), fortwoinstant);
    fields.put(new Field("tsMicros3", AvroTimestampMicros.create().getRecommendedSchema(), null, null),
        ZonedDateTime.ofInstant(fortwoinstant, ZoneId.of("UTC")));
    fields.put(new Field("tsMicros4", AvroTimestampMicros.create().getRecommendedSchema(), null, null),
        Date.from(fortwoinstant));
    fields.put(new Field("tsMicros5", AvroTimestampMicros.create().getRecommendedSchema(), null, null), fortwoinstant);
    fields.put(new Field("tsMicrosC", timestampmicroschema, null, null),
        TSMICRO_CONVERTER.toLong(fortwoinstant, timestampmicroschema, LogicalTypes.timestampMicros()));

    Schema timestampmillischema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Type.LONG));
    fields.put(new Field("tsMillis1", AvroTimestampMillis.create().getRecommendedSchema(), null, null),
        fortwodatestring + "T" + fortwotimestring + "Z");
    fields.put(new Field("tsMillis2", AvroTimestampMillis.create().getRecommendedSchema(), null, null), fortwoinstant);
    fields.put(new Field("tsMillis3", AvroTimestampMillis.create().getRecommendedSchema(), null, null),
        ZonedDateTime.ofInstant(fortwoinstant, ZoneId.of("UTC")));
    fields.put(new Field("tsMillis4", AvroTimestampMillis.create().getRecommendedSchema(), null, null),
        Date.from(fortwoinstant));
    fields.put(new Field("tsMillis5", AvroTimestampMillis.create().getRecommendedSchema(), null, null), fortwoinstant);
    fields.put(new Field("tsMillisC", timestampmillischema, null, null),
        TSMILLI_CONVERTER.toLong(fortwoinstant, timestampmillischema, LogicalTypes.timestampMillis()));

    Schema localtimestampmicroschema = LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Type.LONG));
    fields.put(new Field("localtsMicros1", AvroLocalTimestampMicros.create().getRecommendedSchema(), null, null),
        LocalDateTime.ofInstant(fortwoinstant, ZoneId.of("UTC")));
    fields.put(new Field("localtsMicros2", AvroLocalTimestampMicros.create().getRecommendedSchema(), null, null),
        Date.from(fortwoinstant));
    fields.put(new Field("localtsMicros3", AvroLocalTimestampMicros.create().getRecommendedSchema(), null, null),
        ZonedDateTime.ofInstant(fortwoinstant, ZoneId.of("UTC")));
    fields.put(new Field("localtsMicros4", AvroLocalTimestampMicros.create().getRecommendedSchema(), null, null),
        fortwoinstant);
    fields.put(new Field("localtsMicros5", AvroLocalTimestampMicros.create().getRecommendedSchema(), null, null),
        fortwodatestring + "T" + fortwotimestring);
    fields.put(new Field("localtsMicrosC", localtimestampmicroschema, null, null),
        LOCALTSMICRO_CONVERTER.toLong(fortwoinstant.atZone(ZoneId.of("UTC")).toLocalDateTime(),
            localtimestampmicroschema, LogicalTypes.localTimestampMicros()));

    Schema localtimestampmillischema = LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Type.LONG));
    fields.put(new Field("localtsMillis1", AvroLocalTimestampMillis.create().getRecommendedSchema(), null, null),
        LocalDateTime.ofInstant(fortwoinstant, ZoneId.of("UTC")));
    fields.put(new Field("localtsMillis2", AvroLocalTimestampMillis.create().getRecommendedSchema(), null, null),
        Date.from(fortwoinstant));
    fields.put(new Field("localtsMillis3", AvroLocalTimestampMillis.create().getRecommendedSchema(), null, null),
        ZonedDateTime.ofInstant(fortwoinstant, ZoneId.of("UTC")));
    fields.put(new Field("localtsMillis4", AvroLocalTimestampMillis.create().getRecommendedSchema(), null, null),
        fortwoinstant);
    fields.put(new Field("localtsMillis5", AvroLocalTimestampMillis.create().getRecommendedSchema(), null, null),
        fortwodatestring + "T" + fortwotimestring);
    fields.put(new Field("localtsMillisC", localtimestampmillischema, null, null),
        LOCALTSMILLI_CONVERTER.toLong(fortwoinstant.atZone(ZoneId.of("UTC")).toLocalDateTime(),
            localtimestampmillischema, LogicalTypes.localTimestampMillis()));

    Schema timemicroschema = LogicalTypes.timeMicros().addToSchema(Schema.create(Type.LONG));
    fields.put(new Field("timeMicros1", AvroTimeMicros.create().getRecommendedSchema(), null, null), fortwoinstant);
    fields.put(new Field("timeMicros2", AvroTimeMicros.create().getRecommendedSchema(), null, null),
        fortwoinstant.atZone(ZoneId.of("UTC")).toLocalTime());
    fields.put(new Field("timeMicros3", AvroTimeMicros.create().getRecommendedSchema(), null, null),
        Date.from(fortwoinstant));
    fields.put(new Field("timeMicros4", AvroTimeMicros.create().getRecommendedSchema(), null, null), fortwotimestring);
    fields.put(new Field("timeMicrosC", timemicroschema, null, null), TIMEMICRO_CONVERTER
        .toLong(fortwoinstant.atZone(ZoneId.of("UTC")).toLocalTime(), timemicroschema, LogicalTypes.timeMicros()));

    Schema timemillischema = LogicalTypes.timeMillis().addToSchema(Schema.create(Type.INT));
    fields.put(new Field("timeMillis1", AvroTimeMillis.create().getRecommendedSchema(), null, null), fortwoinstant);
    fields.put(new Field("timeMillis2", AvroTimeMillis.create().getRecommendedSchema(), null, null),
        fortwoinstant.atZone(ZoneId.of("UTC")).toLocalTime());
    fields.put(new Field("timeMillis3", AvroTimeMillis.create().getRecommendedSchema(), null, null),
        Date.from(fortwoinstant));
    fields.put(new Field("timeMillis4", AvroTimeMillis.create().getRecommendedSchema(), null, null), fortwotimestring);
    fields.put(new Field("timeMillisC", timemillischema, null, null), TIMEMILLI_CONVERTER
        .toInt(fortwoinstant.atZone(ZoneId.of("UTC")).toLocalTime(), timemillischema, LogicalTypes.timeMillis()));

    Schema schema = Schema.createRecord("Foo", "test", "mytest", false);
    List<Field> fieldlist = new ArrayList<Field>();
    fieldlist.addAll(fields.keySet());

    schema.setFields(fieldlist);

    Record testRecord = new Record(schema);

    for (Field f : fields.keySet()) {
      Object value = fields.get(f);
      testRecord.put(f.name(), f.getDataType().convertToRawType(value));
    }

    validateRecordLogicalType1(testRecord, fields);

    /*
     * It might be that putting values does not change the object thus reading the
     * same object and comparing is kind of a no-operation. Thus the data is put
     * through a DatumWriter/Reader to make sure all objects are recreated for sure.
     */

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().validatingEncoder(schema,
        EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null));
    datumWriter.write(testRecord, encoder);
    encoder.flush();

    Decoder decoder = DecoderFactory.get().validatingDecoder(schema,
        DecoderFactory.get().binaryDecoder(byteArrayOutputStream.toByteArray(), null));
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    GenericRecord testRecord2 = datumReader.read(null, decoder);

    validateRecordLogicalType1(testRecord2, fields);

  }

  private void validateRecord(GenericRecord testRecord, Map<Field, Object> fields) {
    for (Field f : fields.keySet()) {
      AvroDatatype datatype = f.getDataType();
      Object o = datatype.convertToLogicalType(testRecord.get(f.name()));
      switch (datatype.getAvroType()) {
      case AVROARRAY:
        Array<?> a = (Array<?>) o;
        // Using the toString, otherwise a String is compared with an Avro Utf8 array
        // element
        assertEquals(f.name() + " != [\"42\"]", array.toString(), a.toString());
        break;
      case AVROBOOLEAN:
        assertEquals(f.name() + " != true", Boolean.TRUE, o);
        break;
      case AVROBYTES:
        if (f.name().equals("bytes1")) {
          assertArrayEquals(f.name() + " != [0x04, 0x02]", fortyTwoByteArray, ((ByteBuffer) o).array());
        } else {
          assertArrayEquals(f.name() + " != [42]", fortyTwoBytes1, ((ByteBuffer) o).array());
        }
        break;
      case AVRODOUBLE:
        assertEquals(f.name() + " != 42.0", fortyTwoDouble, o);
        break;
      case AVROENUM:
        assertEquals(f.name() + " != One", enumValues.get(0), ((EnumSymbol) o).toString());
        break;
      case AVROFIXED:
        assertArrayEquals(f.name() + " != [0x04, 0x02]", fortyTwoByteArray, ((GenericFixed) o).bytes());
        break;
      case AVROFLOAT:
        assertEquals(f.name() + " != true", fortyTwoFloat, o);
        break;
      case AVROINT:
        assertEquals(f.name() + " != true", fortyTwoInt, o);
        break;
      case AVROLONG:
        assertEquals(f.name() + " != true", fortyTwoLong, o);
        break;
      case AVROMAP:
        // Using toString() as cheap method for comparing all Map elements
        assertEquals(f.name() + " != {42=42}", map.toString(), o.toString());
        break;
      case AVRORECORD:
        assertEquals(f.name() + " != { \"myString\"}", subRecord, o);
        break;
      case AVROSTRING:
        assertEquals(f.name() + " != \"42\"", fortyTwo, o.toString());
        break;
      default:
        break;
      }
    }
  }

  private void validateRecordLogicalType1(GenericRecord testRecord, Map<Field, Object> fields) {
    for (Field f : fields.keySet()) {
      if (!f.name().endsWith("C")) { // only take fields that have been defined using the new logical types way
        AvroDatatype datatype = f.getDataType();
        Object oNew = datatype.convertToLogicalType(testRecord.get(f.name()));
        Object oRawNew = testRecord.get(f.name());
        String referencefieldname = f.name().substring(0, f.name().length() - 1) + "C";
        Object oRawClassic = testRecord.get(referencefieldname);
        assertEquals("Raw data of " + f.name() + " not equal to " + referencefieldname, oRawNew, oRawClassic);
        switch (datatype.getAvroType()) {
        case AVRODATE:
          assertEquals("AvroDate " + f.name() + " not equal to " + fortwodatestring, oNew,
              LocalDate.parse(fortwodatestring));
          break;
        case AVRODECIMAL:
          assertTrue("AvroDecimal " + f.name() + " not equal to " + forcommatwo.toString(),
              forcommatwo.compareTo((BigDecimal) oNew) == 0);
          break;
        case AVROLOCALTIMESTAMPMICROS:
          assertEquals(
              "AvroTimestampMicros " + f.name() + " not equal to " + fortwodatestring + "T" + fortwotimestring + "Z",
              oNew, LocalDateTime.parse(fortwodatestring + "T" + fortwotimestring));
          break;
        case AVROLOCALTIMESTAMPMILLIS:
          assertEquals(
              "AvroTimestampMillis " + f.name() + " not equal to " + fortwodatestring + "T" + fortwotimestring + "Z",
              oNew, LocalDateTime.parse(fortwodatestring + "T" + fortwotimestring));
          break;
        case AVROTIMEMICROS:
          assertEquals("AvroTimeMicros " + f.name() + " not equal to " + fortwotimestring, oNew,
              LocalTime.parse(fortwotimestring));
          break;
        case AVROTIMEMILLIS:
          assertEquals("AvroTimeMillis " + f.name() + " not equal to " + fortwotimestring, oNew,
              LocalTime.parse(fortwotimestring));
          break;
        case AVROTIMESTAMPMICROS:
          assertEquals("AvroTimestampMicros " + f.name() + " not equal to " + fortwoinstant, oNew, fortwoinstant);
          break;
        case AVROTIMESTAMPMILLIS:
          assertEquals("AvroTimestampMillis " + f.name() + " not equal to " + fortwoinstant, oNew, fortwoinstant);
          break;
        default:
          break;
        }
      }
    }
  }
}
