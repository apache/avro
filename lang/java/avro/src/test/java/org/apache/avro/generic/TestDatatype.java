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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.logicaltypes.AvroDatatype;
import org.junit.Test;

public class TestDatatype {
  String fortyTwo = "42";
  Integer fortyTwoInt = Integer.valueOf(42);
  Long fortyTwoLong = Long.valueOf(42);
  Byte fortyTwoByte = Byte.valueOf((byte) 42);
  byte[] fortyTwoBytes1 = { Byte.valueOf((byte) 42) };
  Float fortyTwoFloat = Float.valueOf((float) 42.0);
  Double fortyTwoDouble = Double.valueOf(42.0);
  Map<Field, Object> fields = new HashMap<>();
  byte[] fortyTwoByteArray = { 0x04, 0x02 };
  List<String> enumValues = new ArrayList<>();
  Schema arraySchema = Schema.createArray(Schema.create(Type.STRING));
  GenericArray<String> array = new GenericData.Array<>(1, arraySchema);
  Map<String, String> map = new HashMap<>();
  Schema recordSchema = SchemaBuilder.record("aRecord").fields().requiredString("myString").endRecord();
  GenericRecord subRecord = new GenericData.Record(recordSchema);

  @Test
  public void testDatatypeForPrimitives() throws IOException {
    fields.put(new Field("string1", Schema.create(Type.STRING), null, null), fortyTwo);
    fields.put(new Field("string2", Schema.create(Type.STRING), null, null), fortyTwoInt);

    fields.put(new Field("bytes1", Schema.create(Type.BYTES), null, null), ByteBuffer.wrap(fortyTwoByteArray));
    fields.put(new Field("bytes2", Schema.create(Type.BYTES), null, null), fortyTwoByte);

    fields.put(new Field("int1", Schema.create(Type.INT), null, null), fortyTwoInt);
    fields.put(new Field("int2", Schema.create(Type.INT), null, null), fortyTwo);

    fields.put(new Field("long1", Schema.create(Type.LONG), null, null), fortyTwoLong);
    fields.put(new Field("long2", Schema.create(Type.LONG), null, null), fortyTwo);

    fields.put(new Field("float1", Schema.create(Type.FLOAT), null, null), fortyTwoFloat);
    fields.put(new Field("float2", Schema.create(Type.FLOAT), null, null), fortyTwoInt);

    fields.put(new Field("double1", Schema.create(Type.DOUBLE), null, null), fortyTwoDouble);
    fields.put(new Field("double2", Schema.create(Type.DOUBLE), null, null), fortyTwoInt);

    fields.put(new Field("boolean1", Schema.create(Type.BOOLEAN), null, null), true);
    fields.put(new Field("boolean2", Schema.create(Type.BOOLEAN), null, null), "True");

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

    validateRecord(testRecord);

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

    validateRecord(testRecord2);

  }

  private void validateRecord(GenericRecord testRecord) {
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

}
