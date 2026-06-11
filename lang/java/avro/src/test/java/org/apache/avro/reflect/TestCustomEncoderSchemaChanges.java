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
package org.apache.avro.reflect;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

public class TestCustomEncoderSchemaChanges {

  private static final Schema SCHEMA1 = Schema.createRecord("R1", null,
      "org.apache.avro.reflect.TestCustomEncoderSchemaChanges", false,
      Arrays.asList(new Schema.Field("v1", Schema.create(Schema.Type.STRING), null, null)));

  private static final Schema SCHEMA2 = Schema.createRecord("R1", null,
      "org.apache.avro.reflect.TestCustomEncoderSchemaChanges", false,
      Arrays.asList(new Schema.Field("v1", Schema.create(Schema.Type.STRING), null, null),
          new Schema.Field("v2", Schema.create(Schema.Type.STRING), null, null)));

  private static final Schema SCHEMA3 = Schema.createRecord("R1", null,
      "org.apache.avro.reflect.TestCustomEncoderSchemaChanges", false,
      Arrays.asList(new Schema.Field("v1", Schema.create(Schema.Type.STRING), null, null),
          new Schema.Field("v2", Schema.create(Schema.Type.STRING), null, null),
          new Schema.Field("v3", Schema.create(Schema.Type.STRING), null, null)));

  private static final Schema SCHEMA_INT1 = Schema.createRecord("R1", null,
      "org.apache.avro.reflect.TestCustomEncoderSchemaChanges", false,
      Arrays.asList(new Schema.Field("v1", Schema.create(Schema.Type.INT), null, null)));

  private static final Schema SCHEMA_INT2 = Schema.createRecord("R1", null,
      "org.apache.avro.reflect.TestCustomEncoderSchemaChanges", false,
      Arrays.asList(new Schema.Field("v1", Schema.create(Schema.Type.INT), null, null),
          new Schema.Field("v2", Schema.create(Schema.Type.INT), null, null)));

  private static final Schema SCHEMA_INT3 = Schema.createRecord("R1", null,
      "org.apache.avro.reflect.TestCustomEncoderSchemaChanges", false,
      Arrays.asList(new Schema.Field("v1", Schema.create(Schema.Type.INT), null, null),
          new Schema.Field("v2", Schema.create(Schema.Type.INT), null, null),
          new Schema.Field("v3", Schema.create(Schema.Type.INT), null, null)));

  EncoderFactory factory = new EncoderFactory();

  @Test
  void testReadTopLevelSchema1() throws IOException {
    // manually write with schema 1
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    var encoder = factory.directBinaryEncoder(out, null);
    encoder.writeString("test");
    ReflectDatumReader<R1> reader = new ReflectDatumReader<>(SCHEMA1);
    var v1 = reader.read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
    assertEquals("test", v1.getValue());
    assertEquals(null, v1.getName());
    assertEquals(null, v1.getName2());
  }

  @Test
  void testReadTopLevelSchema2() throws IOException {
    // manually write with schema 2
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    var encoder = factory.directBinaryEncoder(out, null);
    encoder.writeString("test");
    encoder.writeString("name");

    ReflectDatumReader<R1> reader = new ReflectDatumReader<>(SCHEMA2);
    var v1 = reader.read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
    assertEquals("test", v1.getValue());
    assertEquals("name", v1.getName());
    assertEquals(null, v1.getName2());
  }

  @Test
  void testWriteTopLevelSchema() throws IOException {
    // test write uses correct schema if not specified

    var schema = new ReflectData().getSchema(R1.class);
    ReflectDatumWriter<R1> writer = new ReflectDatumWriter<>(schema);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.write(new R1("1", "2", "3"), factory.directBinaryEncoder(out, null));
    ReflectDatumReader<R1> reader = new ReflectDatumReader<>(schema);
    var returned = reader.read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));

    assertEquals("1", returned.getValue());
    assertEquals("2", returned.getName());
    assertEquals("3", returned.getName2());
  }

  @Test
  void testReadInnerSchema1() throws IOException {
    // manually write with schema 1
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    var encoder = factory.directBinaryEncoder(out, null);
    encoder.writeInt(1);
    ReflectDatumReader<R1Wrapper> reader = new ReflectDatumReader<>(wrapSchema(SCHEMA_INT1));
    var v1 = reader.read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
    assertEquals("1", v1.r1.getValue());
    assertEquals(null, v1.r1.getName());
    assertEquals(null, v1.r1.getName2());
  }

  @Test
  void testReadInnerSchema2() throws IOException {
    // manually write with schema 2
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    var encoder = factory.directBinaryEncoder(out, null);
    encoder.writeInt(1);
    encoder.writeInt(2);
    ReflectDatumReader<R1Wrapper> reader = new ReflectDatumReader<>(wrapSchema(SCHEMA_INT2));
    var v1 = reader.read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
    assertEquals("1", v1.r1.getValue());
    assertEquals("2", v1.r1.getName());
    assertEquals(null, v1.r1.getName2());
  }

  @Test
  void testWriteInnerSchema() throws IOException {
    // test write uses correct schema if not specified

    var schema = new ReflectData().getSchema(R1Wrapper.class);
    ReflectDatumWriter<R1Wrapper> writer = new ReflectDatumWriter<>(schema);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    var wrapper = new R1Wrapper();
    wrapper.r1 = new R1("1", "2", "3");

    writer.write(wrapper, factory.directBinaryEncoder(out, null));
    ReflectDatumReader<R1Wrapper> reader = new ReflectDatumReader<>(schema);
    var returned = reader.read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));

    assertEquals("1", returned.r1.getValue());
    assertEquals("2", returned.r1.getName());
    assertEquals("3", returned.r1.getName2());
  }

  public static Schema wrapSchema(Schema schema) {
    return Schema.createRecord("R1Wrapper", null, "org.apache.avro.reflect.TestCustomEncoderSchemaChanges", false,
        Arrays.asList(new Schema.Field("r1", schema, null, null)));
  }

  public static class R1Wrapper {
    @AvroEncode(using = R1EncoderDouble.class)
    private R1 r1;

  }

  @AvroEncode(using = R1Encoder.class)
  public static class R1 {

    private final String value;
    private final String name;
    private final String name2;

    public R1(String value, String name, String name2) {
      this.value = value;
      this.name = name;
      this.name2 = name2;
    }

    public String getValue() {
      return value;
    }

    public String getName() {
      return name;
    }

    public String getName2() {
      return name2;
    }

  }

  static class R1Encoder extends CustomEncoding<R1> {

    {
      schema = SCHEMA3;
    }

    @Override
    protected void write(Object datum, Encoder out) throws IOException {
      if (!(datum instanceof R1)) {
        throw new AvroTypeException("Expected R1, but got: " + datum.getClass());
      }
      R1 r1 = (R1) datum;
      switch (schema.getFields().size()) {
      case 1:
        out.writeString(r1.getValue());
        break;
      case 2:
        out.writeString(r1.getValue());
        out.writeString(r1.getName());
        break;
      case 3:
        out.writeString(r1.getValue());
        out.writeString(r1.getName());
        out.writeString(r1.getName2());
        break;
      default:
        throw new RuntimeException("Unexpected number of fields: " + schema.getFields().size());
      }

    }

    @Override
    protected R1 read(Object reuse, Decoder in) throws IOException {
      switch (schema.getFields().size()) {
      case 1:
        return new R1(in.readString(), null, null);
      case 2:
        return new R1(in.readString(), in.readString(), null);
      case 3:
        return new R1(in.readString(), in.readString(), in.readString());

      }
      throw new RuntimeException("Unexpected number of fields: " + schema.getFields().size());
    }

    @Override
    public CustomEncoding<R1> withSchema(Schema schema) {
      var encoder = new R1Encoder();
      encoder.schema = schema;
      return encoder;
    }

  }

  static class R1EncoderDouble extends CustomEncoding<R1> {

    {
      schema = SCHEMA_INT3;
    }

    @Override
    protected void write(Object datum, Encoder out) throws IOException {
      if (!(datum instanceof R1)) {
        throw new AvroTypeException("Expected R1, but got: " + datum.getClass());
      }
      R1 r1 = (R1) datum;
      switch (schema.getFields().size()) {
      case 1:
        out.writeInt(Integer.parseInt(r1.getValue()));
        break;
      case 2:
        out.writeInt(Integer.parseInt(r1.getValue()));
        out.writeInt(Integer.parseInt(r1.getName()));
        break;
      case 3:
        out.writeInt(Integer.parseInt(r1.getValue()));
        out.writeInt(Integer.parseInt(r1.getName()));
        out.writeInt(Integer.parseInt(r1.getName2()));
        break;
      default:
        throw new RuntimeException("Unexpected number of fields: " + schema.getFields().size());
      }

    }

    @Override
    protected R1 read(Object reuse, Decoder in) throws IOException {
      switch (schema.getFields().size()) {
      case 1:
        return new R1(Integer.toString(in.readInt()), null, null);
      case 2:
        return new R1(Integer.toString(in.readInt()), Integer.toString(in.readInt()), null);
      case 3:
        return new R1(Integer.toString(in.readInt()), Integer.toString(in.readInt()), Integer.toString(in.readInt()));

      }
      throw new RuntimeException("Unexpected number of fields: " + schema.getFields().size());
    }

    @Override
    public CustomEncoding<R1> withSchema(Schema schema) {
      var encoder = new R1EncoderDouble();
      encoder.schema = schema;
      return encoder;
    }

  }

}
