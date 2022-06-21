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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;

public class TestReflectCustomEncoderFieldAccess {

  @Test
  public void testReadSchemaIsSet() throws IOException {
    Custom in = new Custom("hello", "world");
    byte[] encoded = write(in);

    CustomEncoder.SCHEMA = CustomEncoder.v2Schema;
    Custom decoded = read(encoded);
    assertNotNull(decoded);
    assertEquals("hello", decoded.v1Field);
    assertNull(decoded.v2Field);

    ReflectData.get().removeFromSchemaClassCache(Wrapper.class);
    encoded = write(in);

    decoded = read(encoded);
    assertNotNull(decoded);
    assertEquals("hello", decoded.v1Field);
    assertEquals("world", decoded.v2Field);
  }

  private Custom read(byte[] toDecode) throws IOException {
    DatumReader<Wrapper> datumReader = new ReflectDatumReader<>();
    DataFileStream<Wrapper> dataFileReader = new DataFileStream<>(
        new ByteArrayInputStream(toDecode, 0, toDecode.length), datumReader);
    Wrapper wrapper = null;
    if (dataFileReader.hasNext()) {
      wrapper = dataFileReader.next();
    }
    return wrapper.custom;
  }

  private byte[] write(Custom custom) {
    Schema schema = ReflectData.get().getSchema(Wrapper.class);
    ReflectDatumWriter<Wrapper> datumWriter = new ReflectDatumWriter<>();
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataFileWriter<Wrapper> writer = new DataFileWriter<>(datumWriter)) {
      writer.setCodec(CodecFactory.snappyCodec());
      writer.create(schema, baos);
      writer.append(new Wrapper(custom));
      writer.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static class Custom {
    String v1Field;
    String v2Field;

    Custom(String v1Field, String v2Field) {
      this.v1Field = v1Field;
      this.v2Field = v2Field;
    }
  }

  public static class Wrapper {
    @AvroEncode(using = CustomEncoder.class)
    Custom custom;

    Wrapper() {
    }

    Wrapper(Custom custom) {
      this.custom = custom;
    }
  }

  public static class CustomEncoder extends CustomEncoding<Custom> {

    private boolean hasV2Field = true;

    protected static Schema v1Schema = Schema.createRecord("Custom", null, null, false,
        Collections.singletonList(new Schema.Field("v1Field", Schema.create(Schema.Type.STRING), null, null)));

    protected static Schema v2Schema = Schema.createRecord("Custom", null, null, false,
        Arrays.asList(new Schema.Field("v1Field", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("v2Field", Schema.create(Schema.Type.STRING), null, null)));

    protected static Schema SCHEMA = v1Schema;

    {
      schema = SCHEMA;
    }

    @Override
    protected void write(Object datum, Encoder out) throws IOException {
      Custom custom = (Custom) datum;
      if (SCHEMA == v1Schema) {
        out.writeString(custom.v1Field);
      } else {
        out.writeString(custom.v1Field);
        out.writeString(custom.v2Field);
      }
    }

    @Override
    protected Custom read(Object reuse, Decoder in) throws IOException {
      final String v1Field = in.readString();
      final String v2Field;
      if (hasV2Field) {
        v2Field = in.readString();
      } else {
        v2Field = null;
      }
      return new Custom(v1Field, v2Field);
    }

    @Override
    public CustomEncoding<Custom> setSchema(Schema schema) {
      CustomEncoder encoder = new CustomEncoder();
      encoder.hasV2Field = schema.getField("v2Field") != null;
      return encoder;
    }
  }

}
