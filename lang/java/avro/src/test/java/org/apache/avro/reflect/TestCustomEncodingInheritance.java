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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.junit.Test;

public class TestCustomEncodingInheritance {

  @Test
  public void testSuperclassBeatsInterface() throws IOException {
    ParentAndInterface in = new ParentAndInterface("hello world");
    byte[] encoded = write(in);
    ParentAndInterface decoded = read(encoded);

    assertNotNull(decoded);
    assertEquals("hello world", decoded.getField());
  }

  @Test
  public void testFindsInterface() throws IOException {
    JustInterface in = new JustInterface("hello world");
    byte[] encoded = write(in);
    JustInterface decoded = read(encoded);

    assertNotNull(decoded);
    assertEquals("hello world", decoded.getField());
  }

  private <T> T read(byte[] toDecode) throws IOException {
    DatumReader<T> datumReader = new ReflectDatumReader<>();
    try (DataFileStream<T> dataFileReader = new DataFileStream<>(new ByteArrayInputStream(toDecode, 0, toDecode.length),
        datumReader);) {
      dataFileReader.hasNext();
      return dataFileReader.next();
    }
  }

  private <T> byte[] write(T custom) {
    Schema schema = ReflectData.get().getSchema(custom.getClass());
    ReflectDatumWriter<T> datumWriter = new ReflectDatumWriter<>();
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataFileWriter<T> writer = new DataFileWriter<>(datumWriter)) {
      writer.create(schema, baos);
      writer.append(custom);
      writer.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @AvroEncode(using = CustomEncoderInterface.class)
  public static interface Interface {

  }

  @AvroEncode(using = CustomEncoderParent.class)
  public static abstract class Parent {

  }

  public static class ParentAndInterface extends Parent implements Interface {

    private final String field;

    public ParentAndInterface(String field) {
      this.field = field;
    }

    public String getField() {
      return field;
    }

  }

  public static class JustInterface implements Interface {

    private final String field;

    public JustInterface(String field) {
      this.field = field;
    }

    public String getField() {
      return field;
    }

  }

  public static class CustomEncoderParent extends CustomEncoding<Parent> {

    {
      schema = Schema.createRecord("Parent", null, "org.apache.avro.reflect.TestCustomEncodingInheritance", false,
          Arrays.asList(new Schema.Field("field", Schema.create(Schema.Type.STRING), null, null)));
    }

    @Override
    protected void write(Object datum, Encoder out) throws IOException {
      ParentAndInterface c = (ParentAndInterface) datum;
      out.writeString(c.getField());

    }

    @Override
    protected Parent read(Object reuse, Decoder in) throws IOException {
      return new ParentAndInterface(in.readString());
    }
  }

  public static class CustomEncoderInterface extends CustomEncoding<Interface> {

    {
      schema = Schema.createRecord("Interface", null, "org.apache.avro.reflect.TestCustomEncodingInheritance", false,
          Arrays.asList(new Schema.Field("field", Schema.create(Schema.Type.STRING), null, null)));
    }

    @Override
    protected void write(Object datum, Encoder out) throws IOException {
      JustInterface c = (JustInterface) datum;
      out.writeString(c.getField());

    }

    @Override
    protected Interface read(Object reuse, Decoder in) throws IOException {
      return new JustInterface(in.readString());
    }
  }

}
