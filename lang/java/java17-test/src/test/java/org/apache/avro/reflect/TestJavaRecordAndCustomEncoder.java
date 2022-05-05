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

public class TestJavaRecordAndCustomEncoder {

  @Test
  public void testRead() throws IOException {
    var in = new CustomReadWrapper(new CustomRead("hello world"));
    byte[] encoded = write(in);
    CustomReadWrapper decoded = read(encoded);

    assertNotNull(decoded);
    assertEquals("Fixed", decoded.field().getField());
  }

  @Test
  public void testWrite() throws IOException {
    var in = new CustomWriteWrapper(new CustomWrite("hello world"));
    byte[] encoded = write(in);
    CustomWriteWrapper decoded = read(encoded);

    assertNotNull(decoded);
    assertEquals("Override", decoded.field().getField());
  }

  @Test
  public void testWriteFiedEncoder() throws IOException {
    var in = new CustomWriteWrapperWithEncoder(new CustomWrite("hello world"));
    byte[] encoded = write(in);
    CustomWriteWrapperWithEncoder decoded = read(encoded);

    assertNotNull(decoded);
    assertEquals("Override2", decoded.field().getField());
  }

  @Test
  public void testReadFiedEncoder() throws IOException {
    var in = new CustomReadWrapperWithEncoder(new CustomRead("hello world"));
    byte[] encoded = write(in);
    CustomReadWrapperWithEncoder decoded = read(encoded);

    assertNotNull(decoded);
    assertEquals("Fixed2", decoded.field().getField());
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

  @AvroEncode(using = CustomEncoderWrite.class)
  public static class CustomWrite {

    private final String field;

    public CustomWrite(String field) {
      this.field = field;
    }

    public String getField() {
      return field;
    }
  }

  @AvroEncode(using = CustomEncoderRead.class)
  public static class CustomRead {

    private final String field;

    public CustomRead(String field) {
      this.field = field;
    }

    public String getField() {
      return field;
    }
  }

  public static class Custom {

    private final String field;

    public Custom(String field) {
      this.field = field;
    }

    public String getField() {
      return field;
    }
  }

  public static record CustomReadWrapper(CustomRead field) {
  }

  public static record CustomWriteWrapper(CustomWrite field) {
  }

  public static record CustomReadWrapperWithEncoder(@AvroEncode(using = CustomEncoderRead2.class) CustomRead field) {
  }

  public static record CustomWriteWrapperWithEncoder(@AvroEncode(using = CustomEncoderWrite2.class) CustomWrite field) {
  }

  public static class CustomEncoderRead extends CustomEncoding<CustomRead> {

    {
      schema = Schema.createRecord("CustomRead", null, "org.apache.avro.reflect.TestJavaRecordAndCustomEncoder", false,
          Arrays.asList(new Schema.Field("field", Schema.create(Schema.Type.STRING), null, null)));
    }

    @Override
    protected void write(Object datum, Encoder out) throws IOException {
      CustomRead c = (CustomRead) datum;
      out.writeString(c.getField());

    }

    @Override
    protected CustomRead read(Object reuse, Decoder in) throws IOException {
      in.readString();
      return new CustomRead("Fixed");
    }
  }

  public static class CustomEncoderRead2 extends CustomEncoding<CustomRead> {

    {
      schema = Schema.createRecord("CustomRead", null, "org.apache.avro.reflect.TestJavaRecordAndCustomEncoder", false,
          Arrays.asList(new Schema.Field("field", Schema.create(Schema.Type.STRING), null, null)));
    }

    @Override
    protected void write(Object datum, Encoder out) throws IOException {
      CustomRead c = (CustomRead) datum;
      out.writeString(c.getField());

    }

    @Override
    protected CustomRead read(Object reuse, Decoder in) throws IOException {
      in.readString();
      return new CustomRead("Fixed2");
    }
  }

  public static class CustomEncoderWrite extends CustomEncoding<CustomWrite> {

    {
      schema = Schema.createRecord("CustomWrite", null, "org.apache.avro.reflect.TestJavaRecordAndCustomEncoder", false,
          Arrays.asList(new Schema.Field("field", Schema.create(Schema.Type.STRING), null, null)));
    }

    @Override
    protected void write(Object datum, Encoder out) throws IOException {
      CustomWrite c = (CustomWrite) datum;
      out.writeString("Override");

    }

    @Override
    protected CustomWrite read(Object reuse, Decoder in) throws IOException {
      return new CustomWrite(in.readString());
    }
  }

  public static class CustomEncoderWrite2 extends CustomEncoding<CustomWrite> {

    {
      schema = Schema.createRecord("CustomWrite", null, "org.apache.avro.reflect.TestJavaRecordAndCustomEncoder", false,
          Arrays.asList(new Schema.Field("field", Schema.create(Schema.Type.STRING), null, null)));
    }

    @Override
    protected void write(Object datum, Encoder out) throws IOException {
      CustomWrite c = (CustomWrite) datum;
      out.writeString("Override2");

    }

    @Override
    protected CustomWrite read(Object reuse, Decoder in) throws IOException {
      return new CustomWrite(in.readString());
    }
  }
}
