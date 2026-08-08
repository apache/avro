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

public class TestJavaRecords {

  EncoderFactory factory = new EncoderFactory();

  @Test
  void testRecordWithEncoder() throws IOException {

    var wrapper = new Wrapper(new R1("test"));

    var read = readWrite(wrapper);

    assertEquals("test", wrapper.getR1().value());
    assertEquals("test used this", read.getR1().value());
  }

  public static class Wrapper {

    @AvroEncode(using = R1Encoding.class)
    private R1 r1;

    public Wrapper() {
    }

    public Wrapper(R1 r1) {
      this.r1 = r1;
    }

    public R1 getR1() {
      return r1;
    }

    public void setR1(R1 r1) {
      this.r1 = r1;
    }

  }

  public static record R1(String value) {
  }

  public static class R1Encoding extends CustomEncoding<R1> {

    {

      schema = Schema.createRecord("R1", null, null, false,
          Arrays.asList(new Schema.Field("value", Schema.create(Schema.Type.STRING), null, null)));
    }

    @Override
    protected void write(Object datum, Encoder out) throws IOException {
      if (datum instanceof R1 r1) {
        out.writeString(r1.value());

      } else {
        throw new AvroTypeException("Expected R1, got " + datum.getClass());
      }

    }

    @Override
    protected R1 read(Object reuse, Decoder in) throws IOException {
      return new R1(in.readString() + " used this");
    }
  }

  <T> T readWrite(T object) throws IOException {
    var schema = ReflectData.get().getSchema(object.getClass());
    ReflectDatumWriter<T> writer = new ReflectDatumWriter<>(schema);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.write(object, factory.directBinaryEncoder(out, null));
    ReflectDatumReader<T> reader = new ReflectDatumReader<>(schema);
    return reader.read(null, DecoderFactory.get().binaryDecoder(out.toByteArray(), null));
  }

}
