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
package org.apache.avro.specific;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSpecificDatumReader {

  @Test
  void readMyData() throws IOException {
    // Check that method newInstanceFromString from SpecificDatumReader extension is
    // called.
    final EncoderFactory e_factory = new EncoderFactory().configureBufferSize(30);
    final DecoderFactory factory = new DecoderFactory().configureDecoderBufferSize(30);

    final MyReader reader = new MyReader();
    reader.setExpected(Schema.create(Schema.Type.STRING));
    reader.setSchema(Schema.create(Schema.Type.STRING));

    final ByteArrayOutputStream out = new ByteArrayOutputStream(30);
    final BinaryEncoder encoder = e_factory.binaryEncoder(out, null);
    encoder.writeString(new Utf8("Hello"));
    encoder.flush();

    final BinaryDecoder decoder = factory.binaryDecoder(out.toByteArray(), null);
    reader.getData().setFastReaderEnabled(false);
    final MyData read = reader.read(null, decoder);
    Assertions.assertNotNull(read, "MyReader.newInstanceFromString was not called");
    Assertions.assertEquals("Hello", read.getContent());
  }

  public static class MyData {
    private final String content;

    public MyData(String content) {
      this.content = content;
    }

    public String getContent() {
      return content;
    }
  }

  public static class MyReader extends SpecificDatumReader<MyData> {

    @Override
    protected Class findStringClass(Schema schema) {
      return MyData.class;
    }

    @Override
    protected Object newInstanceFromString(Class c, String s) {
      return new MyData(s);
    }
  }
}
