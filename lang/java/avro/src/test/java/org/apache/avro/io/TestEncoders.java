/**
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
package org.apache.avro.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.junit.Assert;
import org.junit.Test;

public class TestEncoders {
  private static EncoderFactory factory = EncoderFactory.get();

  @Test
  public void testBinaryEncoderInit() throws IOException {
    OutputStream out = new ByteArrayOutputStream();
    BinaryEncoder enc = factory.binaryEncoder(out, null);
    Assert.assertTrue(enc == factory.binaryEncoder(out, enc));
  }
  
  @Test(expected=NullPointerException.class)
  public void testBadBinaryEncoderInit() {
    factory.binaryEncoder(null, null);
  }

  @Test
  public void testBlockingBinaryEncoderInit() throws IOException {
    OutputStream out = new ByteArrayOutputStream();
    BinaryEncoder reuse = null;
    reuse = factory.blockingBinaryEncoder(out, reuse);
    Assert.assertTrue(reuse == factory.blockingBinaryEncoder(out, reuse));
    // comparison 
  }
  
  @Test(expected=NullPointerException.class)
  public void testBadBlockintBinaryEncoderInit() {
    factory.binaryEncoder(null, null);
  }
  
  @Test
  public void testDirectBinaryEncoderInit() throws IOException {
    OutputStream out = new ByteArrayOutputStream();
    BinaryEncoder enc = factory.directBinaryEncoder(out, null);
    Assert.assertTrue(enc ==  factory.directBinaryEncoder(out, enc));
  }
  
  @Test(expected=NullPointerException.class)
  public void testBadDirectBinaryEncoderInit() {
    factory.directBinaryEncoder(null, null);
  }

  @Test
  public void testJsonEncoderInit() throws IOException {
    Schema s = Schema.parse("\"int\"");
    OutputStream out = new ByteArrayOutputStream();
    factory.jsonEncoder(s, out);
    JsonEncoder enc = factory.jsonEncoder(s,
        new JsonFactory().createJsonGenerator(out, JsonEncoding.UTF8));
    enc.configure(out);
  }
  
  @Test(expected=NullPointerException.class)
  public void testBadJsonEncoderInitOS() throws IOException {
    factory.jsonEncoder(Schema.create(Type.INT), (OutputStream)null);
  }
  
  @Test(expected=NullPointerException.class)
  public void testBadJsonEncoderInit() throws IOException {
    factory.jsonEncoder(Schema.create(Type.INT), (JsonGenerator)null);
  }

  @Test
  public void testValidatingEncoderInit() throws IOException {
    Schema s = Schema.parse("\"int\"");
    OutputStream out = new ByteArrayOutputStream();
    Encoder e = factory.directBinaryEncoder(out, null);
    factory.validatingEncoder(s, e).configure(e);
  }

}
