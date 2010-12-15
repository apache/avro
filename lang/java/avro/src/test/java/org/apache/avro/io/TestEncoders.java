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
import org.junit.Test;

public class TestEncoders {
  @Test
  public void testBinaryEncoderInit() throws IOException {
    OutputStream out = null;
    new BinaryEncoder(out).init(new ByteArrayOutputStream());
  }

  @Test
  public void testBlockingBinaryEncoderInit() throws IOException {
    OutputStream out = null;
    new BlockingBinaryEncoder(out).init(new ByteArrayOutputStream());
  }

  @Test
  public void testJsonEncoderInit() throws IOException {
    Schema s = Schema.parse("\"int\"");
    OutputStream out = null;
    new JsonEncoder(s, out).init(new ByteArrayOutputStream());
  }

  @Test
  public void testValidatingEncoderInit() throws IOException {
    Schema s = Schema.parse("\"int\"");
    OutputStream out = null;
    Encoder e = new BinaryEncoder(out);
    new ValidatingEncoder(s, e).init(new ByteArrayOutputStream());
  }

}
