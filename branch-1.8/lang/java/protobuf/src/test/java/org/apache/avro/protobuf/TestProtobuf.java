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
package org.apache.avro.protobuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import com.google.protobuf.ByteString;

import org.apache.avro.protobuf.Test.Foo;
import org.apache.avro.protobuf.Test.A;
import org.apache.avro.protobuf.Test.M.N;

public class TestProtobuf {
  @Test public void testMessage() throws Exception {

    System.out.println(ProtobufData.get().getSchema(Foo.class).toString(true));
    Foo.Builder builder = Foo.newBuilder();
    builder.setInt32(0);
    builder.setInt64(2);
    builder.setUint32(3);
    builder.setUint64(4);
    builder.setSint32(5);
    builder.setSint64(6);
    builder.setFixed32(7);
    builder.setFixed64(8);
    builder.setSfixed32(9);
    builder.setSfixed64(10);
    builder.setFloat(1.0F);
    builder.setDouble(2.0);
    builder.setBool(true);
    builder.setString("foo");
    builder.setBytes(ByteString.copyFromUtf8("bar"));
    builder.setEnum(A.X);
    builder.addIntArray(27);
    builder.addSyms(A.Y);
    Foo fooInner = builder.build();

    Foo fooInArray = builder.build();
    builder = Foo.newBuilder(fooInArray);
    builder.addFooArray(fooInArray);

    builder = Foo.newBuilder(fooInner);
    builder.setFoo(fooInner);
    Foo foo = builder.build();

    System.out.println(foo);

    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    ProtobufDatumWriter<Foo> w = new ProtobufDatumWriter<Foo>(Foo.class);
    Encoder e = EncoderFactory.get().binaryEncoder(bao, null);
    w.write(foo, e);
    e.flush();
    
    Object o = new ProtobufDatumReader<Foo>(Foo.class).read
      (null,
       DecoderFactory.get().createBinaryDecoder
       (new ByteArrayInputStream(bao.toByteArray()), null));

    assertEquals(foo, o);
  }

  @Test public void testNestedEnum() throws Exception {
    Schema s = ProtobufData.get().getSchema(N.class);
    assertEquals(N.class.getName(), SpecificData.getClassName(s));
  }

}
