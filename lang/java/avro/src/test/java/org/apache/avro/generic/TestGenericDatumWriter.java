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
package org.apache.avro.generic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.DirectBinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.junit.Test;
import org.apache.avro.util.Utf8;

public class TestGenericDatumWriter {
  @Test
  public void testWrite() throws IOException {
    String json = "{\"type\": \"record\", \"name\": \"r\", \"fields\": ["
      + "{ \"name\": \"f1\", \"type\": \"long\" }"
      + "]}";
    Schema s = Schema.parse(json);
    GenericRecord r = new GenericData.Record(s);
    r.put("f1", 100L);
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    GenericDatumWriter<GenericRecord> w =
      new GenericDatumWriter<GenericRecord>(s);
    Encoder e = EncoderFactory.get().jsonEncoder(s, bao);
    w.write(r, e);
    e.flush();
    
    Object o = new GenericDatumReader<GenericRecord>(s).read(null,
        DecoderFactory.get().jsonDecoder(s, new ByteArrayInputStream(bao.toByteArray())));
    assertEquals(r, o);
  }

  @Test
  public void testArrayConcurrentModification() throws Exception {
    String json = "{\"type\": \"array\", \"items\": \"int\" }";
    Schema s = Schema.parse(json);
    final GenericArray<Integer> a = new GenericData.Array<Integer>(1, s);
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    final GenericDatumWriter<GenericArray<Integer>> w =
      new GenericDatumWriter<GenericArray<Integer>>(s);

    CountDownLatch sizeWrittenSignal = new CountDownLatch(1);
    CountDownLatch eltAddedSignal = new CountDownLatch(1);

    final TestEncoder e = new TestEncoder(EncoderFactory.get()
        .directBinaryEncoder(bao, null), sizeWrittenSignal, eltAddedSignal);
    
    // call write in another thread
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<Void> result = executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        w.write(a, e);
        return null;
      }
    });
    sizeWrittenSignal.await();
    // size has been written so now add an element to the array
    a.add(7);
    // and signal for the element to be written
    eltAddedSignal.countDown();
    try {
      result.get();
      fail("Expected ConcurrentModificationException");
    } catch (ExecutionException ex) {
      assertTrue(ex.getCause() instanceof ConcurrentModificationException);
    }
  }
  

  @Test
  public void testMapConcurrentModification() throws Exception {
    String json = "{\"type\": \"map\", \"values\": \"int\" }";
    Schema s = Schema.parse(json);
    final Map<String, Integer> m = new HashMap<String, Integer>();
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    final GenericDatumWriter<Map<String, Integer>> w =
      new GenericDatumWriter<Map<String, Integer>>(s);

    CountDownLatch sizeWrittenSignal = new CountDownLatch(1);
    CountDownLatch eltAddedSignal = new CountDownLatch(1);

    final TestEncoder e = new TestEncoder(EncoderFactory.get()
        .directBinaryEncoder(bao, null), sizeWrittenSignal, eltAddedSignal);
    
    // call write in another thread
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<Void> result = executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        w.write(m, e);
        return null;
      }
    });
    sizeWrittenSignal.await();
    // size has been written so now add an entry to the map
    m.put("a", 7);
    // and signal for the entry to be written
    eltAddedSignal.countDown();
    try {
      result.get();
      fail("Expected ConcurrentModificationException");
    } catch (ExecutionException ex) {
      assertTrue(ex.getCause() instanceof ConcurrentModificationException);
    }
  }
  
  static class TestEncoder extends Encoder {
    
    Encoder e;
    CountDownLatch sizeWrittenSignal;
    CountDownLatch eltAddedSignal;
    
    TestEncoder(Encoder encoder, CountDownLatch sizeWrittenSignal,
        CountDownLatch eltAddedSignal) {
      this.e = encoder;
      this.sizeWrittenSignal = sizeWrittenSignal;
      this.eltAddedSignal = eltAddedSignal;
    }
    
    @Override
    public void writeArrayStart() throws IOException {
      e.writeArrayStart();
      sizeWrittenSignal.countDown();
      try {
        eltAddedSignal.await();
      } catch (InterruptedException e) {
        // ignore
      }
    }

    @Override
    public void writeMapStart() throws IOException {
      e.writeMapStart();
      sizeWrittenSignal.countDown();
      try {
        eltAddedSignal.await();
      } catch (InterruptedException e) {
        // ignore
      }
    }
    
    @Override
    public void flush() throws IOException { e.flush(); }
    @Override
    public void writeNull() throws IOException { e.writeNull(); }
    @Override
    public void writeBoolean(boolean b) throws IOException { e.writeBoolean(b); }
    @Override
    public void writeInt(int n) throws IOException { e.writeInt(n); }
    @Override
    public void writeLong(long n) throws IOException { e.writeLong(n); }
    @Override
    public void writeFloat(float f) throws IOException { e.writeFloat(f); }
    @Override
    public void writeDouble(double d) throws IOException { e.writeDouble(d); }
    @Override
    public void writeString(Utf8 utf8) throws IOException { e.writeString(utf8); }
    @Override
    public void writeBytes(ByteBuffer bytes) throws IOException { e.writeBytes(bytes); }
    @Override
    public void writeBytes(byte[] bytes, int start, int len) throws IOException { e.writeBytes(bytes, start, len); }
    @Override
    public void writeFixed(byte[] bytes, int start, int len) throws IOException { e.writeFixed(bytes, start, len); }
    @Override
    public void writeEnum(int en) throws IOException { e.writeEnum(en); }
    @Override
    public void setItemCount(long itemCount) throws IOException { e.setItemCount(itemCount); }
    @Override
    public void startItem() throws IOException { e.startItem(); }
    @Override
    public void writeArrayEnd() throws IOException { e.writeArrayEnd(); }
    @Override
    public void writeMapEnd() throws IOException { e.writeMapEnd(); }
    @Override
    public void writeIndex(int unionIndex) throws IOException { e.writeIndex(unionIndex); }
  };
}
