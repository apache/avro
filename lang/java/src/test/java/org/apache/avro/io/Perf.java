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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import junit.extensions.RepeatedTest;

import org.apache.avro.Schema;

/**
 * Performance tests for various low level operations of
 * Avro encoding and decoding.
 */
public class Perf {
  private static final int COUNT = 200000; // needs to be a multiple of 4
  private static final int CYCLES = 150;
  
  
  public static void main(String[] args) throws IOException {
    List<Test> tests = new ArrayList<Test>();
    for (String arg : args) {
      if (arg.equals("-i")) {
        tests.add(new ReadInt());
      } else if (arg.equals("-f")) {
        tests.add(new ReadFloat());
      } else if (arg.equals("-d")) {
        tests.add(new ReadDouble());
      } else if (arg.equals("-l")) {
        tests.add(new ReadLong());
      } else if (arg.equals("-R")) {
        tests.add(new RepeaterTest());
      } else {
        usage();
      }
    }
    if (tests.isEmpty()) {
      tests.addAll(Arrays.asList(new Test[] {
         new ReadInt(), new ReadLong(), new ReadFloat(), new ReadDouble(),
         new RepeaterTest(),
      }));
    }
    
    for (Test t : tests) {
      // warmup JVM
      // System.out.println("Warming up...");
      for (int i = 0; i < CYCLES; i++) {
        t.test();
      }
      // test
      long s = 0;
      for (int i = 0; i < CYCLES; i++) {
        long l = t.test();
        // System.out.println("** " + l);
        s += l;
      }
      s /= 1000;
      System.out.println(t.name + "(" + t.schema + "): " + s/1000 + " ms, "
          +  ((CYCLES * (double)COUNT) / s) + " million numbers decoded/sec" );
    }
  }
  
  private static abstract class Test {
    public final String name;
    public final Schema schema;
    protected byte[] data;
    
    public Test(String name, String json) throws IOException {
      this.name = name;
      this.schema = Schema.parse(json);
      ByteArrayOutputStream bao = new ByteArrayOutputStream();
      Encoder e = new BinaryEncoder(bao);
      genData(e);
      data = bao.toByteArray();
    }
    
    public final long test() throws IOException {
      Decoder d = getDecoder();
      long t = System.nanoTime();
      for (long l = d.readArrayStart(); l > 0; l = d.arrayNext()) {
        for (int j = 0; j < l; j++) {
          readInternal(d);
        }
      }
      return (System.nanoTime() - t);
    }

    protected Decoder getDecoder() throws IOException {
      return new BinaryDecoder(new ByteArrayInputStream(data));
    }

    abstract void genData(Encoder e) throws IOException;
    abstract void readInternal(Decoder d) throws IOException;
  }
  
  private static class ReadInt extends Test {
    public ReadInt() throws IOException {
      this("ReadInt", "{ \"type\": \"array\", \"items\": \"int\"} ");
    }

    public ReadInt(String name, String json) throws IOException {
      super(name, json);
    }
    @Override void genData(Encoder e) throws IOException {
      e.writeArrayStart();
      e.setItemCount((COUNT/4) * 4); //next lowest multiple of 4  
      Random r = new Random();
      for (int i = 0; i < COUNT/4; i++) {
        e.writeInt(r.nextInt(50)); // fits in 1 byte
        e.writeInt(r.nextInt(5000)); // fits in 2 bytes
        e.writeInt(r.nextInt(500000)); // fits in 3 bytes
        e.writeInt(r.nextInt(150000000)); // most in 4, some in 5
      }
      e.writeArrayEnd();
    }

    @Override
    void readInternal(Decoder d) throws IOException {
       d.readInt();
    }
  }

  private static class ReadLong extends Test {
    public ReadLong() throws IOException {
      super("ReadLong", "{ \"type\": \"array\", \"items\": \"long\"} ");
    }
    @Override void genData(Encoder e) throws IOException {
      e.writeArrayStart();
      e.setItemCount((COUNT / 4) *4);
      Random r = new Random();
      for (int i = 0; i < COUNT /4; i++) {
        e.writeLong(r.nextLong() % 0x7FL); // half fit in 1, half in 2 
        e.writeLong(r.nextLong() % 0x1FFFFFL); // half fit in <=3, half in 4
        e.writeLong(r.nextLong() % 0x3FFFFFFFFL); // half in <=5, half in 6
        e.writeLong(r.nextLong() % 0x1FFFFFFFFFFFFL); // half in <=8, half in 9 
      }
      e.writeArrayEnd();
    }

    @Override
    void readInternal(Decoder d) throws IOException {
       d.readLong();
    }
  }
  
  private static class ReadFloat extends Test {
    public ReadFloat() throws IOException {
      super("ReadFloat", "{ \"type\": \"array\", \"items\": \"float\"} ");
    }
    @Override void genData(Encoder e) throws IOException {
       e.writeArrayStart();
      e.setItemCount(COUNT);
      Random r = new Random();
      for (int i = 0; i < COUNT; i++) {
        e.writeFloat(r.nextFloat());
      }
      e.writeArrayEnd();
    }
    @Override
    void readInternal(Decoder d) throws IOException {
      d.readFloat();
    }
  }

  private static class ReadDouble extends Test {
    public ReadDouble() throws IOException {
      super("ReadDouble", "{ \"type\": \"array\", \"items\": \"double\"} ");
    }
    @Override void genData(Encoder e) throws IOException {
       e.writeArrayStart();
      e.setItemCount(COUNT);
      Random r = new Random();
      for (int i = 0; i < COUNT; i++) {
        e.writeDouble(r.nextFloat());
      }
      e.writeArrayEnd();
    }
    @Override
    void readInternal(Decoder d) throws IOException {
      d.readDouble();
    }
  }
  
  private static class RepeaterTest extends ReadInt {
      public RepeaterTest() throws IOException {
        super("RepeaterTest",
            "{ \"type\": \"array\", \"items\": \"int\"} ");
      }

      @Override
      public Decoder getDecoder() throws IOException {
        return new ValidatingDecoder(schema, super.getDecoder());
      }
    }

  private static void usage() {
    System.out.println("Usage: Perf { -i | -l | -f | -d }");
    System.out.println("    -i      measures readInt() performance");
    System.out.println("    -l      measures readLong() performance");
    System.out.println("    -f      measures readFloat() performance");
    System.out.println("    -d      measures readDouble() performance");
    System.out.println("    -R      measures repeater performance in "
        + "validating encoder");
  }
}
