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

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

/**
 * Performance tests for various low level operations of
 * Avro encoding and decoding.
 */
public class Perf {
  private static final int COUNT = 200000; // needs to be a multiple of 4
  private static final int CYCLES = 150;
  
  public static void main(String[] args) throws IOException {
    List<Test> tests = new ArrayList<Test>();
    for (String a : args) {
      if (a.equals("-i")) {
        tests.add(new ReadInt());
      } else if (a.equals("-f")) {
        tests.add(new ReadFloat());
      } else if (a.equals("-d")) {
        tests.add(new ReadDouble());
      } else if (a.equals("-l")) {
        tests.add(new ReadLong());
      } else if (a.equals("-R")) {
        tests.add(new RepeaterTest());
      } else if (a.equals("-N")) {
        tests.add(new NestedRecordTest());
      } else if (a.equals("-S")) {
        tests.add(new ResolverTest());
      } else if (a.equals("-M")) {
        tests.add(new MigrationTest());
      } else {
        usage();
        System.exit(1);
      }
    }
    if (tests.isEmpty()) {
      tests.addAll(Arrays.asList(new Test[] {
          new ReadInt(), new ReadLong(),
          new ReadFloat(), new ReadDouble(),
          new RepeaterTest(), new NestedRecordTest(),
      }));
    }
    
    for (Test t : tests) {
      // warmup JVM 
    for (int i = 0; i < CYCLES; i++) {
      t.read();
    }
    // test
    long s = 0;
    for (int i = 0; i < CYCLES; i++) {
      long l = t.read();
      // System.out.println("** " + l);
      s += l;
    }
    s /= 1000;
    System.out.println(t.name + ": " + (s / 1000) + " ms, "
        +  ((CYCLES * (double) COUNT) / s) + " million entries/sec");
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
    
    public final long read() throws IOException {
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
      return newDecoder(data);
    }

    protected static Decoder newDecoder(byte[] data) {
      return new BinaryDecoder(new ByteArrayInputStream(data));
    }

    /**
     * Use a fixed value seed for random number generation
     * to allow for better cross-run comparisons.
     */
    private static final long SEED = 19781210;

    protected static Random newRandom() {
      return new Random(SEED);
    }

    abstract void genData(Encoder e) throws IOException;
    abstract void readInternal(Decoder d) throws IOException;
  }
  
  private static class ReadInt extends Test {
    public ReadInt() throws IOException {
      this("ReadInt", "{ \"type\": \"array\", \"items\": \"int\"} ");
    }

    public ReadInt(String name, String schema) throws IOException {
      super(name, schema);
    }

    @Override void genData(Encoder e) throws IOException {
      e.writeArrayStart();
      e.setItemCount((COUNT/4) * 4); //next lowest multiple of 4  
      Random r = newRandom();
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
      Random r = newRandom();
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
      Random r = newRandom();
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
      Random r = newRandom();
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
  
  private static class RepeaterTest extends Test {
    public RepeaterTest() throws IOException {
      this("RepeaterTest");
    }
    
    public RepeaterTest(String name) throws IOException {
      super(name, "{ \"type\": \"array\", \"items\":\n"
          + "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
          + "{ \"name\": \"f1\", \"type\": \"double\" },\n"
          + "{ \"name\": \"f2\", \"type\": \"double\" },\n"
          + "{ \"name\": \"f3\", \"type\": \"double\" },\n"
          + "{ \"name\": \"f4\", \"type\": \"int\" },\n"
          + "{ \"name\": \"f5\", \"type\": \"int\" },\n"
          + "{ \"name\": \"f6\", \"type\": \"int\" }\n"
          + "] } }");
    }
    
    @Override
    protected void genData(Encoder e) throws IOException {
      e.writeArrayStart();
      e.setItemCount(COUNT);
      Random r = newRandom();
      for (int i = 0; i < COUNT; i++) {
        e.writeDouble(r.nextDouble());
        e.writeDouble(r.nextDouble());
        e.writeDouble(r.nextDouble());
        e.writeInt(r.nextInt());
        e.writeInt(r.nextInt());
        e.writeInt(r.nextInt());
      }
      e.writeArrayEnd();
    }
    
    @Override
    protected void readInternal(Decoder d) throws IOException {
      d.readDouble();
      d.readDouble();
      d.readDouble();
      d.readInt();
      d.readInt();
      d.readInt();
    }
    
    @Override
    protected Decoder getDecoder() throws IOException {
      return new ValidatingDecoder(schema, super.getDecoder());
    }
    
  }
  
  private static class ResolverTest extends RepeaterTest {

    public ResolverTest() throws IOException {
      super("ResolverTest");
    }
    
    @Override
    protected Decoder getDecoder() throws IOException {
      return new ResolvingDecoder(schema, schema, newDecoder(data));
    }
    
  }

  /**
   * Tests the performance of introducing default values.
   */
  private static class MigrationTest extends RepeaterTest {
    private final Schema readerSchema;
    public MigrationTest() throws IOException {
      super("MigrationTest");
      readerSchema = Schema.parse( "{ \"type\": \"array\", \"items\":\n"
          + "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
          + "{ \"name\": \"f1\", \"type\": \"double\" },\n"
          + "{ \"name\": \"f2\", \"type\": \"double\" },\n"
          + "{ \"name\": \"f3\", \"type\": \"double\" },\n"
          + "{ \"name\": \"f3_1\", \"type\": \"string\", "
              + "\"default\": \"undefined\" },\n"
          + "{ \"name\": \"f3_2\", \"type\": \"string\","
              + "\"default\": \"undefined\" },\n"
          + "{ \"name\": \"f4\", \"type\": \"int\" },\n"
          + "{ \"name\": \"f5\", \"type\": \"int\" },\n"
          + "{ \"name\": \"f6\", \"type\": \"int\" }\n"
          + "] } }");
    }
    
    @Override
    protected Decoder getDecoder() throws IOException {
      return new ResolvingDecoder(schema, readerSchema, newDecoder(data));
    }
    
    @Override
    protected void readInternal(Decoder d) throws IOException {
      ResolvingDecoder r = (ResolvingDecoder) d;
      Field[] ff = r.readFieldOrder();
      for (Field f : ff) {
        if (f.pos() < 3) {
          r.readDouble();
        } else if (f.pos() >= 5) {
          r.readInt();
        } else {
          r.readString(null);
        }
      }
    }
  }
  
  private static class NestedRecordTest extends ReadInt {
    public NestedRecordTest() throws IOException {
      super("NestedRecordTest",
        "{ \"type\": \"array\", \"items\": \n"
        + "{ \"type\": \"record\", \"name\": \"r1\", \n"
        + "\"fields\": \n"
        + "[ { \"name\": \"f1\", \"type\": \"int\" } ] } } ");
    }

    @Override
    public Decoder getDecoder() throws IOException {
      return new ValidatingDecoder(schema, super.getDecoder());
    }
  }

  private static void usage() {
    System.out.println("Usage: Perf { -i | -l | -f | -d }");
    System.out.println("    -i readInt() performance");
    System.out.println("    -l readLong() performance");
    System.out.println("    -f readFloat() performance");
    System.out.println("    -d readDouble() performance");
    System.out.println("    -R repeater performance in validating decoder");
    System.out.println("    -N nested record performance in validating decoder");
    System.out.println("    -S resolving decoder performance");
  }
}
