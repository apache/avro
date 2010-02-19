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
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericDatumReader;

/**
 * Performance tests for various low level operations of
 * Avro encoding and decoding.
 */
public class Perf {
  private static final int COUNT = 100000; // needs to be a multiple of 4
  private static final int CYCLES = 500;
  
  public static void main(String[] args) throws IOException {
    List<Test> tests = new ArrayList<Test>();
    ReadInt readIntTest = null;
    for (String a : args) {
      if (a.equals("-i")) {
        readIntTest = new ReadInt();
        tests.add(readIntTest);
      } else if (a.equals("-f")) {
        tests.add(new ReadFloat());
      } else if (a.equals("-d")) {
        tests.add(new ReadDouble());
      } else if (a.equals("-l")) {
        tests.add(new ReadLong());
      } else if (a.equals("-ls")) {
        tests.add(new ReadLongSmall(readIntTest));
      } else if (a.equals("-b")) {
        tests.add(new ReadBoolean());
      } else if (a.equals("-R")) {
        tests.add(new RepeaterTest());
      } else if (a.equals("-N")) {
        tests.add(new NestedRecordTest());
      } else if (a.equals("-S")) {
        tests.add(new ResolverTest());
      } else if (a.equals("-M")) {
        tests.add(new MigrationTest());
      } else if (a.equals("-G")) {
        tests.add(new GenericReaderTest());
      } else if (a.equals("-Gd")) {
        tests.add(new GenericReaderWithDefaultTest());
      } else if (a.equals("-Go")) {
        tests.add(new GenericReaderWithOutOfOrderTest());
      } else if (a.equals("-Gp")) {
        tests.add(new GenericReaderWithPromotionTest());
      } else {
        usage();
        System.exit(1);
      }
    }
    if (tests.isEmpty()) {
      readIntTest = new ReadInt();
      tests.addAll(Arrays.asList(new Test[] {
          readIntTest, 
          new ReadLongSmall(readIntTest), 
          new ReadLong(),
          new ReadFloat(), 
          new ReadDouble(),
          new ReadBoolean(),
          new RepeaterTest(), new NestedRecordTest(),
          new ResolverTest(), new MigrationTest(),
          new GenericReaderTest(), new GenericReaderWithDefaultTest(),
          new GenericReaderWithOutOfOrderTest(),
          new GenericReaderWithPromotionTest()
      }));
    }
    
    for (int k = 0; k < tests.size(); k++) {
      Test t = tests.get(k);
      // get everything to compile once 
      t.read();
    }
    for (int k = 0; k < tests.size(); k++) {
      Test t = tests.get(k);
      // warmup JVM 
      for (int i = 0; i < t.cycles; i++) {
        t.read();
    }
    // test
    long s = 0;
    for (int i = 0; i < t.cycles; i++) {
      long l = t.read();
      // System.out.println("** " + l);
      s += l;
    }
    s /= 1000;
    double entries = (t.cycles * (double) t.count);
    double bytes = t.cycles * (double) t.data.length;
    System.out.println(t.name + ": " + (s / 1000) + " ms, "
        +  (entries / s) + " million entries/sec.  "
        +  (bytes / s) + " million bytes/sec" );
    tests.set(k, null);
    }
  }
  
  private abstract static class Test {

    /**
     * Name of the test.
     */
    public final String name;
    public final int count;
    public final int cycles;
    protected byte[] data;
    protected static DecoderFactory factory = new DecoderFactory();
    
    /**
     * Reads the contents and returns the time taken in nanoseconds.
     * @return  The time taken to complete the operation.
     * @throws IOException
     */
    abstract long read() throws IOException;
    
    public Test(String name, int cycles, int count) {
      this.name = name;
      this.cycles = cycles;
      this.count = count;
    }
    
    protected void generateRepeaterData(Encoder e) throws IOException {
      e.writeArrayStart();
      e.setItemCount(count);
      Random r = newRandom();
      for (int i = 0; i < count; i++) {
        e.writeDouble(r.nextDouble());
        e.writeDouble(r.nextDouble());
        e.writeDouble(r.nextDouble());
        e.writeInt(r.nextInt());
        e.writeInt(r.nextInt());
        e.writeInt(r.nextInt());
  }
      e.writeArrayEnd();
    }
  }
  
  private static abstract class DecoderTest extends Test {
    public final Schema schema;
    public DecoderTest(String name, String json) throws IOException {
      this(name, json, 1);
    }
    public DecoderTest(String name, String json, int factor) throws IOException {
      super(name, CYCLES, COUNT/factor);
      this.schema = Schema.parse(json);
      ByteArrayOutputStream bao = new ByteArrayOutputStream();
      Encoder e = new BinaryEncoder(bao);
      genData(e);
      e.flush();
      data = bao.toByteArray();
    }

    @Override
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
      return factory.createBinaryDecoder(data, null);
    }

    abstract void genData(Encoder e) throws IOException;
    abstract void readInternal(Decoder d) throws IOException;
  }
  
  /**
   * Use a fixed value seed for random number generation
   * to allow for better cross-run comparisons.
   */
  private static final long SEED = 19781210;

  protected static Random newRandom() {
    return new Random(SEED);
  }

  private static class ReadInt extends DecoderTest {
    public ReadInt() throws IOException {
      this("ReadInt", "{ \"type\": \"array\", \"items\": \"int\"} ");
    }

    public ReadInt(String name, String schema) throws IOException {
      super(name, schema);
    }

    @Override void genData(Encoder e) throws IOException {
      e.writeArrayStart();
      e.setItemCount((count/4) * 4); //next lowest multiple of 4  
      Random r = newRandom();
      for (int i = 0; i < count/4; i++) {
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

  // This is the same data as ReadInt, but using readLong.
  private static class ReadLongSmall extends DecoderTest {
    public ReadLongSmall(ReadInt dataFrom) throws IOException {
      super("ReadLongSmall", "{ \"type\": \"array\", \"items\": \"long\"} ");
      data = dataFrom.data;
    }
    @Override void genData(Encoder e) throws IOException {
    }
    @Override
    void readInternal(Decoder d) throws IOException {
       d.readLong();
    }
  }
 
  // this tests reading Longs that are sometimes very large
  private static class ReadLong extends DecoderTest {
    public ReadLong() throws IOException {
      super("ReadLong", "{ \"type\": \"array\", \"items\": \"long\"} ");
    }
    
    @Override
    void genData(Encoder e) throws IOException {
      e.writeArrayStart();
      e.setItemCount((count / 4) *4);
      Random r = newRandom();
      for (int i = 0; i < count /4; i++) {
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
  
  private static class ReadFloat extends DecoderTest {
    public ReadFloat() throws IOException {
      super("ReadFloat", "{ \"type\": \"array\", \"items\": \"float\"} ");
    }

    @Override
    void genData(Encoder e) throws IOException {
      e.writeArrayStart();
      e.setItemCount(count);
      Random r = newRandom();
      for (int i = 0; i < count; i++) {
        e.writeFloat(r.nextFloat());
      }
      e.writeArrayEnd();
    }

    @Override
    void readInternal(Decoder d) throws IOException {
      d.readFloat();
    }
  }

  private static class ReadBoolean extends DecoderTest {
    public ReadBoolean() throws IOException {
      super("ReadBoolean", "{ \"type\": \"array\", \"items\": \"boolean\"} ");
    }
    @Override void genData(Encoder e) throws IOException {
       e.writeArrayStart();
      e.setItemCount(count);
      Random r = newRandom();
      for (int i = 0; i < count; i++) {
        e.writeBoolean(r.nextBoolean());
      }
      e.writeArrayEnd();
    }
    @Override
    void readInternal(Decoder d) throws IOException {
      d.readBoolean();
    }
  }

  private static class ReadDouble extends DecoderTest {
    public ReadDouble() throws IOException {
      super("ReadDouble", "{ \"type\": \"array\", \"items\": \"double\"} ");
    }
    
    @Override
    void genData(Encoder e) throws IOException {
      e.writeArrayStart();
      e.setItemCount(count);
      Random r = newRandom();
      for (int i = 0; i < count; i++) {
        e.writeDouble(r.nextFloat());
      }
      e.writeArrayEnd();
    }
    @Override
    void readInternal(Decoder d) throws IOException {
      d.readDouble();
    }
  }
  
  private static final String REPEATER_SCHEMA =
    "{ \"type\": \"array\", \"items\":\n"
    + "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
    + "{ \"name\": \"f1\", \"type\": \"double\" },\n"
    + "{ \"name\": \"f2\", \"type\": \"double\" },\n"
    + "{ \"name\": \"f3\", \"type\": \"double\" },\n"
    + "{ \"name\": \"f4\", \"type\": \"int\" },\n"
    + "{ \"name\": \"f5\", \"type\": \"int\" },\n"
    + "{ \"name\": \"f6\", \"type\": \"int\" }\n"
    + "] } }";

  private static class RepeaterTest extends DecoderTest {
    public RepeaterTest() throws IOException {
      this("RepeaterTest");
    }
    
    public RepeaterTest(String name) throws IOException {
      super(name, REPEATER_SCHEMA, 6);
    }
    
    @Override
    protected void genData(Encoder e) throws IOException {
      generateRepeaterData(e);
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

  private static final String MIGRATION_SCHEMA_WITH_DEFAULT =
    "{ \"type\": \"array\", \"items\":\n"
    + "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
    + "{ \"name\": \"f1\", \"type\": \"double\" },\n"
    + "{ \"name\": \"f2\", \"type\": \"double\" },\n"
    + "{ \"name\": \"f3\", \"type\": \"double\" },\n"
    + "{ \"name\": \"f4\", \"type\": \"int\" },\n"
    + "{ \"name\": \"f5\", \"type\": \"int\" },\n"
    + "{ \"name\": \"f6\", \"type\": \"int\" },\n"
    + "{ \"name\": \"f7\", \"type\": \"string\", "
      + "\"default\": \"undefined\" },\n"
    + "{ \"name\": \"f8\", \"type\": \"string\","
      + "\"default\": \"undefined\" }\n"
    + "] } }";

  private static final String MIGRATION_SCHEMA_WITH_OUT_OF_ORDER =
    "{ \"type\": \"array\", \"items\":\n"
    + "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
    + "{ \"name\": \"f1\", \"type\": \"double\" },\n"
    + "{ \"name\": \"f3\", \"type\": \"double\" },\n"
    + "{ \"name\": \"f5\", \"type\": \"int\" },\n"
    + "{ \"name\": \"f2\", \"type\": \"double\" },\n"
    + "{ \"name\": \"f4\", \"type\": \"int\" },\n"
    + "{ \"name\": \"f6\", \"type\": \"int\" }\n"
    + "] } }";

  private static final String MIGRATION_SCHEMA_WITH_PROMOTION =
    "{ \"type\": \"array\", \"items\":\n"
    + "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
    + "{ \"name\": \"f1\", \"type\": \"double\" },\n"
    + "{ \"name\": \"f2\", \"type\": \"double\" },\n"
    + "{ \"name\": \"f3\", \"type\": \"double\" },\n"
    + "{ \"name\": \"f4\", \"type\": \"long\" },\n"
    + "{ \"name\": \"f5\", \"type\": \"long\" },\n"
    + "{ \"name\": \"f6\", \"type\": \"long\" }\n"
    + "] } }";


  /**
   * Tests the performance of introducing default values.
   */
  private static class MigrationTest extends RepeaterTest {
    private final Schema readerSchema;
    public MigrationTest() throws IOException {
      super("MigrationTest");
      readerSchema = Schema.parse(MIGRATION_SCHEMA_WITH_DEFAULT);
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
        } else if (f.pos() < 6) {
          r.readInt();
        } else {
          r.readString(null);
        }
      }
    }
  }
  
  private static class GenericReaderTest extends Test {
    public final Schema writerSchema;

    public GenericReaderTest() throws IOException {
      this("GenericReaderTest");
    }

    public GenericReaderTest(String name) throws IOException {
      super(name, CYCLES, COUNT/12);
      this.writerSchema = Schema.parse(REPEATER_SCHEMA);
      ByteArrayOutputStream bao = new ByteArrayOutputStream();
      Encoder e = new BinaryEncoder(bao);
      generateRepeaterData(e);
      data = bao.toByteArray();
    }

    @Override
    public final long read() throws IOException {
      GenericDatumReader<Object> r = getReader();
      long t = System.nanoTime();
      Decoder d =
        DecoderFactory.defaultFactory().createBinaryDecoder(data, null);
      Object reuse = null;
      for (; ;) {
        try {
          reuse = r.read(reuse, d);
        } catch (EOFException e) {
          break;
        }
      }
      
      return (System.nanoTime() - t);
    }
    
    protected GenericDatumReader<Object> getReader() throws IOException {
      return new GenericDatumReader<Object>(writerSchema);
    }
  }

  private static class GenericReaderWithMigrationTest extends GenericReaderTest {
    private final Schema readerSchema;
    protected GenericReaderWithMigrationTest(String name, String readerSchema)
      throws IOException {
      super(name);
      this.readerSchema = Schema.parse(readerSchema);
    }
    
    protected GenericDatumReader<Object> getReader() throws IOException {
      return new GenericDatumReader<Object>(writerSchema, readerSchema);
    }
  }

  private static class GenericReaderWithDefaultTest extends
    GenericReaderWithMigrationTest {
    public GenericReaderWithDefaultTest() throws IOException {
      super("GenericReaderTestWithDefaultTest", MIGRATION_SCHEMA_WITH_DEFAULT);
    }
  }

  private static class GenericReaderWithOutOfOrderTest extends
    GenericReaderWithMigrationTest {
    public GenericReaderWithOutOfOrderTest() throws IOException {
      super("GenericReaderTestWithOutOfOrderTest",
          MIGRATION_SCHEMA_WITH_OUT_OF_ORDER);
    }
  }

  private static class GenericReaderWithPromotionTest extends
    GenericReaderWithMigrationTest {
    public GenericReaderWithPromotionTest() throws IOException {
      super("GenericReaderTestWithPromotionTest",
          MIGRATION_SCHEMA_WITH_PROMOTION);
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
    System.out.println("Usage: Perf { -i | -ls | -l | -f | -d | -b | -R | -N " +
      "| -S | -M | -G | -Gd | -Go | Gp }");
    System.out.println("  -i readInt()");
    System.out.println("  -ls readLongSmall()");
    System.out.println("  -l readLong()");
    System.out.println("  -f readFloat()");
    System.out.println("  -d readDouble()");
    System.out.println("  -b readBoolean()");
    System.out.println("  -R repeater in validating decoder");
    System.out.println("  -N nested record in validating decoder");
    System.out.println("  -S resolving decoder");
    System.out.println("  -M resolving decoder (with default fields)");
    System.out.println("  -G GenericDatumReader");
    System.out.println("  -Gd GenericDatumReader (with default fields)");
    System.out.println("  -Go GenericDatumReader (with out-of-order fields)");
    System.out.println("  -Gp GenericDatumReader (with promotion fields)");
  }
}
