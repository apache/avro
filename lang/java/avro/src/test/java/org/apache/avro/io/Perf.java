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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

/**
 * Performance tests for various low level operations of
 * Avro encoding and decoding.
 */
public class Perf {
  private static final int COUNT = 250000; // needs to be a multiple of 4
  private static final int CYCLES = 800;
  
  /**
   * Use a fixed value seed for random number generation
   * to allow for better cross-run comparisons.
   */
  private static final long SEED = 19781210;

  protected static Random newRandom() {
    return new Random(SEED);
  }
  
  private static class TestDescriptor {
    Class<? extends Test> test;
    String param;
    TestDescriptor(Class<? extends Test> test, String param) {
      this.test = test;
      this.param = param;
    }
    void add(List<TestDescriptor> typeList) {
      ALL_TESTS.put(param, this);
      typeList.add(this);
    }
  }
  
  private static final List<TestDescriptor> BASIC = new ArrayList<TestDescriptor>();
  private static final List<TestDescriptor> RECORD = new ArrayList<TestDescriptor>();
  private static final List<TestDescriptor> GENERIC = new ArrayList<TestDescriptor>();
  private static final List<TestDescriptor> GENERIC_ONETIME = new ArrayList<TestDescriptor>();
  private static final LinkedHashMap<String, TestDescriptor> ALL_TESTS;
  private static final LinkedHashMap<String, List<TestDescriptor>> BATCHES;
  static {
    ALL_TESTS = new LinkedHashMap<String, TestDescriptor>();
    BATCHES = new LinkedHashMap<String, List<TestDescriptor>>();
    BATCHES.put("-basic", BASIC);
    new TestDescriptor(IntTest.class, "-i").add(BASIC);
    new TestDescriptor(SmallLongTest.class, "-ls").add(BASIC);
    new TestDescriptor(LongTest.class, "-l").add(BASIC);
    new TestDescriptor(FloatTest.class, "-f").add(BASIC);
    new TestDescriptor(DoubleTest.class, "-d").add(BASIC);
    new TestDescriptor(BoolTest.class, "-b").add(BASIC);
    new TestDescriptor(BytesTest.class, "-by").add(BASIC);
    new TestDescriptor(StringTest.class, "-s").add(BASIC);
    BATCHES.put("-record", RECORD);
    new TestDescriptor(RecordTest.class, "-R").add(RECORD);
    new TestDescriptor(ValidatingRecord.class, "-Rv").add(RECORD);
    new TestDescriptor(ResolvingRecord.class, "-Rr").add(RECORD);
    new TestDescriptor(RecordWithDefault.class, "-Rd").add(RECORD);
    new TestDescriptor(RecordWithOutOfOrder.class, "-Ro").add(RECORD);
    new TestDescriptor(RecordWithPromotion.class, "-Rp").add(RECORD);
    BATCHES.put("-generic", GENERIC);
    new TestDescriptor(GenericTest.class, "-G").add(GENERIC);
    new TestDescriptor(GenericNested.class, "-Gn").add(GENERIC);
    new TestDescriptor(GenericWithDefault.class, "-Gd").add(GENERIC);
    new TestDescriptor(GenericWithOutOfOrder.class, "-Go").add(GENERIC);
    new TestDescriptor(GenericWithPromotion.class, "-Gp").add(GENERIC);
    BATCHES.put("-generic-onetime", GENERIC_ONETIME);
    new TestDescriptor(GenericOneTimeDecoderUse.class, "-Gotd").add(GENERIC_ONETIME);
    new TestDescriptor(GenericOneTimeReaderUse.class, "-Gotr").add(GENERIC_ONETIME);
    new TestDescriptor(GenericOneTimeUse.class, "-Got").add(GENERIC_ONETIME);
  }
  
  private static void usage() {
    StringBuilder usage = new StringBuilder("Usage: Perf { -nowrite | -noread | ");
    StringBuilder details = new StringBuilder();
    details.append(" -nowrite   (do not execute write tests)\n");
    details.append(" -noread   (do not execute write tests)\n");
    for (Map.Entry<String, List<TestDescriptor>> entry : BATCHES.entrySet()) {
      List<TestDescriptor> lt = entry.getValue();
      String param = entry.getKey();
      String paramName = param.substring(1);
      usage.append(param).append(" | ");
      details.append(" ").append(param).append("   (executes all ").append(paramName).append(" tests):\n");
      for (TestDescriptor t : lt) {
        usage.append(t.param).append(" | ");
        details.append("      ").append(t.param).append("  (").append(t.test.getSimpleName()).append(")\n");
      }
    }
    usage.setLength(usage.length() - 2);
    usage.append("}\n");
    System.out.println(usage.toString());
    System.out.print(details.toString());
  }
  
  public static void main(String[] args) throws Exception {
    List<Test> tests = new ArrayList<Test>();
    boolean writeTests = true;
    boolean readTests = true;
    for (String a : args) {
      TestDescriptor t = ALL_TESTS.get(a);
      if (null != t) {
        tests.add(t.test.newInstance());
        continue;
      }
      List<TestDescriptor> lt = BATCHES.get(a);
      if (null != lt) {
        for(TestDescriptor td : lt) {
          tests.add(td.test.newInstance());
        }
        continue;
      }
      if ("-nowrite".equals(a)) {
        writeTests = false;
        continue;
      }
      if ("-noread".equals(a)) {
        readTests = false;
        continue;
      }
      usage();
      System.exit(1);
    }
    if (tests.isEmpty()) {
      for (Map.Entry<String, TestDescriptor> entry : ALL_TESTS.entrySet()) {
        TestDescriptor t = entry.getValue();
        Test test = t.test.newInstance();
        tests.add(test);
      }
    }
    System.out.println("Executing tests: \n" + tests +  "\n readTests:" +
        readTests + "\n writeTests:" + writeTests + "\n cycles=" + CYCLES);
    
    for (int k = 0; k < tests.size(); k++) {
      Test t = tests.get(k);
      try {
        // get everything to compile once
        t.init();
        if (t.isReadTest() && readTests) {
          t.readTest();
        }
        if (t.isWriteTest() && writeTests) {
          t.writeTest();
        }
        t.reset();
      } catch (Exception e) {
        System.out.println("Failed to execute test: " + t.getClass().getSimpleName());
        throw e;
      }
    }
    
    printHeader();

    for (int k = 0; k < tests.size(); k++) {
      Test t = tests.get(k);
      // warmup JVM
      t.init();
      if (t.isReadTest() && readTests) {
        for (int i = 0; i < t.cycles; i++) {
          t.readTest();
        }
      }
      if (t.isWriteTest() && writeTests) {
        for (int i = 0; i < t.cycles; i++) {
          t.writeTest();
        }
      }
      t.reset();
      // test
      long s = 0;
      System.gc();
      t.init();
      if (t.isReadTest() && readTests) {
        for (int i = 0; i < t.cycles; i++) {
          s += t.readTest();
        }
        printResult(s, t, t.name + "Read");
      }
      s = 0;
      if (t.isWriteTest() && writeTests) {
        for (int i = 0; i < t.cycles; i++) {
          s += t.writeTest();
        }
        printResult(s, t, t.name + "Write");
      }
      t.reset();
    }
  }
  
  private static final void printHeader() {
    String header = String.format(
        "%29s     time    M entries/sec   M bytes/sec  bytes/cycle",
        "test name");
    System.out.println(header.toString());
  }
  
  private static final void printResult(long s, Test t, String name) {
    s /= 1000;
    double entries = (t.cycles * (double) t.count);
    double bytes = t.cycles * (double) t.encodedSize;
    StringBuilder result = new StringBuilder();
    result.append(String.format("%29s: %6d ms  ", name, (s/1000)));
    result.append(String.format("%10.3f   %11.3f   %11d", 
        (entries / s), (bytes/ s),  t.encodedSize));
    System.out.println(result.toString());
  }
  
  private abstract static class Test {

    /**
     * Name of the test.
     */
    public final String name;
    public final int count;
    public final int cycles;
    public long encodedSize = 0;
    protected boolean isReadTest = true;
    protected boolean isWriteTest = true;
    static DecoderFactory factory = new DecoderFactory();
    
    public Test(String name, int cycles, int count) {
      this.name = name;
      this.cycles = cycles;
      this.count = count;
    }

    /**
     * Reads data from a Decoder and returns the time taken in nanoseconds.
     */
    abstract long readTest() throws IOException;
    
    /**
     * Writes data to an Encoder and returns the time taken in nanoseconds.
     */
    abstract long writeTest() throws IOException;
    
    final boolean isWriteTest() {
      return isWriteTest;
    }
    
    final boolean isReadTest() {
      return isReadTest;
    }
 
    /** initializes data for read and write tests **/
    abstract void init() throws IOException;

    /** clears generated data arrays and other large objects created during initialization **/
    abstract void reset();
    
    @Override
    public String toString() {
      return this.getClass().getSimpleName();
    }
       
  }
  
  /** the basic test writes a simple schema directly to an encoder or
   * reads from an array.  It does not use GenericDatumReader or any
   * higher level constructs, just manual serialization.
   */
  private static abstract class BasicTest extends Test {
    protected final Schema schema;
    protected byte[] data;
    BasicTest(String name, String json) throws IOException {
      this(name, json, 1);
    }
    BasicTest(String name, String json, int factor) throws IOException {
      super(name, CYCLES, COUNT/factor);
      this.schema = Schema.parse(json);
    }

    @Override
    public final long readTest() throws IOException {
      long t = System.nanoTime();
      Decoder d = getDecoder();
      readInternal(d);
      return (System.nanoTime() - t);
    }
    
    @Override
    public final long writeTest() throws IOException {
      long t = System.nanoTime();
      Encoder e = getEncoder();
      writeInternal(e);
      e.flush();
      return (System.nanoTime() - t);
    }
    
    protected Decoder getDecoder() throws IOException {
      return newDecoder();
    }
    
    protected Encoder getEncoder() throws IOException {
      return newEncoder();
    }

    protected Decoder newDecoder() {
      return factory.createBinaryDecoder(data, null);
    }
    
    protected Encoder newEncoder() {
      OutputStream out = new ByteArrayOutputStream((int)(encodedSize > 0 ? encodedSize : count));
      return new BinaryEncoder(out);
    }
    
    @Override
    void init() throws IOException {
      genSourceData();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      Encoder e = new BinaryEncoder(baos);
      writeInternal(e);
      e.flush();
      data = baos.toByteArray();
      encodedSize = data.length;
    }

    abstract void genSourceData();
    abstract void readInternal(Decoder d) throws IOException;
    abstract void writeInternal(Encoder e) throws IOException;
  }
    
  static class IntTest extends BasicTest {
    protected int[] sourceData = null;
    public IntTest() throws IOException {
      this("Int", "{ \"type\": \"int\"} ");
    }

    private IntTest(String name, String schema) throws IOException {
      super(name, schema);
    }

    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new int[count];
      for (int i = 0; i < sourceData.length; i+=4) {
        sourceData[i] = r.nextInt(50); // fits in 1 byte
        sourceData[i+1] = r.nextInt(5000); // fits in 2 bytes
        sourceData[i+2] = r.nextInt(500000); // fits in 3 bytes
        sourceData[i+3] = r.nextInt(150000000); // most in 4, some in 5
      }
    }
   
    @Override
    void readInternal(Decoder d) throws IOException {
      for (int i = 0; i < count/4; i++) {
        d.readInt();
        d.readInt();
        d.readInt();
        d.readInt();
      }
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      for (int i = 0; i < sourceData.length; i+=4) {
        e.writeInt(sourceData[i]);
        e.writeInt(sourceData[i+1]);
        e.writeInt(sourceData[i+2]);
        e.writeInt(sourceData[i+3]);
      }
    }
  
    @Override
    void reset() {
      sourceData = null;
      data = null;
    }
  }

  // This is the same data as ReadInt, but using readLong.
  static class SmallLongTest extends IntTest {
    public SmallLongTest() throws IOException {
      super("SmallLong", "{ \"type\": \"long\"} ");
    }

    @Override
    void readInternal(Decoder d) throws IOException {
      for (int i = 0; i < count/4; i++) {
        d.readLong();
        d.readLong();
        d.readLong();
        d.readLong();
      }
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      for (int i = 0; i < sourceData.length; i+=4) {
        e.writeLong(sourceData[i]);
        e.writeLong(sourceData[i+1]);
        e.writeLong(sourceData[i+2]);
        e.writeLong(sourceData[i+3]);
      }
    }
  }
 
  // this tests reading Longs that are sometimes very large
  static class LongTest extends BasicTest {
    private long[] sourceData = null;
    public LongTest() throws IOException {
      super("Long", "{ \"type\": \"long\"} ");
    }
    
    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new long[count];
      for (int i = 0; i < sourceData.length; i+=4) {
        sourceData[i] = r.nextLong() % 0x7FL; // half fit in 1, half in 2 
        sourceData[i+1] = r.nextLong() % 0x1FFFFFL; // half fit in <=3, half in 4
        sourceData[i+2] = r.nextLong() % 0x3FFFFFFFFL; // half in <=5, half in 6
        sourceData[i+3] = r.nextLong() % 0x1FFFFFFFFFFFFL; // half in <=8, half in 9 
      }
    }
   
    @Override
    void readInternal(Decoder d) throws IOException {
      for (int i = 0; i < count/4; i++) {
        d.readLong();
        d.readLong();
        d.readLong();
        d.readLong();
      }
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      for (int i = 0; i < sourceData.length;i+=4) {
        e.writeLong(sourceData[i]);
        e.writeLong(sourceData[i+1]);
        e.writeLong(sourceData[i+2]);
        e.writeLong(sourceData[i+3]);
      }
    }
  
    @Override
    void reset() {
      sourceData = null;
      data = null;
    }
  }
  
  static class FloatTest extends BasicTest {
    float[] sourceData = null;
    public FloatTest() throws IOException {
      super("Float", "{ \"type\": \"float\"} ");
    }

    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new float[count];
      for (int i = 0; i < sourceData.length;) {
        sourceData[i++] = r.nextFloat(); 
      }
    }
   
    @Override
    void readInternal(Decoder d) throws IOException {
      for (int i = 0; i < count/4; i++) {
        d.readFloat();
        d.readFloat();
        d.readFloat();
        d.readFloat();
      }
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      for (int i = 0; i < sourceData.length;i+=4) {
        e.writeFloat(sourceData[i]);
        e.writeFloat(sourceData[i+1]);
        e.writeFloat(sourceData[i+2]);
        e.writeFloat(sourceData[i+3]);
      }
    }
  
    @Override
    void reset() {
      sourceData = null;
      data = null;
    }
  }

  static class DoubleTest extends BasicTest {
    double[] sourceData = null;
    public DoubleTest() throws IOException {
      super("Double", "{ \"type\": \"double\"} ");
    }
    
    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new double[count];
      for (int i = 0; i < sourceData.length;) {
        sourceData[i++] = r.nextDouble(); 
      }
    }
   
    @Override
    void readInternal(Decoder d) throws IOException {
      for (int i = 0; i < count/4; i++) {
        d.readDouble();
        d.readDouble();
        d.readDouble();
        d.readDouble();
      }
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      for (int i = 0; i < sourceData.length;i+=4) {
        e.writeDouble(sourceData[i]);
        e.writeDouble(sourceData[i+1]);
        e.writeDouble(sourceData[i+2]);
        e.writeDouble(sourceData[i+3]);
      }
    }
  
    @Override
    void reset() {
      sourceData = null;
      data = null;
    }
  }
  
  static class BoolTest extends BasicTest {
    boolean[] sourceData = null;
    public BoolTest() throws IOException {
      super("Boolean", "{ \"type\": \"boolean\"} ");
    }
    
    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new boolean[count];
      for (int i = 0; i < sourceData.length;) {
        sourceData[i++] = r.nextBoolean(); 
      }
    }
   
    @Override
    void readInternal(Decoder d) throws IOException {
      for (int i = 0; i < count/4; i++) {
        d.readBoolean();
        d.readBoolean();
        d.readBoolean();
        d.readBoolean();
      }
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      for (int i = 0; i < sourceData.length;i+=4) {
        e.writeBoolean(sourceData[i]);
        e.writeBoolean(sourceData[i+1]);
        e.writeBoolean(sourceData[i+2]);
        e.writeBoolean(sourceData[i+3]);
      }
    }
  
    @Override
    void reset() {
      sourceData = null;
      data = null;
    }
  }
  
  static class BytesTest extends BasicTest {
    byte[][] sourceData = null;
    public BytesTest() throws IOException {
      super("Bytes", "{ \"type\": \"bytes\"} ", 5);
    }
    
    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new byte[count][];
      for (int i = 0; i < sourceData.length;) {
        byte[] data = new byte[r.nextInt(70)];
        r.nextBytes(data);
        sourceData[i++] = data; 
      }
    }
   
    @Override
    void readInternal(Decoder d) throws IOException {
      ByteBuffer bb = ByteBuffer.allocate(70);
      for (int i = 0; i < count/4; i++) {
        d.readBytes(bb);
        d.readBytes(bb);
        d.readBytes(bb);
        d.readBytes(bb);
      }
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      for (int i = 0; i < sourceData.length;i+=4) {
        e.writeBytes(sourceData[i]);
        e.writeBytes(sourceData[i+1]);
        e.writeBytes(sourceData[i+2]);
        e.writeBytes(sourceData[i+3]);
      }
    }
  
    @Override
    void reset() {
      sourceData = null;
      data = null;
    }
  }
  
  static class StringTest extends BasicTest {
    String[] sourceData = null;
    public StringTest() throws IOException {
      super("String", "{ \"type\": \"string\"} ", 5);
    }
    
    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new String[count];
      for (int i = 0; i < sourceData.length;) {
        char[] data = new char[r.nextInt(70)];
        for (int j = 0; j < data.length; j++) {
          data[j] = (char)('a' + r.nextInt('z'-'a'));
        }
        sourceData[i++] = new String(data); 
      }
    }
   
    @Override
    void readInternal(Decoder d) throws IOException {
      Utf8 utf = new Utf8();
      for (int i = 0; i < count/4; i++) {
        d.readString(utf).toString();
        d.readString(utf).toString();
        d.readString(utf).toString();
        d.readString(utf).toString();
      }
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      for (int i = 0; i < sourceData.length;i+=4) {
        e.writeString(sourceData[i]);
        e.writeString(sourceData[i+1]);
        e.writeString(sourceData[i+2]);
        e.writeString(sourceData[i+3]);
      }
    }
  
    @Override
    void reset() {
      sourceData = null;
      data = null;
    }
  }
  
  private static final String RECORD_SCHEMA = 
    "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
    + "{ \"name\": \"f1\", \"type\": \"double\" },\n"
    + "{ \"name\": \"f2\", \"type\": \"double\" },\n"
    + "{ \"name\": \"f3\", \"type\": \"double\" },\n"
    + "{ \"name\": \"f4\", \"type\": \"int\" },\n"
    + "{ \"name\": \"f5\", \"type\": \"int\" },\n"
    + "{ \"name\": \"f6\", \"type\": \"int\" }\n"
    + "] }";
  
  private static final String NESTED_RECORD_SCHEMA =
    "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
    + "{ \"name\": \"f1\", \"type\": \n" +
    		"{ \"type\": \"record\", \"name\": \"D\", \"fields\": [\n" +
    		  "{\"name\": \"dbl\", \"type\": \"double\" }]\n" +
        "} },\n"
    + "{ \"name\": \"f2\", \"type\": \"D\" },\n"
    + "{ \"name\": \"f3\", \"type\": \"D\" },\n"
    + "{ \"name\": \"f4\", \"type\": \"int\" },\n"
    + "{ \"name\": \"f5\", \"type\": \"int\" },\n"
    + "{ \"name\": \"f6\", \"type\": \"int\" }\n"
    + "] }";
  
  private static class Rec {
    double f1;
    double f2;
    double f3;
    int f4;
    int f5;
    int f6;
    Rec(Random r) {
      f1 = r.nextDouble();
      f2 = r.nextDouble();
      f3 = r.nextDouble();
      f4 = r.nextInt();
      f5 = r.nextInt();
      f6 = r.nextInt();
    }
  }

  static class RecordTest extends BasicTest {
    Rec[] sourceData = null;
    public RecordTest() throws IOException {
      this("Record");
    }
    public RecordTest(String name) throws IOException {
      super(name, RECORD_SCHEMA, 6);
    }
    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new Rec[count];
      for (int i = 0; i < sourceData.length; i++) {
        sourceData[i] = new Rec(r); 
      }
    }
    @Override
    void readInternal(Decoder d) throws IOException {
      for (int i = 0; i < count; i++) {
        d.readDouble();
        d.readDouble();
        d.readDouble();
        d.readInt();
        d.readInt();
        d.readInt();
      }
    }
    @Override
    void writeInternal(Encoder e) throws IOException {
      for (int i = 0; i < sourceData.length; i++) {
        Rec r = sourceData[i];
        e.writeDouble(r.f1);
        e.writeDouble(r.f2);
        e.writeDouble(r.f3);
        e.writeInt(r.f4);
        e.writeInt(r.f5);
        e.writeInt(r.f6);
      }
    }
    @Override
    void reset() {
      sourceData = null;
      data = null;
    }
  }
  
  static class ValidatingRecord extends RecordTest {
    ValidatingRecord() throws IOException {
      super("ValidatingRecord");
    }
    @Override
    protected Decoder getDecoder() throws IOException {
      return new ValidatingDecoder(schema, super.getDecoder());
    }
    @Override
    protected Encoder getEncoder() throws IOException {
      return new ValidatingEncoder(schema, super.getEncoder());  
    }
  }
  
  static class ResolvingRecord extends RecordTest {
    public ResolvingRecord() throws IOException {
      super("ResolvingRecord");
      isWriteTest = false;
    }
    @Override
    protected Decoder getDecoder() throws IOException {
      return new ResolvingDecoder(schema, schema, super.getDecoder());
    }
  }

  private static final String RECORD_SCHEMA_WITH_DEFAULT =
    "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
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
    + "] }";
  
  private static final String RECORD_SCHEMA_WITH_OUT_OF_ORDER =
    "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
    + "{ \"name\": \"f1\", \"type\": \"double\" },\n"
    + "{ \"name\": \"f3\", \"type\": \"double\" },\n"
    + "{ \"name\": \"f5\", \"type\": \"int\" },\n"
    + "{ \"name\": \"f2\", \"type\": \"double\" },\n"
    + "{ \"name\": \"f4\", \"type\": \"int\" },\n"
    + "{ \"name\": \"f6\", \"type\": \"int\" }\n"
    + "] }";

  private static final String RECORD_SCHEMA_WITH_PROMOTION =
    "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
    + "{ \"name\": \"f1\", \"type\": \"double\" },\n"
    + "{ \"name\": \"f2\", \"type\": \"double\" },\n"
    + "{ \"name\": \"f3\", \"type\": \"double\" },\n"
    + "{ \"name\": \"f4\", \"type\": \"long\" },\n"
    + "{ \"name\": \"f5\", \"type\": \"long\" },\n"
    + "{ \"name\": \"f6\", \"type\": \"long\" }\n"
    + "] }";


  /**
   * Tests the performance of introducing default values.
   */
  static class RecordWithDefault extends RecordTest {
    private final Schema readerSchema;
    public RecordWithDefault() throws IOException {
      super("RecordWithDefault");
      readerSchema = Schema.parse(RECORD_SCHEMA_WITH_DEFAULT);
      isWriteTest = false;
    }
    @Override
    protected Decoder getDecoder() throws IOException {
      return new ResolvingDecoder(schema, readerSchema, super.getDecoder());
    }
    @Override
    protected void readInternal(Decoder d) throws IOException {
      ResolvingDecoder r = (ResolvingDecoder) d;
      Field[] ff = r.readFieldOrder();
      for (int i = 0; i < count; i++) {
        for (int j = 0; j < ff.length; j++) {
          Field f = ff[j];
          switch (f.pos()) {
          case 0:
          case 1:
          case 2:
            r.readDouble();
            break;
          case 3:
          case 4:
          case 5:
            r.readInt();
            break;
          case 6:
          case 7:
            r.readString(null);
            break;
          }
        }
      }
    }
  }
  
  /**
   * Tests the performance of resolving a change in field order.
   */
  static class RecordWithOutOfOrder extends RecordTest {
    private final Schema readerSchema;
    public RecordWithOutOfOrder() throws IOException {
      super("RecordWithOutOfOrder");
      readerSchema = Schema.parse(RECORD_SCHEMA_WITH_OUT_OF_ORDER);
      isWriteTest = false;
    }
    @Override
    protected Decoder getDecoder() throws IOException {
      return new ResolvingDecoder(schema, readerSchema, super.getDecoder());
    }
    @Override
    protected void readInternal(Decoder d) throws IOException {
      ResolvingDecoder r = (ResolvingDecoder) d;
      Field[] ff = r.readFieldOrder();
      for (int i = 0; i < count; i++) {
        for (int j = 0; j < ff.length; j++) {
          Field f = ff[j];
          switch (f.pos()) {
          case 0:
          case 1:
          case 3:
            r.readDouble();
            break;
          case 2:
          case 4:
          case 5:
            r.readInt();
            break;
          }
        }
      }
    }
  }
  
  /**
   * Tests the performance of resolving a type promotion.
   */
  static class RecordWithPromotion extends RecordTest {
    private final Schema readerSchema;
    public RecordWithPromotion() throws IOException {
      super("RecordWithPromotion");
      readerSchema = Schema.parse(RECORD_SCHEMA_WITH_PROMOTION);
      isWriteTest = false;
    }
    @Override
    protected Decoder getDecoder() throws IOException {
      return new ResolvingDecoder(schema, readerSchema, super.getDecoder());
    }
    @Override
    protected void readInternal(Decoder d) throws IOException {
      ResolvingDecoder r = (ResolvingDecoder) d;
      Field[] ff = r.readFieldOrder();
      for (int i = 0; i < count; i++) {
        for (int j = 0; j < ff.length; j++) {
          Field f = ff[j];
          switch (f.pos()) {
          case 0:
          case 1:
          case 2:
            r.readDouble();
            break;
          case 3:
          case 4:
          case 5:
            r.readLong();
            break;
          }
        }
      }
    }
  }
  
  static class GenericTest extends BasicTest {
    GenericRecord[] sourceData = null;
    protected final GenericDatumReader<Object> reader;
    public GenericTest() throws IOException {
      this("Generic");
    }
    protected GenericTest(String name) throws IOException {
      this(name, RECORD_SCHEMA);
    }
    protected GenericTest(String name, String writerSchema) throws IOException {
      super(name, writerSchema, 12);
      reader = newReader();
    }
    protected GenericDatumReader<Object> getReader() {
      return reader;
    }
    protected GenericDatumReader<Object> newReader() {
      return new GenericDatumReader<Object>(schema);
    }
    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new GenericRecord[count];
      for (int i = 0; i < sourceData.length; i++) {
        GenericRecord rec = new GenericData.Record(schema);
        rec.put(0, r.nextDouble());
        rec.put(1, r.nextDouble());
        rec.put(2, r.nextDouble());
        rec.put(3, r.nextInt());
        rec.put(4, r.nextInt());
        rec.put(5, r.nextInt());
        sourceData[i] = rec; 
      }
    }
    @Override
    void readInternal(Decoder d) throws IOException {
      for (int i = 0; i < count; i++) {
        getReader().read(null, d);
      }
    }
    @Override
    void writeInternal(Encoder e) throws IOException {
      GenericDatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);
      for (int i = 0; i < sourceData.length; i++) {
        GenericRecord rec = sourceData[i];
        writer.write(rec, e);
      }
    }
    @Override
    void reset() {
      sourceData = null;
      data = null;
    }
  }
  
  static class GenericNested extends GenericTest {
    public GenericNested() throws IOException {
      super("GenericNested_", NESTED_RECORD_SCHEMA);
    }
    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new GenericRecord[count];
      Schema doubleSchema = schema.getFields().get(0).schema();
      for (int i = 0; i < sourceData.length; i++) {
        GenericRecord rec = new GenericData.Record(schema);
        GenericRecord inner;
        inner = new GenericData.Record(doubleSchema);
        inner.put(0, r.nextDouble());
        rec.put(0, inner);
        inner = new GenericData.Record(doubleSchema);
        inner.put(0, r.nextDouble());
        rec.put(1, inner);
        inner = new GenericData.Record(doubleSchema);
        inner.put(0, r.nextDouble());
        rec.put(2, inner);
        rec.put(3, r.nextInt());
        rec.put(4, r.nextInt());
        rec.put(5, r.nextInt());
        sourceData[i] = rec; 
      }
    }
  }

  private static abstract class GenericResolving extends GenericTest {
    protected GenericResolving(String name)
    throws IOException {
      super(name);
      isWriteTest = false;
    }
    @Override
    protected GenericDatumReader<Object> newReader() {
      return new GenericDatumReader<Object>(schema, getReaderSchema());
    }
    protected abstract Schema getReaderSchema();
  }

  static class GenericWithDefault extends GenericResolving {
    GenericWithDefault() throws IOException {
      super("GenericWithDefault_");
    }
    @Override
    protected Schema getReaderSchema() {
      return  Schema.parse(RECORD_SCHEMA_WITH_DEFAULT);
    }
  }

  static class GenericWithOutOfOrder extends GenericResolving {
    GenericWithOutOfOrder() throws IOException {
      super("GenericWithOutOfOrder_");
    }
    @Override
    protected Schema getReaderSchema() {
      return Schema.parse(RECORD_SCHEMA_WITH_OUT_OF_ORDER);
    }
  }

  static class GenericWithPromotion extends GenericResolving {
    GenericWithPromotion() throws IOException {
      super("GenericWithPromotion_");
    }
    @Override
    protected Schema getReaderSchema() {
      return Schema.parse(RECORD_SCHEMA_WITH_PROMOTION);
    }
  }
  
  static class GenericOneTimeDecoderUse extends GenericTest {
    public GenericOneTimeDecoderUse() throws IOException {
      super("GenericOneTimeDecoderUse_");
      isWriteTest = false;
    }
    @Override
    protected Decoder getDecoder() {
      return newDecoder();
    }
  }

  static class GenericOneTimeReaderUse extends GenericTest {
    public GenericOneTimeReaderUse() throws IOException {
      super("GenericOneTimeReaderUse_");
      isWriteTest = false;
    }
    @Override
    protected GenericDatumReader<Object> getReader() {
      return newReader();
    }
  }

  static class GenericOneTimeUse extends GenericTest {
    public GenericOneTimeUse() throws IOException {
      super("GenericOneTimeUse_");
      isWriteTest = false;
    }
    @Override
    protected GenericDatumReader<Object> getReader() {
      return newReader();
    }
    @Override
    protected Decoder getDecoder() {
      return newDecoder();
    }
  }
  
}
