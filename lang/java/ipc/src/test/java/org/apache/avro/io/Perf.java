/*
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
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.avro.FooBarSpecificRecord;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.TypeEnum;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.Utf8;


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

  /**
   * New random random.
   *
   * @return the random
   */
  protected static Random newRandom() {
    return new Random(SEED);
  }

  private static class TestDescriptor {
    /**
     * The Test.
     */
    Class<? extends Test> test;
    /**
     * The Param.
     */
    String param;

    /**
     * Instantiates a new Test descriptor.
     *
     * @param test  the test
     * @param param the param
     */
    TestDescriptor(Class<? extends Test> test, String param) {
      this.test = test;
      this.param = param;
    }

    /**
     * Add.
     *
     * @param typeList the type list
     */
    void add(List<TestDescriptor> typeList) {
      ALL_TESTS.put(param, this);
      typeList.add(this);
    }
  }

  private static final List<TestDescriptor> BASIC = new ArrayList<>();
  private static final List<TestDescriptor> RECORD = new ArrayList<>();
  private static final List<TestDescriptor> GENERIC = new ArrayList<>();
  private static final List<TestDescriptor> GENERIC_ONETIME = new ArrayList<>();
  private static final List<TestDescriptor> SPECIFIC = new ArrayList<>();
  private static final List<TestDescriptor> REFLECT = new ArrayList<>();
  private static final LinkedHashMap<String, TestDescriptor> ALL_TESTS;
  private static final LinkedHashMap<String, List<TestDescriptor>> BATCHES;
  static {
    ALL_TESTS = new LinkedHashMap<>();
    BATCHES = new LinkedHashMap<>();
    BATCHES.put("-basic", BASIC);
    new TestDescriptor(IntTest.class, "-i").add(BASIC);
    new TestDescriptor(SmallLongTest.class, "-ls").add(BASIC);
    new TestDescriptor(LongTest.class, "-l").add(BASIC);
    new TestDescriptor(FloatTest.class, "-f").add(BASIC);
    new TestDescriptor(DoubleTest.class, "-d").add(BASIC);
    new TestDescriptor(BoolTest.class, "-b").add(BASIC);
    new TestDescriptor(BytesTest.class, "-by").add(BASIC);
    new TestDescriptor(StringTest.class, "-s").add(BASIC);
    new TestDescriptor(ArrayTest.class, "-a").add(BASIC);
    new TestDescriptor(MapTest.class, "-m").add(BASIC);
    BATCHES.put("-record", RECORD);
    new TestDescriptor(RecordTest.class, "-R").add(RECORD);
    new TestDescriptor(ValidatingRecord.class, "-Rv").add(RECORD);
    new TestDescriptor(ResolvingRecord.class, "-Rr").add(RECORD);
    new TestDescriptor(RecordWithDefault.class, "-Rd").add(RECORD);
    new TestDescriptor(RecordWithOutOfOrder.class, "-Ro").add(RECORD);
    new TestDescriptor(RecordWithPromotion.class, "-Rp").add(RECORD);
    BATCHES.put("-generic", GENERIC);
    new TestDescriptor(GenericTest.class, "-G").add(GENERIC);
    new TestDescriptor(GenericStrings.class, "-Gs").add(GENERIC);
    new TestDescriptor(GenericNested.class, "-Gn").add(GENERIC);
    new TestDescriptor(GenericNestedFake.class, "-Gf").add(GENERIC);
    new TestDescriptor(GenericWithDefault.class, "-Gd").add(GENERIC);
    new TestDescriptor(GenericWithOutOfOrder.class, "-Go").add(GENERIC);
    new TestDescriptor(GenericWithPromotion.class, "-Gp").add(GENERIC);
    BATCHES.put("-generic-onetime", GENERIC_ONETIME);
    new TestDescriptor(GenericOneTimeDecoderUse.class, "-Gotd").add(GENERIC_ONETIME);
    new TestDescriptor(GenericOneTimeReaderUse.class, "-Gotr").add(GENERIC_ONETIME);
    new TestDescriptor(GenericOneTimeUse.class, "-Got").add(GENERIC_ONETIME);
    new TestDescriptor(FooBarSpecificRecordTest.class, "-Sf").add(SPECIFIC);
    BATCHES.put("-reflect", REFLECT);
    new TestDescriptor(ReflectRecordTest.class, "-REFr").add(REFLECT);
    new TestDescriptor(ReflectBigRecordTest.class, "-REFbr").add(REFLECT);
    new TestDescriptor(ReflectFloatTest.class, "-REFf").add(REFLECT);
    new TestDescriptor(ReflectDoubleTest.class, "-REFd").add(REFLECT);
    new TestDescriptor(ReflectIntArrayTest.class, "-REFia").add(REFLECT);
    new TestDescriptor(ReflectLongArrayTest.class, "-REFla").add(REFLECT);
    new TestDescriptor(ReflectDoubleArrayTest.class, "-REFda").add(REFLECT);
    new TestDescriptor(ReflectFloatArrayTest.class, "-REFfa").add(REFLECT);
    new TestDescriptor(ReflectNestedFloatArrayTest.class, "-REFnf").add(REFLECT);
    new TestDescriptor(ReflectNestedObjectArrayTest.class, "-REFno").add(REFLECT);
    new TestDescriptor(ReflectNestedLargeFloatArrayTest.class, "-REFnlf").add(REFLECT);
    new TestDescriptor(ReflectNestedLargeFloatArrayBlockedTest.class, "-REFnlfb").add(REFLECT);
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

  /**
   * The entry point of application.
   *
   * @param args the input arguments
   * @throws Exception the exception
   */
  public static void main(String[] args) throws Exception {
    List<Test> tests = new ArrayList<>();
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
        for (TestDescriptor td : lt) {
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
        if (t.isReadTest()) {
          t.readTest();
        }
        if (t.isWriteTest()) {
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
        for (int i = 0; i < t.cycles/2; i++) {
          t.readTest();
        }
      }
      if (t.isWriteTest() && writeTests) {
        for (int i = 0; i < t.cycles/2; i++) {
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
        "%60s     time    M entries/sec   M bytes/sec  bytes/cycle",
        "test name");
    System.out.println(header.toString());
  }

  private static final void printResult(long s, Test t, String name) {
    s /= 1000;
    double entries = (t.cycles * (double) t.count);
    double bytes = t.cycles * (double) t.encodedSize;
    StringBuilder result = new StringBuilder();
    result.append(String.format("%42s: %6d ms  ", name, (s/1000)));
    result.append(String.format("%10.3f   %11.3f   %11d",
        (entries / s), (bytes/ s),  t.encodedSize));
    System.out.println(result.toString());
  }

  private abstract static class Test {

    /**
     * Name of the test.
     */
    public final String name;
    /**
     * The Count.
     */
    public final int count;
    /**
     * The Cycles.
     */
    public final int cycles;
    /**
     * The Encoded size.
     */
    public long encodedSize = 0;
    /**
     * The Is read test.
     */
    protected boolean isReadTest = true;
    /**
     * The Is write test.
     */
    protected boolean isWriteTest = true;
    /**
     * The Decoder factory.
     */
    static DecoderFactory decoder_factory = new DecoderFactory();
    /**
     * The Encoder factory.
     */
    static EncoderFactory encoder_factory = new EncoderFactory();

    /**
     * Instantiates a new Test.
     *
     * @param name   the name
     * @param cycles the cycles
     * @param count  the count
     */
    public Test(String name, int cycles, int count) {
      this.name = name;
      this.cycles = cycles;
      this.count = count;
    }

    /**
     * Reads data from a Decoder and returns the time taken in nanoseconds.
     *
     * @return the long
     * @throws IOException the io exception
     */
    abstract long readTest() throws IOException;

    /**
     * Writes data to an Encoder and returns the time taken in nanoseconds.
     *
     * @return the long
     * @throws IOException the io exception
     */
    abstract long writeTest() throws IOException;

    /**
     * Is write test boolean.
     *
     * @return the boolean
     */
    final boolean isWriteTest() {
      return isWriteTest;
    }

    /**
     * Is read test boolean.
     *
     * @return the boolean
     */
    final boolean isReadTest() {
      return isReadTest;
    }

    /**
     * initializes data for read and write tests  @throws IOException the io exception
     */
    abstract void init() throws IOException;

    /**
     * clears generated data arrays and other large objects created during initialization
     */
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
    /**
     * The Schema.
     */
    protected final Schema schema;
    /**
     * The Data.
     */
    protected byte[] data;

    /**
     * Instantiates a new Basic test.
     *
     * @param name the name
     * @param json the json
     * @throws IOException the io exception
     */
    BasicTest(String name, String json) throws IOException {
      this(name, json, 1);
    }

    /**
     * Instantiates a new Basic test.
     *
     * @param name   the name
     * @param json   the json
     * @param factor the factor
     * @throws IOException the io exception
     */
    BasicTest(String name, String json, int factor) throws IOException {
      super(name, CYCLES, COUNT/factor);
      this.schema = new Schema.Parser().parse(json);
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

    /**
     * Gets decoder.
     *
     * @return the decoder
     * @throws IOException the io exception
     */
    protected Decoder getDecoder() throws IOException {
      return newDecoder();
    }

    private Encoder getEncoder() throws IOException {
      return newEncoder(getOutputStream());
    }

    /**
     * New decoder decoder.
     *
     * @return the decoder
     */
    protected Decoder newDecoder() {
      return decoder_factory.binaryDecoder(data, null);
    }

    /**
     * New encoder encoder.
     *
     * @param out the out
     * @return the encoder
     * @throws IOException the io exception
     */
    protected Encoder newEncoder(ByteArrayOutputStream out) throws IOException {
      Encoder e = encoder_factory.binaryEncoder(out, null);
//    Encoder e = encoder_factory.directBinaryEncoder(out, null);
//    Encoder e = encoder_factory.blockingBinaryEncoder(out, null);
//    Encoder e = new LegacyBinaryEncoder(out);
      return e;
    }

    private ByteArrayOutputStream getOutputStream() {
      return new ByteArrayOutputStream((int)(encodedSize > 0 ? encodedSize : count));
    }

    @Override
    void init() throws IOException {
      genSourceData();
      ByteArrayOutputStream baos = getOutputStream();
      Encoder e = newEncoder(baos);
      writeInternal(e);
      e.flush();
      data = baos.toByteArray();
      encodedSize = data.length;
      //System.out.println(this.getClass().getSimpleName() + " encodedSize=" + encodedSize);
    }

    /**
     * Gen source data.
     */
    abstract void genSourceData();

    /**
     * Read internal.
     *
     * @param d the d
     * @throws IOException the io exception
     */
    abstract void readInternal(Decoder d) throws IOException;

    /**
     * Write internal.
     *
     * @param e the e
     * @throws IOException the io exception
     */
    abstract void writeInternal(Encoder e) throws IOException;
  }

  /**
   * The type Int test.
   */
  static class IntTest extends BasicTest {
    /**
     * The Source data.
     */
    protected int[] sourceData = null;

    /**
     * Instantiates a new Int test.
     *
     * @throws IOException the io exception
     */
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

  /**
   * The type Small long test.
   */
// This is the same data as ReadInt, but using readLong.
  static class SmallLongTest extends IntTest {
    /**
     * Instantiates a new Small long test.
     *
     * @throws IOException the io exception
     */
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

  /**
   * The type Long test.
   */
// this tests reading Longs that are sometimes very large
  static class LongTest extends BasicTest {
    private long[] sourceData = null;

    /**
     * Instantiates a new Long test.
     *
     * @throws IOException the io exception
     */
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
      // last 16, make full size
      for (int i = sourceData.length - 16; i < sourceData.length; i ++) {
        sourceData[i] = r.nextLong();
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

  /**
   * The type Float test.
   */
  static class FloatTest extends BasicTest {
    /**
     * The Source data.
     */
    float[] sourceData = null;

    /**
     * Instantiates a new Float test.
     *
     * @throws IOException the io exception
     */
    public FloatTest() throws IOException {
      this("Float", "{ \"type\": \"float\"} ");
    }

    /**
     * Instantiates a new Float test.
     *
     * @param name   the name
     * @param schema the schema
     * @throws IOException the io exception
     */
    public FloatTest(String name, String schema) throws IOException {
      super(name, schema);
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
      for (int i = 0; i < count; i+=4) {
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

  /**
   * The type Double test.
   */
  static class DoubleTest extends BasicTest {
    /**
     * The Source data.
     */
    double[] sourceData = null;

    /**
     * Instantiates a new Double test.
     *
     * @throws IOException the io exception
     */
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
      for (int i = 0; i < count; i+=4) {
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

  /**
   * The type Bool test.
   */
  static class BoolTest extends BasicTest {
    /**
     * The Source data.
     */
    boolean[] sourceData = null;

    /**
     * Instantiates a new Bool test.
     *
     * @throws IOException the io exception
     */
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

  /**
   * The type Bytes test.
   */
  static class BytesTest extends BasicTest {
    /**
     * The Source data.
     */
    byte[][] sourceData = null;

    /**
     * Instantiates a new Bytes test.
     *
     * @throws IOException the io exception
     */
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

  private static String randomString(Random r) {
    char[] data = new char[r.nextInt(70)];
    for (int j = 0; j < data.length; j++) {
      data[j] = (char)('a' + r.nextInt('z'-'a'));
    }
    return new String(data);
  }

  /**
   * The type String test.
   */
  static class StringTest extends BasicTest {
    /**
     * The Source data.
     */
    String[] sourceData = null;

    /**
     * Instantiates a new String test.
     *
     * @throws IOException the io exception
     */
    public StringTest() throws IOException {
      super("String", "{ \"type\": \"string\"} ", 5);
    }

    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new String[count];
      for (int i = 0; i < sourceData.length;)
        sourceData[i++] = randomString(r);
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

  /**
   * The type Array test.
   */
  static class ArrayTest extends FloatTest {
    /**
     * Instantiates a new Array test.
     *
     * @throws IOException the io exception
     */
    public ArrayTest() throws IOException {
      super("Array",
          "{ \"type\": \"array\", \"items\": " +
          " { \"type\": \"record\", \"name\":\"Foo\", \"fields\": " +
          "  [{\"name\":\"bar\", \"type\":" +
          "    {\"type\": \"array\", \"items\": " +
          "     { \"type\": \"record\", \"name\":\"Vals\", \"fields\": [" +
          "      {\"name\":\"f1\", \"type\":\"float\"}," +
          "      {\"name\":\"f2\", \"type\":\"float\"}," +
          "      {\"name\":\"f3\", \"type\":\"float\"}," +
          "      {\"name\":\"f4\", \"type\":\"float\"}]" +
          "     }" +
          "    }" +
          "   }]}}");
    }

    @Override
    void readInternal(Decoder d) throws IOException {
      d.readArrayStart();
      for (long i = d.readArrayStart(); i != 0; i = d.arrayNext()) {
        for (long j = 0; j < i; j++) {
          d.readFloat();
          d.readFloat();
          d.readFloat();
          d.readFloat();
        }
      }
      d.arrayNext();
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      int items = sourceData.length/4;
      e.writeArrayStart();
      e.setItemCount(1);
      e.startItem();
      e.writeArrayStart();
      e.setItemCount(items);
      for (int i = 0; i < sourceData.length;i+=4) {
        e.startItem();
        e.writeFloat(sourceData[i]);
        e.writeFloat(sourceData[i+1]);
        e.writeFloat(sourceData[i+2]);
        e.writeFloat(sourceData[i+3]);
      }
      e.writeArrayEnd();
      e.writeArrayEnd();
    }
  }

  /**
   * The type Map test.
   */
  static class MapTest extends FloatTest {
    /**
     * Instantiates a new Map test.
     *
     * @throws IOException the io exception
     */
    public MapTest() throws IOException {
      super("Map", "{ \"type\": \"map\", \"values\": " +
          "  { \"type\": \"record\", \"name\":\"Vals\", \"fields\": [" +
          "   {\"name\":\"f1\", \"type\":\"float\"}," +
          "   {\"name\":\"f2\", \"type\":\"float\"}," +
          "   {\"name\":\"f3\", \"type\":\"float\"}," +
          "   {\"name\":\"f4\", \"type\":\"float\"}]" +
          "  }} ");
    }

    @Override
    void readInternal(Decoder d) throws IOException {
      Utf8 key = new Utf8();
      for (long i = d.readMapStart(); i != 0; i = d.mapNext()) {
        for (long j = 0; j < i; j++) {
          key = d.readString(key);
          d.readFloat();
          d.readFloat();
          d.readFloat();
          d.readFloat();
        }
      }
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      int items = sourceData.length/4;
      e.writeMapStart();
      e.setItemCount(items);
      Utf8 foo = new Utf8("foo");
      for (int i = 0; i < sourceData.length;i+=4) {
        e.startItem();
        e.writeString(foo);
        e.writeFloat(sourceData[i]);
        e.writeFloat(sourceData[i+1]);
        e.writeFloat(sourceData[i+2]);
        e.writeFloat(sourceData[i+3]);
      }
      e.writeMapEnd();
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
    /**
     * The F 1.
     */
    double f1;
    /**
     * The F 2.
     */
    double f2;
    /**
     * The F 3.
     */
    double f3;
    /**
     * The F 4.
     */
    int f4;
    /**
     * The F 5.
     */
    int f5;
    /**
     * The F 6.
     */
    int f6;

    /**
     * Instantiates a new Rec.
     */
    Rec() {

    }

    /**
     * Instantiates a new Rec.
     *
     * @param r the r
     */
    Rec(Random r) {
      f1 = r.nextDouble();
      f2 = r.nextDouble();
      f3 = r.nextDouble();
      f4 = r.nextInt();
      f5 = r.nextInt();
      f6 = r.nextInt();
    }
  }

  /**
   * The type Record test.
   */
  static class RecordTest extends BasicTest {
    /**
     * The Source data.
     */
    Rec[] sourceData = null;

    /**
     * Instantiates a new Record test.
     *
     * @throws IOException the io exception
     */
    public RecordTest() throws IOException {
      this("Record");
    }

    /**
     * Instantiates a new Record test.
     *
     * @param name the name
     * @throws IOException the io exception
     */
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

  /**
   * The type Validating record.
   */
  static class ValidatingRecord extends RecordTest {
    /**
     * Instantiates a new Validating record.
     *
     * @throws IOException the io exception
     */
    ValidatingRecord() throws IOException {
      super("ValidatingRecord");
    }
    @Override
    protected Decoder getDecoder() throws IOException {
      return new ValidatingDecoder(schema, super.getDecoder());
    }
    @Override
    protected Encoder newEncoder(ByteArrayOutputStream out) throws IOException {
      return encoder_factory.validatingEncoder(schema, super.newEncoder(out));
    }
  }

  /**
   * The type Resolving record.
   */
  static class ResolvingRecord extends RecordTest {
    /**
     * Instantiates a new Resolving record.
     *
     * @throws IOException the io exception
     */
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

    /**
     * Instantiates a new Record with default.
     *
     * @throws IOException the io exception
     */
    public RecordWithDefault() throws IOException {
      super("RecordWithDefault");
      readerSchema = new Schema.Parser().parse(RECORD_SCHEMA_WITH_DEFAULT);
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

    /**
     * Instantiates a new Record with out of order.
     *
     * @throws IOException the io exception
     */
    public RecordWithOutOfOrder() throws IOException {
      super("RecordWithOutOfOrder");
      readerSchema = new Schema.Parser().parse(RECORD_SCHEMA_WITH_OUT_OF_ORDER);
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

    /**
     * Instantiates a new Record with promotion.
     *
     * @throws IOException the io exception
     */
    public RecordWithPromotion() throws IOException {
      super("RecordWithPromotion");
      readerSchema = new Schema.Parser().parse(RECORD_SCHEMA_WITH_PROMOTION);
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

  /**
   * The type Generic test.
   */
  static class GenericTest extends BasicTest {
    /**
     * The Source data.
     */
    GenericRecord[] sourceData = null;
    /**
     * The Reader.
     */
    protected final GenericDatumReader<Object> reader;

    /**
     * Instantiates a new Generic test.
     *
     * @throws IOException the io exception
     */
    public GenericTest() throws IOException {
      this("Generic");
    }

    /**
     * Instantiates a new Generic test.
     *
     * @param name the name
     * @throws IOException the io exception
     */
    protected GenericTest(String name) throws IOException {
      this(name, RECORD_SCHEMA);
    }

    /**
     * Instantiates a new Generic test.
     *
     * @param name         the name
     * @param writerSchema the writer schema
     * @throws IOException the io exception
     */
    protected GenericTest(String name, String writerSchema) throws IOException {
      super(name, writerSchema, 12);
      reader = newReader();
    }

    /**
     * Gets reader.
     *
     * @return the reader
     */
    protected GenericDatumReader<Object> getReader() {
      return reader;
    }

    /**
     * New reader generic datum reader.
     *
     * @return the generic datum reader
     */
    protected GenericDatumReader<Object> newReader() {
      return new GenericDatumReader<>(schema);
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
      GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
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

  private static final String GENERIC_STRINGS =
    "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
    + "{ \"name\": \"f1\", \"type\": \"string\" },\n"
    + "{ \"name\": \"f2\", \"type\": \"string\" },\n"
    + "{ \"name\": \"f3\", \"type\": \"string\" }\n"
    + "] }";

  /**
   * The type Generic strings.
   */
  static class GenericStrings extends GenericTest {
    /**
     * Instantiates a new Generic strings.
     *
     * @throws IOException the io exception
     */
    public GenericStrings() throws IOException {
      super("GenericStrings", GENERIC_STRINGS);
    }
    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new GenericRecord[count];
      for (int i = 0; i < sourceData.length; i++) {
        GenericRecord rec = new GenericData.Record(schema);
        rec.put(0, randomString(r));
        rec.put(1, randomString(r));
        rec.put(2, randomString(r));
        sourceData[i] = rec;
      }
    }
  }

  /**
   * The type Generic nested.
   */
  static class GenericNested extends GenericTest {
    /**
     * Instantiates a new Generic nested.
     *
     * @throws IOException the io exception
     */
    public GenericNested() throws IOException {
      super("GenericNested_", NESTED_RECORD_SCHEMA);
    }
    @Override
    void genSourceData() {
      sourceData = generateGenericNested(schema, count);
    }
  }

  /**
   * Generate generic nested generic record [ ].
   *
   * @param schema the schema
   * @param count  the count
   * @return the generic record [ ]
   */
  static GenericRecord[] generateGenericNested(Schema schema, int count) {
    Random r = newRandom();
    GenericRecord[] sourceData = new GenericRecord[count];
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
    return sourceData;
  }

  /**
   * The type Generic nested fake.
   */
  static class GenericNestedFake extends BasicTest {
    /**
     * The Source data.
     */
//reads and writes generic data, but not using
    //GenericDatumReader or GenericDatumWriter
    GenericRecord[] sourceData = null;

    /**
     * Instantiates a new Generic nested fake.
     *
     * @throws IOException the io exception
     */
    public GenericNestedFake() throws IOException {
      super("GenericNestedFake_", NESTED_RECORD_SCHEMA, 12);
    }
    @Override
    void readInternal(Decoder d) throws IOException {
      Schema doubleSchema = schema.getFields().get(0).schema();
      for (int i = 0; i < count; i++) {
        GenericRecord rec = new GenericData.Record(schema);
        GenericRecord inner;
        inner = new GenericData.Record(doubleSchema);
        inner.put(0, d.readDouble());
        rec.put(0, inner);
        inner = new GenericData.Record(doubleSchema);
        inner.put(0, d.readDouble());
        rec.put(1, inner);
        inner = new GenericData.Record(doubleSchema);
        inner.put(0, d.readDouble());
        rec.put(2, inner);
        rec.put(3, d.readInt());
        rec.put(4, d.readInt());
        rec.put(5, d.readInt());
      }
    }
    @Override
    void writeInternal(Encoder e) throws IOException {
      for (int i = 0; i < sourceData.length; i++) {
        GenericRecord rec = sourceData[i];
        GenericRecord inner;
        inner = (GenericRecord)rec.get(0);
        e.writeDouble((Double)inner.get(0));
        inner = (GenericRecord)rec.get(1);
        e.writeDouble((Double)inner.get(0));
        inner = (GenericRecord)rec.get(2);
        e.writeDouble((Double)inner.get(0));
        e.writeInt((Integer)rec.get(3));
        e.writeInt((Integer)rec.get(4));
        e.writeInt((Integer)rec.get(5));
      }
    }
    @Override
    void genSourceData() {
      sourceData = generateGenericNested(schema, count);
    }
    @Override
    void reset() {
      data = null;
      sourceData = null;
    }

  }

  private static abstract class GenericResolving extends GenericTest {
    /**
     * Instantiates a new Generic resolving.
     *
     * @param name the name
     * @throws IOException the io exception
     */
    protected GenericResolving(String name)
    throws IOException {
      super(name);
      isWriteTest = false;
    }
    @Override
    protected GenericDatumReader<Object> newReader() {
      return new GenericDatumReader<>(schema, getReaderSchema());
    }

    /**
     * Gets reader schema.
     *
     * @return the reader schema
     */
    protected abstract Schema getReaderSchema();
  }

  /**
   * The type Generic with default.
   */
  static class GenericWithDefault extends GenericResolving {
    /**
     * Instantiates a new Generic with default.
     *
     * @throws IOException the io exception
     */
    GenericWithDefault() throws IOException {
      super("GenericWithDefault_");
    }
    @Override
    protected Schema getReaderSchema() {
      return  new Schema.Parser().parse(RECORD_SCHEMA_WITH_DEFAULT);
    }
  }

  /**
   * The type Generic with out of order.
   */
  static class GenericWithOutOfOrder extends GenericResolving {
    /**
     * Instantiates a new Generic with out of order.
     *
     * @throws IOException the io exception
     */
    GenericWithOutOfOrder() throws IOException {
      super("GenericWithOutOfOrder_");
    }
    @Override
    protected Schema getReaderSchema() {
      return new Schema.Parser().parse(RECORD_SCHEMA_WITH_OUT_OF_ORDER);
    }
  }

  /**
   * The type Generic with promotion.
   */
  static class GenericWithPromotion extends GenericResolving {
    /**
     * Instantiates a new Generic with promotion.
     *
     * @throws IOException the io exception
     */
    GenericWithPromotion() throws IOException {
      super("GenericWithPromotion_");
    }
    @Override
    protected Schema getReaderSchema() {
      return new Schema.Parser().parse(RECORD_SCHEMA_WITH_PROMOTION);
    }
  }

  /**
   * The type Generic one time decoder use.
   */
  static class GenericOneTimeDecoderUse extends GenericTest {
    /**
     * Instantiates a new Generic one time decoder use.
     *
     * @throws IOException the io exception
     */
    public GenericOneTimeDecoderUse() throws IOException {
      super("GenericOneTimeDecoderUse_");
      isWriteTest = false;
    }
    @Override
    protected Decoder getDecoder() {
      return newDecoder();
    }
  }

  /**
   * The type Generic one time reader use.
   */
  static class GenericOneTimeReaderUse extends GenericTest {
    /**
     * Instantiates a new Generic one time reader use.
     *
     * @throws IOException the io exception
     */
    public GenericOneTimeReaderUse() throws IOException {
      super("GenericOneTimeReaderUse_");
      isWriteTest = false;
    }
    @Override
    protected GenericDatumReader<Object> getReader() {
      return newReader();
    }
  }

  /**
   * The type Generic one time use.
   */
  static class GenericOneTimeUse extends GenericTest {
    /**
     * Instantiates a new Generic one time use.
     *
     * @throws IOException the io exception
     */
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

  /**
   * The type Specific test.
   *
   * @param <T> the type parameter
   */
  static abstract class SpecificTest<T extends SpecificRecordBase> extends BasicTest {
    /**
     * The Reader.
     */
    protected final SpecificDatumReader<T> reader;
    /**
     * The Writer.
     */
    protected final SpecificDatumWriter<T> writer;
    private Object[] sourceData;

    /**
     * Instantiates a new Specific test.
     *
     * @param name         the name
     * @param writerSchema the writer schema
     * @throws IOException the io exception
     */
    protected SpecificTest(String name, String writerSchema) throws IOException {
      super(name, writerSchema, 48);
      reader = newReader();
      writer = newWriter();
    }

    /**
     * Gets reader.
     *
     * @return the reader
     */
    protected SpecificDatumReader<T> getReader() {
      return reader;
    }

    /**
     * Gets writer.
     *
     * @return the writer
     */
    protected SpecificDatumWriter<T> getWriter() {
      return writer;
    }

    /**
     * New reader specific datum reader.
     *
     * @return the specific datum reader
     */
    protected SpecificDatumReader<T> newReader() {
      return new SpecificDatumReader<>(schema);
    }

    /**
     * New writer specific datum writer.
     *
     * @return the specific datum writer
     */
    protected SpecificDatumWriter<T> newWriter() {
      return new SpecificDatumWriter<>(schema);
    }
    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new Object[count];
      for (int i = 0; i < sourceData.length; i++) {
        sourceData[i] = genSingleRecord(r);
      }
    }

    /**
     * Gen single record t.
     *
     * @param r the r
     * @return the t
     */
    protected abstract T genSingleRecord(Random r);

    @Override
    void readInternal(Decoder d) throws IOException {
      for (int i = 0; i < count; i++) {
        getReader().read(null, d);
      }
    }
    @Override
    void writeInternal(Encoder e) throws IOException {
      for (int i = 0; i < sourceData.length; i++) {
        @SuppressWarnings("unchecked")
        T rec = (T) sourceData[i];
        getWriter().write(rec, e);
      }
    }
    @Override
    void reset() {
      sourceData = null;
      data = null;
    }
  }

  /**
   * The type Foo bar specific record test.
   */
  static class FooBarSpecificRecordTest extends
      SpecificTest<FooBarSpecificRecord> {
    /**
     * Instantiates a new Foo bar specific record test.
     *
     * @throws IOException the io exception
     */
    public FooBarSpecificRecordTest() throws IOException {
      super("FooBarSpecificRecordTest", FooBarSpecificRecord.SCHEMA$.toString());
    }

    @Override
    protected FooBarSpecificRecord genSingleRecord(Random r) {
      TypeEnum[] typeEnums = TypeEnum.values();
      List<Integer> relatedIds = new ArrayList<>(10);
      for (int i = 0; i < 10; i++) {
        relatedIds.add(r.nextInt());
      }

      try {
        return FooBarSpecificRecord.newBuilder().setId(r.nextInt())
            .setName(randomString(r))
            .setNicknames(Arrays.asList(randomString(r), randomString(r)))
            .setTypeEnum(typeEnums[r.nextInt(typeEnums.length)])
            .setRelatedids(relatedIds).build();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * The type Reflect test.
   *
   * @param <T> the type parameter
   */
  static abstract class ReflectTest<T> extends BasicTest {
    /**
     * The Source data.
     */
    T[] sourceData = null;
    /**
     * The Reader.
     */
    ReflectDatumReader<T> reader;
    /**
     * The Writer.
     */
    ReflectDatumWriter<T> writer;
    /**
     * The Clazz.
     */
    Class<T> clazz;

    /**
     * Instantiates a new Reflect test.
     *
     * @param name   the name
     * @param sample the sample
     * @param factor the factor
     * @throws IOException the io exception
     */
    @SuppressWarnings("unchecked")
    ReflectTest(String name, T sample, int factor) throws IOException {
      super(name, ReflectData.get().getSchema(sample.getClass()).toString(), factor);
      clazz = (Class<T>) sample.getClass();
      reader = new ReflectDatumReader<>(schema);
      writer = new ReflectDatumWriter<>(schema);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected final void genSourceData() {
      Random r = newRandom();
      sourceData = (T[]) Array.newInstance(clazz, count);
      for (int i = 0; i < sourceData.length; i++) {
        sourceData[i] = createDatum(r);
      }
    }

    /**
     * Create datum t.
     *
     * @param r the r
     * @return the t
     */
    protected abstract T createDatum(Random r);

    @Override
    protected final void readInternal(Decoder d) throws IOException {
      for (int i = 0; i < count; i++) {
        reader.read(null, d);
      }
    }

    @Override
    protected final void writeInternal(Encoder e) throws IOException {
      for (int i = 0; i < sourceData.length; i++) {
        writer.write(sourceData[i], e);
      }
    }

    @Override
    protected final void reset() {
      sourceData = null;
      data = null;
    }
  }

  /**
   * The type Reflect record test.
   */
  static class ReflectRecordTest extends ReflectTest<Rec> {
    /**
     * Instantiates a new Reflect record test.
     *
     * @throws IOException the io exception
     */
    ReflectRecordTest() throws IOException {
      super("ReflectRecord", new Rec(), 12);
    }

    @Override
    protected Rec createDatum(Random r) {
      return new Rec(r);
    }
  }

  /**
   * The type Reflect float test.
   */
  static class ReflectFloatTest extends ReflectTest<float[]> {
    /**
     * Instantiates a new Reflect float test.
     *
     * @throws IOException the io exception
     */
    ReflectFloatTest() throws IOException {
      super("ReflectFloat", new float[0], COUNT);
    }

    @Override
    protected float[] createDatum(Random r) {
      return populateFloatArray(r, COUNT / count);
    }
  }

  /**
   * The type Reflect double test.
   */
  static class ReflectDoubleTest extends ReflectTest<double[]> {
    /**
     * Instantiates a new Reflect double test.
     *
     * @throws IOException the io exception
     */
    ReflectDoubleTest() throws IOException {
      super("ReflectDouble", new double[0], COUNT);
    }

    @Override
    protected double[] createDatum(Random r) {
      return populateDoubleArray(r, COUNT / count);
    }
  }

  /**
   * The type Reflect float array test.
   */
  static class ReflectFloatArrayTest extends ReflectTest<float[]> {
    /**
     * Instantiates a new Reflect float array test.
     *
     * @throws IOException the io exception
     */
    ReflectFloatArrayTest() throws IOException {
      super("ReflectFloatArray", new float[0], 10);
    }

    @Override
    protected float[] createDatum(Random r) {
      return populateFloatArray(r, false);
    }
  }

  /**
   * The type Reflect double array test.
   */
  static class ReflectDoubleArrayTest extends ReflectTest<double[]> {
    /**
     * Instantiates a new Reflect double array test.
     *
     * @throws IOException the io exception
     */
    ReflectDoubleArrayTest() throws IOException {
      super("ReflectDoubleArray", new double[0], 20);
    }

    @Override
    protected double[] createDatum(Random r) {
      return populateDoubleArray(r);
    }
  }

  /**
   * The type Reflect int array test.
   */
  static class ReflectIntArrayTest extends ReflectTest<int[]> {
    /**
     * Instantiates a new Reflect int array test.
     *
     * @throws IOException the io exception
     */
    ReflectIntArrayTest() throws IOException {
      super("ReflectIntArray", new int[0], 12);
    }

    @Override
    protected int[] createDatum(Random r) {
      return populateIntArray(r);
    }
  }

  /**
   * The type Reflect long array test.
   */
  static class ReflectLongArrayTest extends ReflectTest<long[]> {
    /**
     * Instantiates a new Reflect long array test.
     *
     * @throws IOException the io exception
     */
    ReflectLongArrayTest() throws IOException {
      super("ReflectLongArray", new long[0], 24);
    }

    @Override
    protected long[] createDatum(Random r) {
      return populateLongArray(r);
    }
  }


  /**
   * The type Reflect nested object array test.
   */
  static class ReflectNestedObjectArrayTest extends
      ReflectTest<ReflectNestedObjectArrayTest.Foo> {
    /**
     * Instantiates a new Reflect nested object array test.
     *
     * @throws IOException the io exception
     */
    ReflectNestedObjectArrayTest() throws IOException {
      super("ReflectNestedObjectArray", new Foo(new Random()), 50);
    }

    @Override
    protected Foo createDatum(Random r) {
      return new Foo(r);
    }

    /**
     * The type Foo.
     */
    static public class Foo {
      /**
       * The Bar.
       */
      Vals[] bar;

      /**
       * Instantiates a new Foo.
       */
      Foo() {
      }

      /**
       * Instantiates a new Foo.
       *
       * @param r the r
       */
      Foo(Random r) {
        bar = new Vals[smallArraySize(r)];
        for (int i = 0; i < bar.length; i++) {
          bar[i] = new Vals(r);
        }
      }
    }

    /**
     * The type Vals.
     */
    static class Vals {
      /**
       * The F 1.
       */
      float f1;
      /**
       * The F 2.
       */
      float f2;
      /**
       * The F 3.
       */
      float f3;
      /**
       * The F 4.
       */
      float f4;

      /**
       * Instantiates a new Vals.
       */
      Vals(){
      }

      /**
       * Instantiates a new Vals.
       *
       * @param r the r
       */
      Vals(Random r) {
        this.f1 = r.nextFloat();
        this.f2 = r.nextFloat();
        this.f3 = r.nextFloat();
        this.f4 = r.nextFloat();
      }
    }

  }

  /**
   * The type Float foo.
   */
  static public class FloatFoo {
    /**
     * The Float bar.
     */
    float[] floatBar;

    /**
     * Instantiates a new Float foo.
     */
    FloatFoo() {
    }

    /**
     * Instantiates a new Float foo.
     *
     * @param r     the r
     * @param large the large
     */
    FloatFoo(Random r, boolean large) {
      floatBar = populateFloatArray(r, large);
    }
  }

  // average of 8, between 1 and 15
  private static int smallArraySize(Random r) {
    return r.nextInt(15) + 1;
  }

  // average of 64, between 16 and 112
  private static int largeArraySize(Random r) {
    return r.nextInt(97) + 16;
  }

  /**
   * Populate float array float [ ].
   *
   * @param r     the r
   * @param large the large
   * @return the float [ ]
   */
  static float[] populateFloatArray(Random r, boolean large) {
    int size = large ? largeArraySize(r) : smallArraySize(r);
    return populateFloatArray(r, size);
  }

  /**
   * Populate float array float [ ].
   *
   * @param r    the r
   * @param size the size
   * @return the float [ ]
   */
  static float[] populateFloatArray(Random r, int size) {
    float[] result = new float[size];
    for (int i = 0; i < result.length; i++) {
      result[i] = r.nextFloat();
    }
    return result;
  }

  /**
   * Populate double array double [ ].
   *
   * @param r the r
   * @return the double [ ]
   */
  static double[] populateDoubleArray(Random r) {
    return populateDoubleArray(r, smallArraySize(r));
  }

  /**
   * Populate double array double [ ].
   *
   * @param r    the r
   * @param size the size
   * @return the double [ ]
   */
  static double[] populateDoubleArray(Random r, int size) {
    double[] result = new double[size];
    for (int i = 0; i < result.length; i++) {
      result[i] = r.nextDouble();
    }
    return result;
  }

  /**
   * Populate int array int [ ].
   *
   * @param r the r
   * @return the int [ ]
   */
  static int[] populateIntArray(Random r) {
    int size = smallArraySize(r);
    int[] result = new int[size];
    for (int i = 0; i < result.length; i++) {
      result[i] = r.nextInt();
    }
    return result;
  }

  /**
   * Populate long array long [ ].
   *
   * @param r the r
   * @return the long [ ]
   */
  static long[] populateLongArray(Random r) {
    int size = smallArraySize(r);
    long[] result = new long[size];
    for (int i = 0; i < result.length; i++) {
      result[i] = r.nextLong();
    }
    return result;
  }

  /**
   * The type Reflect nested float array test.
   */
  static class ReflectNestedFloatArrayTest extends ReflectTest<FloatFoo> {
    /**
     * Instantiates a new Reflect nested float array test.
     *
     * @throws IOException the io exception
     */
    public ReflectNestedFloatArrayTest() throws IOException {
      super("ReflectNestedFloatArray", new FloatFoo(new Random(), false), 10);
    }

    @Override
    protected FloatFoo createDatum(Random r) {
      return new FloatFoo(r, false);
    }
  }

  /**
   * The type Reflect nested large float array test.
   */
  static class ReflectNestedLargeFloatArrayTest extends ReflectTest<FloatFoo> {
    /**
     * Instantiates a new Reflect nested large float array test.
     *
     * @throws IOException the io exception
     */
    public ReflectNestedLargeFloatArrayTest() throws IOException {
      super("ReflectNestedLargeFloatArray", new FloatFoo(new Random(), true),
          60);
    }

    @Override
    protected FloatFoo createDatum(Random r) {
      return new FloatFoo(r, true);
    }

  }

  /**
   * The type Reflect nested large float array blocked test.
   */
  static class ReflectNestedLargeFloatArrayBlockedTest extends ReflectTest<FloatFoo> {
    /**
     * Instantiates a new Reflect nested large float array blocked test.
     *
     * @throws IOException the io exception
     */
    public ReflectNestedLargeFloatArrayBlockedTest() throws IOException {
      super("ReflectNestedLargeFloatArrayBlocked", new FloatFoo(new Random(), true),
          60);
    }

    @Override
    protected FloatFoo createDatum(Random r) {
      return new FloatFoo(r, true);
    }

    @Override
    protected Encoder newEncoder(ByteArrayOutputStream out) throws IOException {
      return new EncoderFactory().configureBlockSize(254).blockingBinaryEncoder(out, null);
    }

  }

  @SuppressWarnings("unused")
  private static class Rec1 {
    /**
     * The D 1.
     */
    double d1;
    /**
     * The D 11.
     */
    double d11;
    /**
     * The F 2.
     */
    float f2;
    /**
     * The F 22.
     */
    float f22;
    /**
     * The F 3.
     */
    int f3;
    /**
     * The F 33.
     */
    int f33;
    /**
     * The F 4.
     */
    long f4;
    /**
     * The F 44.
     */
    long f44;
    /**
     * The F 5.
     */
    byte f5;
    /**
     * The F 55.
     */
    byte f55;
    /**
     * The F 6.
     */
    short f6;
    /**
     * The F 66.
     */
    short f66;

    /**
     * Instantiates a new Rec 1.
     */
    Rec1() {
    }

    /**
     * Instantiates a new Rec 1.
     *
     * @param r the r
     */
    Rec1(Random r) {
      d1 = r.nextDouble();
      d11 = r.nextDouble();
      f2 = r.nextFloat();
      f22 = r.nextFloat();
      f3 = r.nextInt();
      f33 = r.nextInt();
      f4 = r.nextLong();
      f44 = r.nextLong();
      f5 = (byte) r.nextInt();
      f55 = (byte) r.nextInt();
      f6 = (short) r.nextInt();
      f66 = (short) r.nextInt();
    }
  }

  /**
   * The type Reflect big record test.
   */
  static class ReflectBigRecordTest extends ReflectTest<Rec1> {
    /**
     * Instantiates a new Reflect big record test.
     *
     * @throws IOException the io exception
     */
    public ReflectBigRecordTest() throws IOException {
      super("ReflectBigRecord", new Rec1(new Random()), 20);
    }

    @Override
    protected Rec1 createDatum(Random r) {
      return new Rec1(r);
    }
  }
}
