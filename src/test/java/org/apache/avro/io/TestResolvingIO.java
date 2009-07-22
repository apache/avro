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
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.io.TestValidatingIO.Encoding;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestResolvingIO {
  @Test(dataProvider="data1")
  public void test_identical(Encoding encoding,
      int skipLevel, String jsonWriterSchema,
      String writerCalls, 
      String jsonReaderSchema, String readerCalls)
  throws IOException {
    performTest(encoding, skipLevel, jsonWriterSchema, writerCalls,
        jsonReaderSchema, readerCalls);
  }
  
  private static final int COUNT = 10;
  
  @Test(dataProvider="data2")
  public void test_compatible(Encoding encoding,
      int skipLevel, String jsonWriterSchema,
      String writerCalls, 
      String jsonReaderSchema, String readerCalls)
  throws IOException {
    performTest(encoding, skipLevel, jsonWriterSchema, writerCalls,
        jsonReaderSchema, readerCalls);
  }

  @Test(dataProvider="data3")
  public void test_resolving(Encoding encoding, int skipLevel,
      String jsonWriterSchema, String writerCalls,
      Object[] writerValues,
      String jsonReaderSchema, String readerCalls, Object[] readerValues)
    throws IOException {
    Schema writerSchema = Schema.parse(jsonWriterSchema);
    byte[] bytes = TestValidatingIO.make(writerSchema, writerCalls,
        writerValues, Encoding.BINARY);
    Schema readerSchema = Schema.parse(jsonReaderSchema);
    check(writerSchema, readerSchema, bytes, readerCalls,
        readerValues,
        Encoding.BINARY, skipLevel);
  }

  private void performTest(Encoding encoding,
      int skipLevel, String jsonWriterSchema,
      String writerCalls, 
      String jsonReaderSchema, String readerCalls)
  throws IOException {
    for (int i = 0; i < COUNT; i++) {
      testOnce(jsonWriterSchema, writerCalls,
          jsonReaderSchema, readerCalls, encoding, skipLevel);
    }
  }
  
  private void testOnce(String jsonWriterSchema,
      String writerCalls,
      String jsonReaderSchema,
      String readerCalls,
      Encoding encoding,
      int skipLevel) throws IOException {
    Object[] values = TestValidatingIO.randomValues(writerCalls);
    Schema writerSchema = Schema.parse(jsonWriterSchema);
    byte[] bytes = TestValidatingIO.make(writerSchema, writerCalls,
        values, encoding);
    Schema readerSchema = Schema.parse(jsonReaderSchema);
    check(writerSchema, readerSchema, bytes, readerCalls,
        values,
        encoding, skipLevel);
  }

  private static void check(Schema wsc, Schema rsc, byte[] bytes,
      String calls, Object[] values, Encoding encoding,
      int skipLevel)
      throws IOException {
    // TestValidatingIO.dump(bytes);
    // System.out.println(new String(bytes, "UTF-8"));
    InputStream in = new ByteArrayInputStream(bytes);
    Decoder bvi = null;
    switch (encoding) {
    case BINARY:
    case BLOCKING_BINARY:
      bvi = new BinaryDecoder(in);
      break;
    case JSON:
      bvi = new JsonDecoder(wsc, in);
      break;
    }
    Decoder vi = new ResolvingDecoder(wsc, rsc, bvi);
    TestValidatingIO.check(vi, calls, values, skipLevel);
  }
  
  @DataProvider
  public static Iterator<Object[]> data1() {
    return TestValidatingIO.cartesian(encodings, skipLevels,
        TestValidatingIO.paste(TestValidatingIO.testSchemas(),
            TestValidatingIO.testSchemas()));
  }
  
  @DataProvider
  public static Iterator<Object[]> data2() {
    return TestValidatingIO.cartesian(encodings, skipLevels, testSchemas());
  }
  
  @DataProvider
  public static Iterator<Object[]> data3() {
    return TestValidatingIO.cartesian(encodings, skipLevels,
        dataForResolvingTests());
  }

  private static Object[][] encodings = new Object[][] { { Encoding.BINARY } };
  private static Object[][] skipLevels =
    new Object[][] { { -1 }, { 0 }, { 1 }, { 2 }  };
  private static Object[][] testSchemas() {
    // The mnemonics are the same as {@link TestValidatingIO#testSchemas}
    return new Object[][] {
        // { "\"int\"", "I", "\"float\"", "F" }, // makes sense?
        { "\"int\"", "I", "\"double\"", "D" },
        // { "\"long\"", "L", "\"float\"", "F" }, // And this?
        { "\"long\"", "L", "\"double\"", "D" },
        { "\"float\"", "F", "\"double\"", "D" },
        { "\"double\"", "D", "\"long\"", "L" },

        { "{\"type\":\"array\", \"items\": \"int\"}", "[]",
          "{\"type\":\"array\", \"items\": \"long\"}", "[]", },
        { "{\"type\":\"array\", \"items\": \"int\"}", "[]",
          "{\"type\":\"array\", \"items\": \"double\"}", "[]" },
        { "{\"type\":\"array\", \"items\": \"long\"}", "[]",
          "{\"type\":\"array\", \"items\": \"double\"}", "[]" },
        { "{\"type\":\"array\", \"items\": \"float\"}", "[]",
          "{\"type\":\"array\", \"items\": \"double\"}", "[]" },

        { "{\"type\":\"array\", \"items\": \"int\"}", "[c1sI]",
          "{\"type\":\"array\", \"items\": \"long\"}", "[c1sL]" },
        { "{\"type\":\"array\", \"items\": \"int\"}", "[c1sI]",
          "{\"type\":\"array\", \"items\": \"double\"}", "[c1sD]" },
        { "{\"type\":\"array\", \"items\": \"long\"}", "[c1sL]",
          "{\"type\":\"array\", \"items\": \"double\"}", "[c1sD]" },
        { "{\"type\":\"array\", \"items\": \"float\"}", "[c1sF]",
          "{\"type\":\"array\", \"items\": \"double\"}", "[c1sD]" },

        { "{\"type\":\"map\", \"values\": \"int\"}", "{}",
          "{\"type\":\"map\", \"values\": \"long\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"int\"}", "{}",
          "{\"type\":\"map\", \"values\": \"double\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"long\"}", "{}",
          "{\"type\":\"map\", \"values\": \"double\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"float\"}", "{}",
          "{\"type\":\"map\", \"values\": \"double\"}", "{}" },

        { "{\"type\":\"map\", \"values\": \"int\"}", "{c1sK5I}",
          "{\"type\":\"map\", \"values\": \"long\"}", "{c1sK5L}" },
        { "{\"type\":\"map\", \"values\": \"int\"}", "{c1sK5I}",
          "{\"type\":\"map\", \"values\": \"double\"}", "{c1sK5D}" },
        { "{\"type\":\"map\", \"values\": \"long\"}", "{c1sK5L}",
          "{\"type\":\"map\", \"values\": \"double\"}", "{c1sK5D}" },
        { "{\"type\":\"map\", \"values\": \"float\"}", "{c1sK5F}",
          "{\"type\":\"map\", \"values\": \"double\"}", "{c1sK5D}" },

        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"int\"}]}", "I",
          "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"long\"}]}", "L" },
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"int\"}]}", "I",
          "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"double\"}]}", "D" },

        // multi-field record with promotions
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f0\", \"type\":\"boolean\"},"
          + "{\"name\":\"f1\", \"type\":\"int\"},"
          + "{\"name\":\"f2\", \"type\":\"float\"},"
          + "{\"name\":\"f3\", \"type\":\"string\"}]}", "BIFS",
          "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f0\", \"type\":\"boolean\"},"
          + "{\"name\":\"f1\", \"type\":\"long\"},"
          + "{\"name\":\"f2\", \"type\":\"double\"},"
          + "{\"name\":\"f3\", \"type\":\"string\"}]}", "BLDS" },

        { "[\"int\"]", "U0I",
              "[\"long\"]", "U0L" },
        { "[\"int\"]", "U0I",
              "[\"double\"]", "U0D" },
        { "[\"long\"]", "U0L",
              "[\"double\"]", "U0D" },
        { "[\"float\"]", "U0F",
              "[\"double\"]", "U0D" },

        { "\"int\"", "I", "[\"int\"]", "U0I" },

        { "[\"int\"]", "U0I", "\"int\"", "I" },
        { "[\"int\"]", "U0I", "\"long\"", "L" },

        { "[\"boolean\", \"int\"]", "U1I",
              "[\"boolean\", \"long\"]", "U1L" },
        { "[\"boolean\", \"int\"]", "U1I",
              "[\"long\", \"boolean\"]", "U0L" },
    };
  }

  private static Object[][] dataForResolvingTests() {
    // The mnemonics are the same as {@link TestValidatingIO#testSchemas}
    return new Object[][] {
        // Reordered fields
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f1\", \"type\":\"int\"},"
          + "{\"name\":\"f2\", \"type\":\"string\"}]}", "IS10",
          new Object[] { 10, "hello" },
          "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f2\", \"type\":\"string\" },"
          + "{\"name\":\"f1\", \"type\":\"long\"}]}", "LS10",
          new Object[] { 10L, "hello" } },
        
        // Default values
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":[]}", "",
          new Object[] { },
          "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"int\", \"default\": 100}]}", "I",
          new Object[] { 100 } },
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
            + "{\"name\":\"f2\", \"type\":\"int\"}]}", "I",
          new Object[] { 10 },
          "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f1\", \"type\":\"int\", \"default\": 101},"
          + "{\"name\":\"f2\", \"type\":\"int\"}]}", "II",
          new Object[] { 10, 101 } },
        { "{\"type\":\"record\",\"name\":\"outer\",\"fields\":["
            + "{\"name\": \"g1\", " +
            		"\"type\":{\"type\":\"record\",\"name\":\"inner\",\"fields\":["
                + "{\"name\":\"f2\", \"type\":\"int\"}]}}, "
            + "{\"name\": \"g2\", \"type\": \"long\"}]}", "IL",
          new Object[] { 10, 11L },
          "{\"type\":\"record\",\"name\":\"outer\",\"fields\":["
            + "{\"name\": \"g1\", " +
            		"\"type\":{\"type\":\"record\",\"name\":\"inner\",\"fields\":["
                + "{\"name\":\"f1\", \"type\":\"int\", \"default\": 101},"
                + "{\"name\":\"f2\", \"type\":\"int\"}]}}, "
          + "{\"name\": \"g2\", \"type\": \"long\"}]}}", "IIL",
          new Object[] { 10, 101, 11L } },
    };
  }
}
