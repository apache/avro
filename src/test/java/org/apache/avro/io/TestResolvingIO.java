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
import java.util.Vector;

import org.apache.avro.Schema;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestResolvingIO {
  @Test(dataProvider="data")
  public void testRead(String jsonWriterSchema,
      String writerCalls, 
      String jsonReaderSchema,
      String readerCalls) throws IOException {
    test(jsonWriterSchema, writerCalls, jsonReaderSchema, readerCalls, -1);
  }

  @Test(dataProvider="data")
  public void testSkip_0(String jsonWriterSchema,
      String writerCalls, 
      String jsonReaderSchema,
      String readerCalls) throws IOException {
    test(jsonWriterSchema, writerCalls, jsonReaderSchema, readerCalls, 0);
  }

  @Test(dataProvider="data")
  public void testSkip_1(String jsonWriterSchema,
      String writerCalls, 
      String jsonReaderSchema,
      String readerCalls) throws IOException {
    test(jsonWriterSchema, writerCalls, jsonReaderSchema, readerCalls, 1);
  }

  @Test(dataProvider="data")
  public void testSkip_2(String jsonWriterSchema,
      String writerCalls, 
      String jsonReaderSchema,
      String readerCalls) throws IOException {
    test(jsonWriterSchema, writerCalls, jsonReaderSchema, readerCalls, 2);
  }

  private void test(String jsonWriterSchema, String writerCalls,
      String jsonReaderSchema, String readerCalls, int skipLevel)
      throws IOException {
    for (int i = 0; i < 10; i++) {
      testOnce(jsonWriterSchema, writerCalls, jsonReaderSchema,
          readerCalls, skipLevel);
    }
  }
  
  private void testOnce(String jsonWriterSchema,
      String writerCalls, 
      String jsonReaderSchema,
      String readerCalls,
      int skipLevel) throws IOException {
    Vector<Object> values = new Vector<Object>();
    Schema writerSchema = Schema.parse(jsonWriterSchema);
    byte[] bytes = TestValidatingIO.make(writerSchema, writerCalls,
        values, false);
    Schema readerSchema = Schema.parse(jsonReaderSchema);
    // dump(bytes);
    check(writerSchema, readerSchema, bytes, readerCalls, values.toArray(),
        skipLevel);
  }

  private static void check(Schema wsc, Schema rsc, byte[] bytes,
      String calls, Object[] values, int skipLevel) throws IOException {
    // dump(bytes);
    Decoder bvi= new BinaryDecoder(new ByteArrayInputStream(bytes));
    Decoder vi = new ResolvingDecoder(wsc, rsc, bvi);
    TestValidatingIO.check(vi, calls, values, skipLevel);
  }
  
  @DataProvider
  public static Object[][] data() {
    /**
     * The first argument is a schema.
     * The second one is a sequence of (single character) mnemonics:
     * N  null
     * B  boolean
     * I  int
     * L  long
     * F  float
     * D  double
     * S  string
     * b  bytes
     * [  Start array
     * ]  End array
     * {  Start map
     * }  End map
     * s  start item
     * 0-9  branch id for union
     */
    return new Object[][] {
        { "\"null\"", "N", "\"null\"", "N" },
        { "\"boolean\"", "B", "\"boolean\"", "B" },
        { "\"int\"", "I", "\"int\"", "I" },
        { "\"int\"", "I", "\"long\"", "L" },
        // { "\"int\"", "I", "\"float\"", "F" },  // These promotions make sense?
        { "\"int\"", "I", "\"double\"", "D" },
        { "\"long\"", "L", "\"long\"", "L" },
        // { "\"long\"", "L", "\"float\"", "F" }, // And this?
        { "\"long\"", "L", "\"double\"", "D" },
        { "\"float\"", "F", "\"float\"", "F" },
        { "\"float\"", "F", "\"double\"", "D" },
        { "\"double\"", "D", "\"double\"", "D" },
        { "\"double\"", "D", "\"long\"", "L" },
        { "\"string\"", "S", "\"string\"", "S" },
        { "\"bytes\"", "b", "\"bytes\"", "b" },
        { "{\"type\":\"fixed\", \"name\":\"fi\", \"size\": 0}", "f0",
          "{\"type\":\"fixed\", \"name\":\"fi\", \"size\": 0}", "f0" },
        { "{\"type\":\"fixed\", \"name\":\"fi\", \"size\": 10}", "f10",
          "{\"type\":\"fixed\", \"name\":\"fi\", \"size\": 10}", "f10" },
        { "{\"type\":\"enum\", \"name\":\"en\", \"symbols\":[\"v1\", \"v2\"]}",
            "e2",
          "{\"type\":\"enum\", \"name\":\"en\", \"symbols\":[\"v1\", \"v2\"]}",
            "e2"},

        { "{\"type\":\"array\", \"items\": \"boolean\"}", "[]",
          "{\"type\":\"array\", \"items\": \"boolean\"}", "[]" },
        { "{\"type\":\"array\", \"items\": \"int\"}", "[]",
          "{\"type\":\"array\", \"items\": \"int\"}", "[]" },
        { "{\"type\":\"array\", \"items\": \"int\"}", "[]",
          "{\"type\":\"array\", \"items\": \"long\"}", "[]" },
        { "{\"type\":\"array\", \"items\": \"int\"}", "[]",
          "{\"type\":\"array\", \"items\": \"double\"}", "[]" },
        { "{\"type\":\"array\", \"items\": \"long\"}", "[]",
          "{\"type\":\"array\", \"items\": \"long\"}", "[]" },
        { "{\"type\":\"array\", \"items\": \"long\"}", "[]",
          "{\"type\":\"array\", \"items\": \"double\"}", "[]" },
        { "{\"type\":\"array\", \"items\": \"float\"}", "[]",
          "{\"type\":\"array\", \"items\": \"float\"}", "[]" },
        { "{\"type\":\"array\", \"items\": \"float\"}", "[]",
          "{\"type\":\"array\", \"items\": \"double\"}", "[]" },
        { "{\"type\":\"array\", \"items\": \"double\"}", "[]",
          "{\"type\":\"array\", \"items\": \"double\"}", "[]", },
        { "{\"type\":\"array\", \"items\": \"string\"}", "[]",
          "{\"type\":\"array\", \"items\": \"string\"}", "[]" },
        { "{\"type\":\"array\", \"items\": \"bytes\"}", "[]",
          "{\"type\":\"array\", \"items\": \"bytes\"}", "[]" },

        { "{\"type\":\"array\", \"items\": \"boolean\"}", "[c1sB]",
            "{\"type\":\"array\", \"items\": \"boolean\"}", "[c1sB]" },
        { "{\"type\":\"array\", \"items\": \"int\"}", "[c1sI]",
          "{\"type\":\"array\", \"items\": \"int\"}", "[c1sI]" },
        { "{\"type\":\"array\", \"items\": \"int\"}", "[c1sI]",
          "{\"type\":\"array\", \"items\": \"long\"}", "[c1sL]" },
        { "{\"type\":\"array\", \"items\": \"int\"}", "[c1sI]",
          "{\"type\":\"array\", \"items\": \"double\"}", "[c1sD]" },
        { "{\"type\":\"array\", \"items\": \"long\"}", "[c1sL]",
            "{\"type\":\"array\", \"items\": \"long\"}", "[c1sL]" },
        { "{\"type\":\"array\", \"items\": \"long\"}", "[c1sL]",
          "{\"type\":\"array\", \"items\": \"double\"}", "[c1sD]" },
        { "{\"type\":\"array\", \"items\": \"float\"}", "[c1sF]",
            "{\"type\":\"array\", \"items\": \"float\"}", "[c1sF]" },
        { "{\"type\":\"array\", \"items\": \"float\"}", "[c1sF]",
          "{\"type\":\"array\", \"items\": \"double\"}", "[c1sD]" },
        { "{\"type\":\"array\", \"items\": \"double\"}", "[c1sD]",
            "{\"type\":\"array\", \"items\": \"double\"}", "[c1sD]" },
        { "{\"type\":\"array\", \"items\": \"string\"}", "[c1sS]",
            "{\"type\":\"array\", \"items\": \"string\"}", "[c1sS]" },
        { "{\"type\":\"array\", \"items\": \"bytes\"}", "[c1sb]",
            "{\"type\":\"array\", \"items\": \"bytes\"}", "[c1sb]" },

        { "{\"type\":\"map\", \"values\": \"boolean\"}", "{}",
          "{\"type\":\"map\", \"values\": \"boolean\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"int\"}", "{}",
          "{\"type\":\"map\", \"values\": \"int\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"int\"}", "{}",
          "{\"type\":\"map\", \"values\": \"long\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"int\"}", "{}",
          "{\"type\":\"map\", \"values\": \"double\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"long\"}", "{}",
          "{\"type\":\"map\", \"values\": \"long\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"long\"}", "{}",
          "{\"type\":\"map\", \"values\": \"double\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"float\"}", "{}",
          "{\"type\":\"map\", \"values\": \"float\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"float\"}", "{}",
          "{\"type\":\"map\", \"values\": \"double\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"double\"}", "{}",
          "{\"type\":\"map\", \"values\": \"double\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"string\"}", "{}",
          "{\"type\":\"map\", \"values\": \"string\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"bytes\"}", "{}",
          "{\"type\":\"map\", \"values\": \"bytes\"}", "{}" },
        { "{\"type\":\"map\", \"values\": "
          + "{\"type\":\"array\", \"items\":\"int\"}}", "{}",
          "{\"type\":\"map\", \"values\": "
            + "{\"type\":\"array\", \"items\":\"int\"}}", "{}" },

        { "{\"type\":\"map\", \"values\": \"boolean\"}", "{c1sSB}",
          "{\"type\":\"map\", \"values\": \"boolean\"}", "{c1sSB}" },
        { "{\"type\":\"map\", \"values\": \"int\"}", "{c1sSI}",
          "{\"type\":\"map\", \"values\": \"int\"}", "{c1sSI}" },
        { "{\"type\":\"map\", \"values\": \"int\"}", "{c1sSI}",
          "{\"type\":\"map\", \"values\": \"long\"}", "{c1sSL}" },
        { "{\"type\":\"map\", \"values\": \"int\"}", "{c1sSI}",
          "{\"type\":\"map\", \"values\": \"double\"}", "{c1sSD}" },
        { "{\"type\":\"map\", \"values\": \"long\"}", "{c1sSL}",
          "{\"type\":\"map\", \"values\": \"long\"}", "{c1sSL}" },
        { "{\"type\":\"map\", \"values\": \"long\"}", "{c1sSL}",
          "{\"type\":\"map\", \"values\": \"double\"}", "{c1sSD}" },
        { "{\"type\":\"map\", \"values\": \"float\"}", "{c1sSF}",
          "{\"type\":\"map\", \"values\": \"float\"}", "{c1sSF}" },
        { "{\"type\":\"map\", \"values\": \"float\"}", "{c1sSF}",
          "{\"type\":\"map\", \"values\": \"double\"}", "{c1sSD}" },
        { "{\"type\":\"map\", \"values\": \"double\"}", "{c1sSD}",
          "{\"type\":\"map\", \"values\": \"double\"}", "{c1sSD}" },
        { "{\"type\":\"map\", \"values\": \"string\"}", "{c1sSS}",
          "{\"type\":\"map\", \"values\": \"string\"}", "{c1sSS}" },
        { "{\"type\":\"map\", \"values\": \"bytes\"}", "{c1sSb}",
          "{\"type\":\"map\", \"values\": \"bytes\"}", "{c1sSb}" },
        { "{\"type\":\"map\", \"values\": "
          + "{\"type\":\"array\", \"items\":\"int\"}}", "{c1sS[c3sIsIsI]}",
          "{\"type\":\"map\", \"values\": "
          + "{\"type\":\"array\", \"items\":\"int\"}}", "{c1sS[c3sIsIsI]}" },

        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"boolean\"}]}", "B",
          "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"boolean\"}]}", "B" },
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"int\"}]}", "I",
          "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"int\"}]}", "I"},
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"int\"}]}", "I",
          "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"long\"}]}", "L"},
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"int\"}]}", "I",
          "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"double\"}]}", "D"},
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"long\"}]}", "L",
          "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"long\"}]}", "L" },
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"float\"}]}", "F",
          "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"float\"}]}", "F"},
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"double\"}]}", "D",
          "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"double\"}]}", "D"},
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"string\"}]}", "S",
          "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"string\"}]}", "S"},
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"bytes\"}]}", "b",
          "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"bytes\"}]}", "b"},

        // multi-field record
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f1\", \"type\":\"int\"},"
          + "{\"name\":\"f2\", \"type\":\"double\"},"
          + "{\"name\":\"f3\", \"type\":\"string\"}]}", "IDS",
          "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f1\", \"type\":\"int\"},"
          + "{\"name\":\"f2\", \"type\":\"double\"},"
          + "{\"name\":\"f3\", \"type\":\"string\"}]}", "IDS" },
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

        // record with array
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f1\", \"type\":\"long\"},"
          + "{\"name\":\"f2\", "
          + "\"type\":{\"type\":\"array\", \"items\":\"int\"}}]}", "L[c1sI]",
          "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f1\", \"type\":\"long\"},"
          + "{\"name\":\"f2\", "
          + "\"type\":{\"type\":\"array\", \"items\":\"int\"}}]}", "L[c1sI]" },

        // record with map
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f1\", \"type\":\"long\"},"
          + "{\"name\":\"f2\", "
          + "\"type\":{\"type\":\"map\", \"values\":\"int\"}}]}", "L{c1sSI}",
          "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f1\", \"type\":\"long\"},"
          + "{\"name\":\"f2\", "
          + "\"type\":{\"type\":\"map\", \"values\":\"int\"}}]}", "L{c1sSI}" },

        { "[\"boolean\"]", "u0B", "[\"boolean\"]", "u0B" },
        { "[\"int\"]", "u0I", "[\"int\"]", "u0I" },
        { "[\"int\"]", "u0I", "[\"long\"]", "u0L" },
        { "[\"int\"]", "u0I", "[\"double\"]", "u0D" },
        { "[\"long\"]", "u0L", "[\"long\"]", "u0L" },
        { "[\"long\"]", "u0L", "[\"double\"]", "u0D" },
        { "[\"float\"]", "u0F", "[\"float\"]", "u0F" },
        { "[\"float\"]", "u0F", "[\"double\"]", "u0D" },
        { "[\"double\"]", "u0D", "[\"double\"]", "u0D" },
        { "[\"string\"]", "u0S", "[\"string\"]", "u0S" },
        { "[\"bytes\"]", "u0b", "[\"bytes\"]", "u0b" },

        { "\"int\"", "I", "[\"int\"]", "u0I" },

        { "[\"int\"]", "u0I", "\"int\"", "I" },
        { "[\"int\"]", "u0I", "\"long\"", "I" },

        { "[\"null\", \"int\"]", "u0N", "[\"null\", \"int\"]", "u0N" },
        { "[\"boolean\", \"int\"]", "u0B", "[\"boolean\", \"int\"]", "u0B" },
        { "[\"boolean\", \"int\"]", "u1I", "[\"boolean\", \"int\"]", "u1I" },
        { "[\"boolean\", \"int\"]", "u1I", "[\"int\", \"boolean\"]", "u0I" },
        { "[\"boolean\", \"int\"]", "u1I", "[\"boolean\", \"long\"]", "u1L" },
        { "[\"boolean\", \"int\"]", "u1I", "[\"long\", \"boolean\"]", "u0L" },
        { "[\"boolean\", {\"type\":\"array\", \"items\":\"int\"} ]", "u0B",
          "[\"boolean\", {\"type\":\"array\", \"items\":\"int\"} ]", "u0B" },
        { "[\"boolean\", {\"type\":\"array\", \"items\":\"int\"} ]", "u1[c1sI]",
          "[\"boolean\", {\"type\":\"array\", \"items\":\"int\"} ]", "u1[c1sI]" },

          // TODO: test more complicated types such as arrays of records
    };
  }
  
  protected static void dump(byte[] bb) {
    int col = 0;
    for (byte b : bb) {
      if (col % 16 == 0) {
        System.out.println();
      }
      col++;
      System.out.print(Integer.toHexString(b & 0xff) + " ");
    }
    System.out.println();
  }

}
