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

import org.junit.Test;
import org.junit.runners.Parameterized;
import org.junit.runner.RunWith;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Collection;
import java.util.Arrays;

@RunWith(Parameterized.class)
public class TestResolvingIOResolving {
  protected TestValidatingIO.Encoding eEnc;
  protected final int iSkipL;
  protected final String sJsWrtSchm;
  protected final String sWrtCls;
  protected final String sJsRdrSchm;
  protected final String sRdrCls;

  protected final Object[] oaWrtVals;
  protected final Object[] oaRdrVals;

  public TestResolvingIOResolving(TestValidatingIO.Encoding encoding,
      int skipLevel, String jsonWriterSchema,
      String writerCalls,
      Object[] writerValues,
      String jsonReaderSchema, String readerCalls,
      Object[] readerValues
  ) {
    this.eEnc = encoding;
    this.iSkipL = skipLevel;
    this.sJsWrtSchm = jsonWriterSchema;
    this.sWrtCls = writerCalls;
    this.oaWrtVals = writerValues;
    this.sJsRdrSchm = jsonReaderSchema;
    this.sRdrCls = readerCalls;
    this.oaRdrVals = readerValues;
  }

  @Test
  public void testResolving()
    throws IOException {
    Schema writerSchema = Schema.parse(sJsWrtSchm);
    byte[] bytes = TestValidatingIO.make(writerSchema, sWrtCls,
        oaWrtVals, TestValidatingIO.Encoding.BINARY);
    Schema readerSchema = Schema.parse(sJsRdrSchm);
    TestResolvingIO.check(writerSchema, readerSchema, bytes, sRdrCls,
        oaRdrVals,
        TestValidatingIO.Encoding.BINARY, iSkipL);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data3() {
    Collection<Object[]> ret = Arrays.asList(
                TestValidatingIO.convertTo2dArray(TestResolvingIO.encodings,
                                TestResolvingIO.skipLevels,
        dataForResolvingTests()));
    return ret;
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
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":[]}", "",
          new Object[] { },
          "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":{ \"type\": \"array\", \"items\": \"int\" }, "
            + "\"default\": [100]}]}", "[c1sI]",
          new Object[] { 100 } },
        { "{ \"type\": \"array\", \"items\": {\"type\":\"record\","
            + "\"name\":\"r\",\"fields\":[]} }", "[c1s]",
            new Object[] { },
          "{ \"type\": \"array\", \"items\": {\"type\":\"record\","
            + "\"name\":\"r\",\"fields\":["
            + "{\"name\":\"f\", \"type\":\"int\", \"default\": 100}]} }",
            "[c1sI]",
          new Object[] { 100 } },
        // Enum resolution
        { "{\"type\":\"enum\",\"name\":\"e\",\"symbols\":[\"x\",\"y\",\"z\"]}",
            "e2",
            new Object[] {  },
            "{\"type\":\"enum\",\"name\":\"e\",\"symbols\":[ \"y\", \"z\" ]}",
            "e1",
            new Object[] {  } },
        { "{\"type\":\"enum\",\"name\":\"e\",\"symbols\":[ \"x\", \"y\" ]}",
            "e1",
            new Object[] {  },
            "{\"type\":\"enum\",\"name\":\"e\",\"symbols\":[ \"y\", \"z\" ]}",
            "e0",
            new Object[] {  } },

        // Union
        { "\"int\"", "I", new Object[] { 100 },
            "[ \"long\", \"int\"]", "U1I", new Object[] { 100 } },
        { "[ \"long\", \"int\"]", "U1I", new Object[] { 100 } ,
            "\"int\"", "I", new Object[] { 100 } },
        // Union + promotion
        { "\"int\"", "I", new Object[] { 100 },
            "[ \"long\", \"string\"]", "U0L", new Object[] { 100L } },
        { "[ \"int\", \"string\"]", "U0I", new Object[] { 100 },
            "\"long\"", "L", new Object[] { 100 } },
    };
  }
}
