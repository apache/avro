package org.apache.avro.io;

import org.junit.Test;
import org.junit.runners.Parameterized;
import org.junit.runner.RunWith;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Collection;
import java.util.Arrays;

@RunWith(Parameterized.class)
public class TestResolvingIO_resolving {
  protected TestValidatingIO.Encoding eEnc;
  protected final int iSkipL;
  protected final String sJsWrtSchm;
  protected final String sWrtCls;
  protected final String sJsRdrSchm;
  protected final String sRdrCls;

  protected final Object[] oaWrtVals;
  protected final Object[] oaRdrVals;

  public TestResolvingIO_resolving(TestValidatingIO.Encoding encoding,
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
  public void test_resolving()
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
    };
  }
}
