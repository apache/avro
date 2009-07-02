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
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Vector;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestValidatingIO {
  @Test(dataProvider="data")
  public void testRead(String jsonSchema, String calls) throws IOException {
    test(jsonSchema, calls, -1, false);
  }

  @Test(dataProvider="data")
  public void testSkip_0(String jsonSchema, String calls) throws IOException {
    test(jsonSchema, calls, 0, false);
  }
  
  @Test(dataProvider="data")
  public void testSkip_1(String jsonSchema, String calls) throws IOException {
    test(jsonSchema, calls, 1, false);
  }
  
  @Test(dataProvider="data")
  public void testSkip_2(String jsonSchema, String calls) throws IOException {
    test(jsonSchema, calls, 2, false);
  }
  
  @Test(dataProvider="data")
  public void testRead_blocking(String jsonSchema, String calls)
    throws IOException {
    test(jsonSchema, calls, -1, true);
  }

  @Test(dataProvider="data")
  public void testSkip_0_blocking(String jsonSchema, String calls)
    throws IOException {
    test(jsonSchema, calls, 0, true);
  }
  
  @Test(dataProvider="data")
  public void testSkip_1_blocking(String jsonSchema, String calls)
    throws IOException {
    test(jsonSchema, calls, 1, true);
  }
  
  @Test(dataProvider="data")
  public void testSkip_2_blocking(String jsonSchema, String calls)
    throws IOException {
    test(jsonSchema, calls, 2, true);
  }
  
  private void test(String jsonSchema, String calls, int skipLevel,
      boolean useBlocking)
    throws IOException {
    for (int i = 0; i < 10; i++) {
      testOnce(Schema.parse(jsonSchema), calls, skipLevel, useBlocking);
    }
  }
  
  private void testOnce(Schema schema, String calls, int skipLevel,
      boolean useBlocking)
    throws IOException {
    Vector<Object> values = new Vector<Object>();
    byte[] bytes = make(schema, calls, values, useBlocking);
    check(schema, bytes, calls, values.toArray(), skipLevel);
  }

  public static byte[] make(Schema sc, String calls,
      Vector<Object> values, boolean useBlocking) throws IOException {
    ByteArrayOutputStream ba = new ByteArrayOutputStream();
    Encoder bvo = useBlocking ? new BlockingBinaryEncoder(ba) :
        new BinaryEncoder(ba);
    Encoder vo = new ValidatingEncoder(sc, bvo);
    generate(vo, calls, values);
    vo.flush();
    return ba.toByteArray();
  }

  public static void generate(Encoder vw, String calls,
      Vector<Object> values) throws IOException {
    char[] cc = calls.toCharArray();
    Random r = new Random();
    for (int i = 0; i < cc.length; i++) {
      char c = cc[i];
      switch (c) {
      case 'N':
        vw.writeNull();
        break;
      case 'B':
        boolean b = r.nextBoolean();
        values.add(b);
        vw.writeBoolean(b);
        break;
      case 'I':
        int ii = r.nextInt();
        values.add(ii);
        vw.writeInt(ii);
        break;
      case 'L':
        long l = r.nextLong();
        values.add(l);
        vw.writeLong(l);
        break;
      case 'F':
        float f = r.nextFloat();
        values.add(f);
        vw.writeFloat(f);
        break;
      case 'D':
        double d = r.nextDouble();
        values.add(d);
        vw.writeDouble(d);
        break;
      case 'S':
        {
          IntPair ip = extractInt(cc, i);
          String s = nextString(r, ip.first);
          values.add(s);
          vw.writeString(new Utf8(s));
          i = ip.second;
          break;
        }
      case 'b':
        {
          IntPair ip = extractInt(cc, i);
          byte[] bb = nextBytes(r, ip.first);
          values.add(bb);
          vw.writeBytes(bb);
          i = ip.second;
          break;
        }
      case 'f':
        {
          IntPair ip = extractInt(cc, i);
          byte[] bb = nextBytes(r, ip.first);
          values.add(bb);
          vw.writeFixed(bb);
          i = ip.second;
          break;
        }
      case 'e':
        {
          IntPair ip = extractInt(cc, i);
          int e = r.nextInt(ip.first);
          values.add(e);
          vw.writeEnum(e);
          i = ip.second;
          break;
        }
      case '[':
        vw.writeArrayStart();
        break;
      case ']':
        vw.writeArrayEnd();
        break;
      case '{':
        vw.writeMapStart();
        break;
      case '}':
        vw.writeMapEnd();
        break;
      case 'c':
        {
          IntPair ip = extractInt(cc, i);
          vw.setItemCount(ip.first);
          i = ip.second;
        }
        break;
      case 's':
        vw.startItem();
        break;
      case 'u':
        {
          IntPair ip = extractInt(cc, i);
          vw.writeIndex(ip.first);
          i = ip.second;
          break;
        }
      default:
        Assert.fail();
        break;
      }
    }
  }

  private static IntPair extractInt(char[] cc, int i) {
    int r = 0;
    i++;
    while (i < cc.length && Character.isDigit(cc[i])) {
      r = r * 10 + cc[i] - '0';
      i++;
    }
    return new IntPair(r, i - 1);
  }

  private static class IntPair {
    public final int first;
    public final int second;
    
    public IntPair(int first, int second) {
      this.first = first;
      this.second = second;
    }
  }
  private static byte[] nextBytes(Random r, int length) {
    byte[] bb = new byte[length];
    r.nextBytes(bb);
    return bb;
  }

  private static String nextString(Random r, int length) {
    char[] cc = new char[length]; 
    for (int i = 0; i < length; i++) {
      cc[i] = (char) ('A' + r.nextInt(26));
    }
    return new String(cc);
  }

  private static void check(Schema sc, byte[] bytes,
      String calls, Object[] values, final int skipLevel) throws IOException {
    // dump(bytes);
    Decoder bvi= new BinaryDecoder(new ByteArrayInputStream(bytes));
    Decoder vi = new ValidatingDecoder(sc, bvi);
    check(vi, calls, values, skipLevel);
  }
  
  public static void check(Decoder vi, String calls, Object[] values,
      final int skipLevel) throws IOException {
    char[] cc = calls.toCharArray();
    int p = 0;
    int level = 0;
    long[] counts = new long[100];
    boolean[] isArray = new boolean[100];
    boolean[] isEmpty = new boolean[100];
    for (int i = 0; i < cc.length; i++) {
      final char c = cc[i];
      switch (c) {
      case 'N':
        vi.readNull();
        break;
      case 'B':
        boolean b = ((Boolean) values[p++]).booleanValue(); 
        Assert.assertEquals(vi.readBoolean(), b);
        break;
      case 'I':
        int ii = ((Integer) values[p++]).intValue(); 
        Assert.assertEquals(vi.readInt(), ii);
        break;
      case 'L':
        long l = longValue(values[p++]); 
        Assert.assertEquals(vi.readLong(), l);
        break;
      case 'F':
        float f = ((Float) values[p++]).floatValue(); 
        Assert.assertEquals(vi.readFloat(), f, Math.abs(f / 1000));
        break;
      case 'D':
        double d = doubleValue(values[p++]); 
        Assert.assertEquals(vi.readDouble(), d, Math.abs(d / 1000));
        break;
      case 'S':
        i = extractInt(cc, i).second;
        if (level == skipLevel) {
          vi.skipString();
          p++;
        } else {
          String s = (String) values[p++]; 
          Assert.assertEquals(vi.readString(null), new Utf8(s));
        }
        break;
      case 'b':
        i = extractInt(cc, i).second;
        if (level == skipLevel) {
          vi.skipBytes();
          p++;
        } else {
          byte[] bb = (byte[]) values[p++];
          ByteBuffer bb2 = vi.readBytes(null);
          byte[] actBytes = new byte[bb2.remaining()];
          System.arraycopy(bb2.array(), bb2.position(), actBytes,
              0, bb2.remaining());
          Assert.assertEquals(actBytes, bb);
        }
        break;
      case 'f':
        {
          IntPair ip = extractInt(cc, i);
          i = ip.second;
          if (level == skipLevel) {
            vi.skipFixed(ip.first);
            p++;
          } else {
            byte[] bb = (byte[]) values[p++];
            byte[] actBytes = new byte[ip.first];
            vi.readFixed(actBytes);
            Assert.assertEquals(actBytes, bb);
          }
        }
        break;
      case 'e':
      {
        IntPair ip = extractInt(cc, i);
        i = ip.second;
        if (level == skipLevel) {
          vi.readEnum();
          p++;
        } else {
          int e = ((Integer)(values[p++])).intValue();
          Assert.assertEquals(vi.readEnum(), e);
        }
      }
      break;
      case '[':
        if (level == skipLevel) {
          IntPair ip = skip(cc, i, vi);
          i = ip.second;
          p += ip.first;
          break;
        } else {
          level++;
          counts[level] = vi.readArrayStart();
          isArray[level] = true;
          isEmpty[level] = counts[level] == 0;
          continue;
        }
      case '{':
        if (level == skipLevel) {
          IntPair ip = skip(cc, i, vi);
          i = ip.second;
          p += ip.first;
          break;
        } else {
          level++;
          counts[level] = vi.readMapStart();
          isArray[level] = false;
          isEmpty[level] = counts[level] == 0;
          continue;
        }
      case ']':
        Assert.assertEquals(counts[level], 0);
        if (! isEmpty[level]) {
          Assert.assertEquals(vi.arrayNext(), 0);
        }
        level--;
        break;
      case '}':
        Assert.assertEquals(counts[level], 0);
        if (! isEmpty[level]) {
          Assert.assertEquals(vi.mapNext(), 0);
        }
        level--;
        break;
      case 's':
        if (counts[level] == 0) {
          if (isArray[level]) {
            counts[level] = vi.arrayNext();
          } else {
            counts[level] = vi.mapNext();
          }
        }
        counts[level]--;
        continue;
      case 'c':
        i = extractInt(cc, i).second;
        continue;
      case 'u':
        {
          IntPair ip = extractInt(cc, i);
          Assert.assertEquals(ip.first, vi.readIndex());
          i = ip.second;
          continue;
        }
      default:
        Assert.fail();
      }
    }
    Assert.assertEquals(p, values.length);
  }
  
  private static double doubleValue(Object object) {
    return (object instanceof Double) ? ((Double) object).doubleValue() :
      (object instanceof Float) ? ((Float) object).doubleValue() :
      (object instanceof Long) ? ((Long) object).doubleValue() :
      ((Integer) object).doubleValue();
  }

  private static long longValue(Object object) {
    return (object instanceof Long) ? ((Long) object).longValue() :
      (object instanceof Double) ? ((Double) object).longValue() :
      ((Integer) object).longValue();
  }

  private static IntPair skip(char[] cc, int i, Decoder vi)
    throws IOException {
    char end = cc[i] == '[' ? ']' : '}';
    if (end == ']') {
      Assert.assertEquals(vi.skipArray(), 0);
    } else {
      Assert.assertEquals(vi.skipMap(), 0);
    }
    int level = 0;
    int p = 0;
    while (++i < cc.length) {
      switch (cc[i]) {
      case '[':
      case '{':
        ++level;
        break;
      case ']':
      case '}':
        if (cc[i] == end && level == 0) {
          return new IntPair(p, i);
        }
        level--;
        break;
      case 'B':
      case 'I':
      case 'L':
      case 'F':
      case 'D':
      case 'S':
      case 'b':
      case 'f':
      case 'e':
        p++;
        break;
      }
    }
    return null;
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
     * S followed by integer - string and its size
     * b followed by integer - bytes and size
     * u followed by integer - union and the index to choose
     * c  Number of items to follow in an array/map.
     * [  Start array
     * ]  End array
     * {  Start map
     * }  End map
     * s  start item
     */
    return new Object[][] {
        { "\"null\"", "N" },
        { "\"boolean\"", "B" },
        { "\"int\"", "I" },
        { "\"long\"", "L" },
        { "\"float\"", "F" },
        { "\"double\"", "D" },
        { "\"string\"", "S0" },
        { "\"string\"", "S10" },
        { "\"bytes\"", "b0" },
        { "\"bytes\"", "b10" },
        { "{\"type\":\"fixed\", \"name\":\"fi\", \"size\": 0}", "f0" },
        { "{\"type\":\"fixed\", \"name\":\"fi\", \"size\": 10}", "f10" },
        { "{\"type\":\"enum\", \"name\":\"en\", \"symbols\":[\"v1\", \"v2\"]}",
            "e2" },

        { "{\"type\":\"array\", \"items\": \"boolean\"}", "[]" },
        { "{\"type\":\"array\", \"items\": \"int\"}", "[]" },
        { "{\"type\":\"array\", \"items\": \"long\"}", "[]" },
        { "{\"type\":\"array\", \"items\": \"float\"}", "[]" },
        { "{\"type\":\"array\", \"items\": \"double\"}", "[]" },
        { "{\"type\":\"array\", \"items\": \"string\"}", "[]" },
        { "{\"type\":\"array\", \"items\": \"bytes\"}", "[]" },
        { "{\"type\":\"array\", \"items\":{\"type\":\"fixed\", "
          + "\"name\":\"fi\", \"size\": 10}}", "[]" },

        { "{\"type\":\"array\", \"items\": \"boolean\"}", "[c1sB]" },
        { "{\"type\":\"array\", \"items\": \"int\"}", "[c1sI]" },
        { "{\"type\":\"array\", \"items\": \"long\"}", "[c1sL]" },
        { "{\"type\":\"array\", \"items\": \"float\"}", "[c1sF]" },
        { "{\"type\":\"array\", \"items\": \"double\"}", "[c1sD]" },
        { "{\"type\":\"array\", \"items\": \"string\"}", "[c1sS10]" },
        { "{\"type\":\"array\", \"items\": \"bytes\"}", "[c1sb10]" },
        { "{\"type\":\"array\", \"items\": \"int\"}", "[c1sIc1sI]" },
        { "{\"type\":\"array\", \"items\": \"int\"}", "[c2sIsI]" },
        { "{\"type\":\"array\", \"items\":{\"type\":\"fixed\", "
          + "\"name\":\"fi\", \"size\": 10}}", "[c2sf10sf10]" },

        { "{\"type\":\"map\", \"values\": \"boolean\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"int\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"long\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"float\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"double\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"string\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"bytes\"}", "{}" },
        { "{\"type\":\"map\", \"values\": "
          + "{\"type\":\"array\", \"items\":\"int\"}}", "{}" },

        { "{\"type\":\"map\", \"values\": \"boolean\"}", "{c1sSB}" },
        { "{\"type\":\"map\", \"values\": \"int\"}", "{c1sS5I}" },
        { "{\"type\":\"map\", \"values\": \"long\"}", "{c1sS5L}" },
        { "{\"type\":\"map\", \"values\": \"float\"}", "{c1sS5F}" },
        { "{\"type\":\"map\", \"values\": \"double\"}", "{c1sS5D}" },
        { "{\"type\":\"map\", \"values\": \"string\"}", "{c1sS5S10}" },
        { "{\"type\":\"map\", \"values\": \"bytes\"}", "{c1sS5b10}" },
        { "{\"type\":\"map\", \"values\": "
          + "{\"type\":\"array\", \"items\":\"int\"}}", "{c1sS[c3sIsIsI]}" },

        { "{\"type\":\"map\", \"values\": \"boolean\"}",
            "{c1sSBc2sSBsSB}" },

        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"boolean\"}]}", "B" },
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"int\"}]}", "I" },
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"long\"}]}", "L" },
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"float\"}]}", "F" },
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"double\"}]}", "D" },
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"string\"}]}", "S10" },
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f\", \"type\":\"bytes\"}]}", "b10" },

        // multi-field records
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f1\", \"type\":\"int\"},"
          + "{\"name\":\"f2\", \"type\":\"double\"},"
          + "{\"name\":\"f3\", \"type\":\"string\"}]}", "IDS10" },
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f0\", \"type\":\"null\"},"
          + "{\"name\":\"f1\", \"type\":\"boolean\"},"
          + "{\"name\":\"f2\", \"type\":\"int\"},"
          + "{\"name\":\"f3\", \"type\":\"long\"},"
          + "{\"name\":\"f4\", \"type\":\"float\"},"
          + "{\"name\":\"f5\", \"type\":\"double\"},"
          + "{\"name\":\"f6\", \"type\":\"string\"},"
          + "{\"name\":\"f7\", \"type\":\"bytes\"}]}",
            "NBILFDS10b25" },

        // record of records
        { "{\"type\":\"record\",\"name\":\"outer\",\"fields\":["
          + "{\"name\":\"f1\", \"type\":{\"type\":\"record\", "
          + "\"name\":\"inner\", \"fields\":["
          + "{\"name\":\"g1\", \"type\":\"int\"}, {\"name\":\"g2\", "
          + "\"type\":\"double\"}]}},"
          + "{\"name\":\"f2\", \"type\":\"string\"},"
          + "{\"name\":\"f3\", \"type\":\"inner\"}]}",
          "IDS10ID" },
        // record with array
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f1\", \"type\":\"long\"},"
          + "{\"name\":\"f2\", "
          + "\"type\":{\"type\":\"array\", \"items\":\"int\"}}]}", "L[c1sI]" },

        // record with map
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f1\", \"type\":\"long\"},"
          + "{\"name\":\"f2\", "
          + "\"type\":{\"type\":\"map\", \"values\":\"int\"}}]}", "L{c1sS5I}" },
          
        // array of records
        { "{\"type\":\"array\", \"items\":"
            + "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f1\", \"type\":\"long\"},"
          + "{\"name\":\"f2\", \"type\":\"null\"}]}}",
            "[c2sLNsLN]" },
        { "{\"type\":\"array\", \"items\":"
            + "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f1\", \"type\":\"long\"},"
          + "{\"name\":\"f2\", "
          + "\"type\":{\"type\":\"array\", \"items\":\"int\"}}]}}",
            "[c2sL[c1sI]sL[c2sIsI]]" },
        { "{\"type\":\"array\", \"items\":"
            + "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f1\", \"type\":\"long\"},"
          + "{\"name\":\"f2\", "
          + "\"type\":{\"type\":\"map\", \"values\":\"int\"}}]}}",
            "[c2sL{c1sS5I}sL{c2sS5IsS5I}]" },
        { "{\"type\":\"array\", \"items\":"
            + "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
          + "{\"name\":\"f1\", \"type\":\"long\"},"
          + "{\"name\":\"f2\", "
          + "\"type\":[\"null\", \"int\"]}]}}",
            "[c2sLu0NsLu1I]" },

        { "[\"boolean\"]", "u0B" },
        { "[\"int\"]", "u0I" },
        { "[\"long\"]", "u0L" },
        { "[\"float\"]", "u0F" },
        { "[\"double\"]", "u0D" },
        { "[\"string\"]", "u0S10" },
        { "[\"bytes\"]", "u0b10" },

        { "[\"null\", \"int\"]", "u0N" },
        { "[\"boolean\", \"int\"]", "u0B" },
        { "[\"boolean\", \"int\"]", "u1I" },
        { "[\"boolean\", {\"type\":\"array\", \"items\":\"int\"} ]", "u0B" },
        { "[\"boolean\", {\"type\":\"array\", \"items\":\"int\"} ]",
            "u1[c1sI]" },
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
