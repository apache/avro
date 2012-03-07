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
package org.apache.avro;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Formatter;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.experimental.runners.Enclosed;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.avro.util.CaseFinder;


@RunWith(Enclosed.class)
public class TestSchemaNormalization {

  @RunWith(Parameterized.class)
  public static class TestCanonical {
    String input, expectedOutput;
    public TestCanonical(String i, String o) { input=i; expectedOutput=o; }

    @Parameters public static List<Object[]> cases() throws IOException
    { return CaseFinder.find(data(), "canonical", new ArrayList<Object[]>()); }

    @Test public void testCanonicalization() throws Exception {
      assertEquals(SchemaNormalization.toParsingForm(Schema.parse(input)),
                   expectedOutput);
    }
  }

  @RunWith(Parameterized.class)
  public static class TestFingerprint {
    String input, expectedOutput;
    public TestFingerprint(String i, String o) { input=i; expectedOutput=o; }

    @Parameters public static List<Object[]> cases() throws IOException
    { return CaseFinder.find(data(),"fingerprint",new ArrayList<Object[]>()); }

    @Test public void testCanonicalization() throws Exception {
      Schema s = Schema.parse(input);
      long carefulFP = altFingerprint(SchemaNormalization.toParsingForm(s));
      assertEquals(carefulFP, Long.parseLong(expectedOutput));
      assertEqHex(carefulFP, SchemaNormalization.parsingFingerprint64(s));
    }
  }

  private static String DATA_FILE =
    (System.getProperty("share.dir", "../../../share")
     + "/test/data/schema-tests.txt");

  private static BufferedReader data() throws IOException
  { return new BufferedReader(new FileReader(DATA_FILE)); }

  /** Compute the fingerprint of <i>bytes[s,s+l)</i> using a slow
      algorithm that's an alternative to that implemented in {@link
      SchemaNormalization}.  Algo from Broder93 ("Some applications of Rabin's
      fingerpringint method"). */
  public static long altFingerprint(String s) {
    // In our algorithm, we multiply all inputs by x^64 (which is
    // equivalent to prepending it with a single "1" bit followed
    // by 64 zero bits).  This both deals with the fact that
    // CRCs ignore leading zeros, and also ensures some degree of
    // randomness for small inputs
    try {
      long tmp = altExtend(SchemaNormalization.EMPTY64, 64, ONE,
                           s.getBytes("UTF-8"));
      return altExtend(SchemaNormalization.EMPTY64, 64, tmp, POSTFIX);
    } catch (java.io.UnsupportedEncodingException e)
      { throw new RuntimeException(e); } 
  }

  private static long altExtend(long poly, int degree, long fp, byte[] b) {
    final long overflowBit = 1L<<(64-degree);
    for (int i = 0; i < b.length; i++) {
      for (int j = 1; j < 129; j = j<<1) {
        boolean overflow = (0 != (fp & overflowBit));
        fp >>>= 1;
        if (0 != (j&b[i])) fp |= ONE; // shift in the input bit
        if (overflow) {
          fp ^= poly; // hi-order coeff of poly kills overflow bit
        }
      }
    }
    return fp;
  }

  private static final long ONE = 0x8000000000000000L;
  private static final byte[] POSTFIX = { 0, 0, 0, 0, 0, 0, 0, 0 };

  private static void assertEqHex(long expected, long actual) {
    String m = format("0x%016x != 0x%016x", expected, actual).toString();
    assertTrue(m, expected == actual);
  }

  private static String format(String f, Object... args) {
    return (new Formatter()).format(f, args).toString();
  }
}
