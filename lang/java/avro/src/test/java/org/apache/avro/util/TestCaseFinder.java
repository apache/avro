/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.util;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestCaseFinder {

  @Nested
  public static class SimpleCases {

    public static List<Object[]> cases() {
      List<Object[]> result = new ArrayList<>();
      result.add(new Object[] { "", "foo", new Object[][] {} });
      result.add(new Object[] { "<<INPUT a\n<<OUTPUT b", "OUTPUT", new Object[][] { { "a", "b" } } });
      result.add(new Object[] { "<<INPUT a\n<<OUTPUT b\n", "OUTPUT", new Object[][] { { "a", "b" } } });
      result.add(new Object[] { "<<INPUT a\n<<OUTPUT b\n\n", "OUTPUT", new Object[][] { { "a", "b" } } });
      result.add(new Object[] { "<<INPUT a\r<<OUTPUT b", "OUTPUT", new Object[][] { { "a", "b" } } });
      result.add(
          new Object[] { "// This is a test\n<<INPUT a\n\n\n<<OUTPUT b", "OUTPUT", new Object[][] { { "a", "b" } } });
      result.add(new Object[] { "<<INPUT a\n<<OUTPUT\nb\nOUTPUT", "OUTPUT", new Object[][] { { "a", "b" } } });
      result.add(new Object[] { "<<INPUT a\n<<OUTPUT\nb\nOUTPUT", "OUTPUT", new Object[][] { { "a", "b" } } });
      result.add(new Object[] { "<<INPUT a\n<<OUTPUT\nb\n\nOUTPUT", "OUTPUT", new Object[][] { { "a", "b\n" } } });
      result.add(
          new Object[] { "<<INPUT a\n<<OUTPUT\n\n  b  \n\nOUTPUT", "OUTPUT", new Object[][] { { "a", "\n  b  \n" } } });
      result.add(
          new Object[] { "<<INPUT a\n<<O b\n<<INPUT c\n<<O d", "O", new Object[][] { { "a", "b" }, { "c", "d" } } });
      result.add(new Object[] { "<<INPUT a\n<<O b\n<<F z\n<<INPUT c\n<<O d", "O",
          new Object[][] { { "a", "b" }, { "c", "d" } } });
      result.add(new Object[] { "<<INPUT a\n<<O b\n<<F z\n<<INPUT c\n<<O d", "F", new Object[][] { { "a", "z" } } });
      result.add(new Object[] { "<<INPUT a\n<<O b\n<<F z\n<<INPUT\nc\nINPUT\n<<O d\n<<INPUT e", "INPUT",
          new Object[][] { { "a", null }, { "c", null }, { "e", null } } });
      return result;
    }

    @ParameterizedTest
    @MethodSource("cases")
    void output(String input, String label, Object[][] ex) throws Exception {
      List<Object[]> result = new ArrayList<>();
      CaseFinder.find(mk(input), label, result);
      List<Object[]> expectedOutput = Arrays.asList(ex);
      assertTrue(eq(result, expectedOutput), pr(result));
    }
  }

  @Nested
  public class NonParameterized {

    @Test
    void badDocLabel1() throws Exception {
      assertThrows(java.lang.IllegalArgumentException.class, () -> {
        List<Object[]> result = new ArrayList<>();
        CaseFinder.find(mk("<<INPUT blah"), "", result);
      });
    }

    @Test
    void badDocLabel2() throws Exception {
      assertThrows(java.lang.IllegalArgumentException.class, () -> {
        List<Object[]> result = new ArrayList<>();
        CaseFinder.find(mk("<<INPUT blah"), "kill-er", result);
      });
    }

    @Test
    void badSingleLineHeredoc() throws Exception {
      assertThrows(java.io.IOException.class, () -> {
        List<Object[]> result = new ArrayList<>();
        CaseFinder.find(mk("<<INPUTblah"), "foo", result);
      });
    }

    @Test
    void unterminatedHeredoc() throws Exception {
      assertThrows(java.io.IOException.class, () -> {
        List<Object[]> result = new ArrayList<>();
        CaseFinder.find(mk("<<INPUT"), "foo", result);
      });
    }
  }

  private static BufferedReader mk(String s) {
    return new BufferedReader(new StringReader(s));
  }

  private static String pr(List<Object[]> t) {
    StringBuilder b = new StringBuilder();
    b.append("{ ");
    boolean firstTime = true;
    for (Object[] p : t) {
      if (!firstTime)
        b.append(", ");
      else
        firstTime = false;
      b.append("{ \"").append(p[0]).append("\", \"").append(p[1]).append("\" }");
    }
    b.append("}");
    return b.toString();
  }

  private static boolean eq(List<Object[]> l1, List<Object[]> l2) {
    if (l1 == null || l2 == null)
      return l1 == l2;
    if (l1.size() != l2.size())
      return false;
    for (int i = 0; i < l1.size(); i++)
      if (!Arrays.equals(l1.get(i), l2.get(i)))
        return false;
    return true;
  }
}
