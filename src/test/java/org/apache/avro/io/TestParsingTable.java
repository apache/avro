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

import java.util.BitSet;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestParsingTable {
  @Test(dataProvider="data")
  public void test(String jsonSchema, int startSymbol,
      int[][] nonTerminals,
      int[][] productions) {
    BitSet bs = new BitSet();
    ParsingTable t = new ParsingTable(Schema.parse(jsonSchema));
    t.toString();
    if (t.isTerminal(t.root)) {
      Assert.assertEquals(startSymbol, t.root);
    } else {
      checkSymbol(startSymbol, t, t.root, bs, nonTerminals, productions);
    }
  }
  
  private void checkSymbol(int n, ParsingTable t, int node, BitSet bs,
      int[][] nonTerminals, int[][] productions) {
    if (t.isTerminal(node)) {
      Assert.assertEquals(n, node);
    } else {
      if (! bs.get(n)) {
        bs.set(n);
        if (t.isUnion(node)) {
          Assert.assertEquals(nonTerminals[n].length, t.size(node));
          for (int i = 0; i < nonTerminals[n].length; i++) {
            checkSymbol(nonTerminals[n][i], t, t.getBranch(node, i), bs,
                nonTerminals, productions);
          }
        } else {
          Assert.assertEquals(1, nonTerminals[n].length);
          checkProduction(productions[nonTerminals[n][0]], t, node, bs,
              nonTerminals, productions);
        }
      }
    }
  }

  private void checkProduction(int[] n, ParsingTable t, int sym, BitSet bs,
      int[][] nonTerminals, int[][] productions) {
    Assert.assertTrue(t.isNonTerminal(sym) || t.isRepeater(sym) || t.isUnion(sym));
    Assert.assertEquals(n.length, t.size(sym));
    for (int i = 0; i < n.length; i++) {
      checkSymbol(n[i], t, t.prods[i + sym], bs, nonTerminals, productions);
    }
  }

  @DataProvider
  public static Object[][] data() {
    return new Object[][] {
        // primitives
        makePrimitiveTestData(ParsingTable.NULL),
        makePrimitiveTestData(ParsingTable.BOOLEAN),
        makePrimitiveTestData(ParsingTable.INT),
        makePrimitiveTestData(ParsingTable.LONG),
        makePrimitiveTestData(ParsingTable.FLOAT),
        makePrimitiveTestData(ParsingTable.DOUBLE),
        makePrimitiveTestData(ParsingTable.STRING),
        makePrimitiveTestData(ParsingTable.BYTES),

        // Simple arrays
        makeArrayTestData(ParsingTable.BOOLEAN),
        makeArrayTestData(ParsingTable.INT),
        makeArrayTestData(ParsingTable.LONG),
        makeArrayTestData(ParsingTable.FLOAT),
        makeArrayTestData(ParsingTable.DOUBLE),
        makeArrayTestData(ParsingTable.STRING),
        makeArrayTestData(ParsingTable.BYTES),

        // Simple maps
        makeMapTestData(ParsingTable.BOOLEAN),
        makeMapTestData(ParsingTable.INT),
        makeMapTestData(ParsingTable.LONG),
        makeMapTestData(ParsingTable.FLOAT),
        makeMapTestData(ParsingTable.DOUBLE),
        makeMapTestData(ParsingTable.STRING),
        makeMapTestData(ParsingTable.BYTES),
        
        // Simple records
        makeRecordTestData(ParsingTable.BOOLEAN),
        makeRecordTestData(ParsingTable.INT),
        makeRecordTestData(ParsingTable.LONG),
        makeRecordTestData(ParsingTable.FLOAT),
        makeRecordTestData(ParsingTable.DOUBLE),
        makeRecordTestData(ParsingTable.STRING),
        makeRecordTestData(ParsingTable.BYTES),
        makeRecordTestData(ParsingTable.BOOLEAN, ParsingTable.BOOLEAN),
        makeRecordTestData(ParsingTable.BOOLEAN, ParsingTable.INT,
            ParsingTable.LONG, ParsingTable.FLOAT, ParsingTable.DOUBLE,
            ParsingTable.STRING, ParsingTable.BYTES),

        // Simple unions
        makeUnionTestData(ParsingTable.BOOLEAN),
        makeUnionTestData(ParsingTable.INT),
        makeUnionTestData(ParsingTable.LONG),
        makeUnionTestData(ParsingTable.FLOAT),
        makeUnionTestData(ParsingTable.DOUBLE),
        makeUnionTestData(ParsingTable.STRING),
        makeUnionTestData(ParsingTable.BYTES),
        makeUnionTestData(ParsingTable.BOOLEAN, ParsingTable.INT),
        makeUnionTestData(ParsingTable.BOOLEAN, ParsingTable.INT,
            ParsingTable.LONG, ParsingTable.FLOAT, ParsingTable.DOUBLE,
            ParsingTable.STRING, ParsingTable.BYTES),
            
        // TODO: test complex structures (map of arrays, arrays of records etc.)
    };
  }

  private static Object[] makePrimitiveTestData(int sym) {
    return new Object[] { "\"" + ParsingTable.getTerminalName(sym) + "\"",
        sym, null, null };
  }

  private static Object[] makeArrayTestData(int sym) {
    return new Object[] {
      "{\"type\":\"array\", \"items\": \"" +
        ParsingTable.getTerminalName(sym) + "\"}",
      0, new int[][] { { 0 }, { 1 } },
      new int[][] {
        { ParsingTable.ARRAYEND, 1, ParsingTable.ARRAYSTART },
        { 1, sym },
        },
    };
  }

  private static Object[] makeMapTestData(int sym) {
    return new Object[] {
        "{\"type\":\"map\", \"values\": \"" +
        ParsingTable.getTerminalName(sym) + "\"}",
      0, new int[][] { { 0 }, { 1 } },
      new int[][] {
        { ParsingTable.MAPEND, 1, ParsingTable.MAPSTART },
        { 1, sym, ParsingTable.STRING },
      },
    };
  }

  private static Object[] makeRecordTestData(int ... syms) {
    return new Object[] {
        getRecordSchema(syms),
        0,
        new int[][] { { 0 } },
        new int[][] { reverse(syms) },
    };
  }

  private static int[] reverse(int[] syms) {
    int[] result = new int[syms.length];
    for (int i = 0; i < syms.length; i++) {
      result[i] = syms[syms.length - 1 - i];
    }
    return result;
  }

  private static Object[] makeUnionTestData(int ... syms) {
    return new Object[] {
        getUnionSchema(syms),
        0,
        new int[][] { { 0 }, syms },
        new int[][] { { 1, ParsingTable.UNION }},
    };
  }

  private static String getRecordSchema(int[] syms) {
    StringBuffer sb = new StringBuffer();
    sb.append("{\"type\":\"record\", \"name\":\"rec\", \"fields\":[");
    for (int i = 0; i < syms.length; i++) {
      if (i != 0) {
        sb.append(",");
      }
      sb.append("{\"name\":\"f");
      sb.append(i);
      sb.append("\", \"type\":\"");
      sb.append(ParsingTable.getTerminalName(syms[i]));
      sb.append("\"}");
    }
      
    sb.append("]}");
    return sb.toString();
  }

  private static String getUnionSchema(int[] syms) {
    StringBuffer sb = new StringBuffer();
    sb.append("[");
    for (int i = 0; i < syms.length; i++) {
      if (i != 0) {
        sb.append(",");
      }
      sb.append("\"");
      sb.append(ParsingTable.getTerminalName(syms[i]));
      sb.append("\"");
    }
      
    sb.append("]");
    return sb.toString();
  }
}
