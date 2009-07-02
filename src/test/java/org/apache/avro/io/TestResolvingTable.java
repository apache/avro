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
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestResolvingTable {
  @Test(dataProvider="data")
  public void test(String jsonWriterSchema, String jsonReaderSchema,
      int startSymbol,
      int[][] nonTerminals,
      int[][] productions) {
    BitSet bs = new BitSet();
    ResolvingTable t = new ResolvingTable(Schema.parse(jsonWriterSchema),
        Schema.parse(jsonReaderSchema));
    if (t.isTerminal(t.root)) {
      Assert.assertEquals(startSymbol, t.root);
    } else {
      checkSymbol(startSymbol, t, t.root, bs, nonTerminals, productions);
    }
  }
  
  private void checkSymbol(int n, ResolvingTable t, int node, BitSet bs,
      int[][] nonTerminals, int[][] productions) {
    if (t.isTerminal(node)) {
      Assert.assertEquals(n, node);
    } else {
      if (! bs.get(n)) {
        bs.set(n);
        if (isAction(n, SKIP_ACTION)) {
          Assert.assertTrue(t.isSkipAction(node));
          checkSymbol(n - SKIP_ACTION, t, t.getProductionToSkip(node), bs,
              nonTerminals, productions);
        } else if (isAction(n, FIELD_ACTION)) {
          Assert.assertTrue(t.isFieldAction(node));
        } else if (isAction(n, ERROR_ACTION)) {
          Assert.assertTrue(t.isErrorAction(node));
          Assert.assertNotNull(t.getMessage(node));
        } else if (isAction(n, RESOLVER_ACTION)) {
          Assert.assertTrue(t.isResolverAction(node));
          checkSymbol(n - RESOLVER_ACTION, t, t.getResolverActual(node), bs,
              nonTerminals, productions);
        } else if (isAction(n, READER_UNION_ACTION)) {
          Assert.assertTrue(t.isReaderUnionAction(node));
          checkSymbol(n - READER_UNION_ACTION, t,
              t.getReaderUnionSym(node), bs,
              nonTerminals, productions);
        } else {
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
  }

  private static boolean isAction(int n, int group) {
    return ((((n + 500) / 1000) * 1000) == group);
  }

  private void checkProduction(int[] n, ResolvingTable t, int sym, BitSet bs,
      int[][] nonTerminals, int[][] productions) {
    Assert.assertTrue(t.isNonTerminal(sym) || t.isRepeater(sym) ||
        t.isUnion(sym) || t.isReaderUnionAction(sym) ||
        t.isWriterUnionAction(sym));
    Assert.assertEquals(n.length, t.size(sym));
    for (int i = 0; i < n.length; i++) {
      checkSymbol(n[i], t, t.prods[i + sym], bs, nonTerminals, productions);
    }
  }

  @DataProvider
  public static Object[][] data() {
    return
        concat(primitiveTestData(),
            new Object[][] {

        // identical simple arrays
        makeArrayTestData(ParsingTable.BOOLEAN, ParsingTable.BOOLEAN),
        makeArrayTestData(ParsingTable.INT, ParsingTable.INT),
        makeArrayTestData(ParsingTable.LONG, ParsingTable.LONG),
        makeArrayTestData(ParsingTable.FLOAT, ParsingTable.FLOAT),
        makeArrayTestData(ParsingTable.DOUBLE, ParsingTable.DOUBLE),
        makeArrayTestData(ParsingTable.STRING, ParsingTable.STRING),
        makeArrayTestData(ParsingTable.BYTES, ParsingTable.BYTES),

        // identical simple maps
        makeMapTestData(ParsingTable.BOOLEAN, ParsingTable.BOOLEAN),
        makeMapTestData(ParsingTable.INT, ParsingTable.INT),
        makeMapTestData(ParsingTable.LONG, ParsingTable.LONG),
        makeMapTestData(ParsingTable.FLOAT, ParsingTable.FLOAT),
        makeMapTestData(ParsingTable.DOUBLE, ParsingTable.DOUBLE),
        makeMapTestData(ParsingTable.STRING, ParsingTable.STRING),
        makeMapTestData(ParsingTable.BYTES, ParsingTable.BYTES),

        // identical simple records
        makeRecordTestData(new int[] { 0, ParsingTable.BOOLEAN },
            new int[] { 0, ParsingTable.BOOLEAN }),
        makeRecordTestData(new int[] { 0, ParsingTable.INT },
            new int[] { 0, ParsingTable.INT }),
        makeRecordTestData(new int[] { 0, ParsingTable.LONG },
            new int[] { 0, ParsingTable.LONG }),
        makeRecordTestData(new int[] { 0, ParsingTable.FLOAT },
            new int[] { 0, ParsingTable.FLOAT }),
        makeRecordTestData(new int[] { 0, ParsingTable.DOUBLE },
            new int[] { 0, ParsingTable.DOUBLE }),
        makeRecordTestData(new int[] { 0, ParsingTable.STRING },
            new int[] { 0, ParsingTable.STRING }),
        makeRecordTestData(new int[] { 0, ParsingTable.BYTES },
            new int[] { 0, ParsingTable.BYTES }),
        makeRecordTestData(new int[] { 0, ParsingTable.BOOLEAN,
            1, ParsingTable.INT, 2, ParsingTable.LONG, 3, ParsingTable.FLOAT,
            4, ParsingTable.DOUBLE, 5, ParsingTable.STRING,
            6, ParsingTable.BYTES },
            new int[] { 0, ParsingTable.BOOLEAN, 1, ParsingTable.INT,
            2, ParsingTable.LONG, 3, ParsingTable.FLOAT,
            4, ParsingTable.DOUBLE, 5, ParsingTable.STRING,
            6, ParsingTable.BYTES  }),
            // Excess fields in writer
        makeRecordTestData(new int[] { 0, ParsingTable.BOOLEAN,
            1, ParsingTable.INT, 2, ParsingTable.LONG, 3, ParsingTable.FLOAT,
            4, ParsingTable.DOUBLE, 5, ParsingTable.STRING,
            6, ParsingTable.BYTES },
            new int[] { 0, ParsingTable.BOOLEAN, 2, ParsingTable.LONG,
            4, ParsingTable.DOUBLE, 5, ParsingTable.STRING,
            6, ParsingTable.BYTES }),
        // promotions
        makeRecordTestData(new int[] { 0, ParsingTable.INT },
            new int[] { 0, ParsingTable.LONG }),

        makeUnionTestData(new int[] { ParsingTable.BOOLEAN, },
            new int[] { ParsingTable.BOOLEAN, }),
        makeUnionTestData(new int[] { ParsingTable.INT, },
            new int[] { ParsingTable.INT, }),
        makeUnionTestData(new int[] { ParsingTable.LONG, },
            new int[] { ParsingTable.LONG, }),
        makeUnionTestData(new int[] { ParsingTable.FLOAT, },
            new int[] { ParsingTable.FLOAT, }),
        makeUnionTestData(new int[] { ParsingTable.DOUBLE, },
            new int[] { ParsingTable.DOUBLE, }),
        makeUnionTestData(new int[] { ParsingTable.STRING, },
            new int[] { ParsingTable.STRING, }),
        makeUnionTestData(new int[] { ParsingTable.BYTES, },
            new int[] { ParsingTable.BYTES, }),
        makeUnionTestData(new int[] { ParsingTable.BOOLEAN, ParsingTable.INT },
            new int[] { ParsingTable.BOOLEAN, ParsingTable.INT }),
        makeUnionTestData(
            new int[] { ParsingTable.BOOLEAN, ParsingTable.INT,
                ParsingTable.LONG, ParsingTable.FLOAT, ParsingTable.DOUBLE,
                ParsingTable.STRING,
                ParsingTable.BYTES },
            new int[] { ParsingTable.BOOLEAN, ParsingTable.INT,
                ParsingTable.LONG, ParsingTable.FLOAT, ParsingTable.DOUBLE,
                ParsingTable.STRING, ParsingTable.BYTES }),
                /*
        { "\"int\"", "[\"int\"]", 0,
          new int[][] { { 0 } },
          new int[][] { { READER_UNION_ACTION + Table.INT, 0 } } },    
        { "[\"int\", \"boolean\"]", "\"int\"", 0,
            new int[][] { { 0 } },
            new int[][] { { READER_UNION_ACTION + Table.INT, 0 } } },    
        makeUnionTestData(
            new int[] { Table.BOOLEAN, Table.INT,
                Table.FLOAT, Table.STRING, Table.BYTES },
            new int[] { Table.BOOLEAN, Table.LONG,
                Table.DOUBLE, Table.STRING, Table.BYTES }),
                */
    });
  }

  private static Object[][] concat(Object[][]... parts) {
    int t = 0;
    for (Object[][] p : parts) {
      t += p.length;
    }
    Object[][] result = new Object[t][];
    int pos = 0;
    for (Object[][] p : parts) {
      System.arraycopy(p, 0, result, pos, p.length);
      pos += p.length;
    }
    return result;
  }

  private static Object[][] primitiveTestData() {
    // Primitives
    return concat(
        makePrimitiveTestData(ParsingTable.NULL),
        makePrimitiveTestData(ParsingTable.BOOLEAN),
        makePrimitiveTestData(ParsingTable.INT),
        makePrimitiveTestData(ParsingTable.LONG),
        makePrimitiveTestData(ParsingTable.FLOAT),
        makePrimitiveTestData(ParsingTable.DOUBLE),
        makePrimitiveTestData(ParsingTable.STRING),
        makePrimitiveTestData(ParsingTable.BYTES));
        
  }

  private static Object[][] makePrimitiveTestData(int writeSym) {
    return new Object[][] {
        makePrimitiveTestData(writeSym, ParsingTable.NULL),
        makePrimitiveTestData(writeSym, ParsingTable.BOOLEAN),
        makePrimitiveTestData(writeSym, ParsingTable.INT),
        makePrimitiveTestData(writeSym, ParsingTable.LONG),
        makePrimitiveTestData(writeSym, ParsingTable.FLOAT),
        makePrimitiveTestData(writeSym, ParsingTable.DOUBLE),
        makePrimitiveTestData(writeSym, ParsingTable.STRING),
        makePrimitiveTestData(writeSym, ParsingTable.BYTES),
    };
  }

  private static Object[] makePrimitiveTestData(int writerSym, int readerSym) {
    return new Object[] { "\"" + ParsingTable.getTerminalName(writerSym) + "\"",
        "\"" + ParsingTable.getTerminalName(readerSym) + "\"",
        resolvePrimitive(readerSym, writerSym), null, null };
  }

  private static int resolvePrimitive(int readerSym, int writerSym) {
    if (readerSym == writerSym) {
      return readerSym;
    } else if (readerSym == ParsingTable.LONG) {
      if (writerSym == ParsingTable.INT ||
        writerSym == ParsingTable.DOUBLE ||
        writerSym == ParsingTable.FLOAT) {
        return RESOLVER_ACTION + writerSym;
      }
    } else if (readerSym == ParsingTable.DOUBLE) {
      if (writerSym == ParsingTable.INT || writerSym == ParsingTable.LONG
          || writerSym == ParsingTable.FLOAT) {
        return RESOLVER_ACTION + writerSym;
      }
    }
    return ERROR_ACTION;
  }

  private static Object[] makeArrayTestData(int writerSym, int readerSym) {
    return new Object[] {
      "{\"type\":\"array\", \"items\": \"" +
        ParsingTable.getTerminalName(writerSym)
        + "\"}",
      "{\"type\":\"array\", \"items\": \"" +
        ParsingTable.getTerminalName(readerSym)
        + "\"}",
      0, new int[][] { { 0 }, { 1 } },
      new int[][] {
        { ParsingTable.ARRAYEND, 1, ParsingTable.ARRAYSTART },
        { 1, readerSym },
        },
    };
  }

  private static final int FIELD_ACTION = 1000;
  private static final int SKIP_ACTION = 2000;
  private static final int ERROR_ACTION = 3000;
  private static final int RESOLVER_ACTION = 4000;
  private static final int READER_UNION_ACTION = 5000;
  
  private static Object[] makeMapTestData(int writerSym, int readerSym) {
    return new Object[] {
        "{\"type\":\"map\", \"values\": \"" +
        ParsingTable.getTerminalName(writerSym) + "\"}",
        "{\"type\":\"map\", \"values\": \"" +
        ParsingTable.getTerminalName(readerSym) + "\"}",
      0, new int[][] { { 0 }, { 1 } },
      new int[][] {
        { ParsingTable.MAPEND, 1, ParsingTable.MAPSTART },
        { 1, readerSym, ParsingTable.STRING },
      },
    };
  }

  private static Object[] makeRecordTestData(int[] writerSyms,
      int[] readerSyms) {
    return new Object[] {
        getRecordSchema(writerSyms),
        getRecordSchema(readerSyms),
        0,
        new int[][] { { 0 } },
        new int[][] { makeRecordProduction(writerSyms, readerSyms) },
    };
  }

  private static int[] makeRecordProduction(int[] writerSyms,
      int[] readerSyms)
  {
    Map<Integer, Integer> m = new HashMap<Integer, Integer>();
    int total = 0;
    for (int i = 0; i < readerSyms.length; i += 2) {
      int r = readerSyms[i];
      int v = m.get(r) == null ? 0 : m.get(r);
      m.put(r, v + 1);
      total++;
    }
    for (int i = 0; i < writerSyms.length; i += 2) {
      int w = writerSyms[i];
      int v = m.get(w) == null ? 0 : m.get(w);
      m.put(w, v + 1);
      total++;
    }
    int[] result = new int[total];
    int p = result.length;
    for (int i = 0; i < writerSyms.length; i += 2) {
      int w = writerSyms[i + 1];
      int v = m.get(writerSyms[i]);
      int r = 0;
      for (int j = 0; j < readerSyms.length; j += 2) {
        if (readerSyms[j] == writerSyms[i]) {
          r = readerSyms[j + 1];
          break;
        }
      }
      if (v == 1) {
        result[--p] = SKIP_ACTION + w;
      } else {
        result[--p] = FIELD_ACTION;
        result[--p] = resolvePrimitive(r, w);
      }
    }
    return result;
  }

  private static Object[] makeUnionTestData(int[] writerSyms,
      int[] readerSyms) {
    return new Object[] {
        getUnionSchema(writerSyms),
        getUnionSchema(readerSyms),
        0,
        new int[][] { { 0 }, makeUnionProduction(writerSyms, readerSyms) },
        new int[][] { { 1, ParsingTable.UNION } },
    };
  }

  private static int[] makeUnionProduction(int[] writerSyms,
      int[] readerSyms) {
    int[] result = new int[writerSyms.length];
    int i = 0;
    for (int w : writerSyms) {
      result[i++] = READER_UNION_ACTION + w;
    }
    return result;
  }

  private static String getRecordSchema(int[] syms) {
    StringBuffer sb = new StringBuffer();
    sb.append("{\"type\":\"record\",\"name\":\"n\", \"fields\":[");
    for (int i = 0; i < syms.length; i += 2) {
      if (i != 0) {
        sb.append(",");
      }
      sb.append("{\"name\":\"f");
      sb.append(syms[i]);
      sb.append("\", \"type\":\"");
      sb.append(ParsingTable.getTerminalName(syms[i + 1]));
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
