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

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.AvroTypeException;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * The parsing table.
 *
 */
class ParsingTable {
  /**
   * The terminal symbols for the grammar. Symbols are also index into
   * production table {@link #prods}. Since terminals do not have productions,
   * we can use negative values.
   */
  protected static final int NULL = -1;
  protected static final int BOOLEAN = NULL - 1;
  protected static final int INT = BOOLEAN - 1;
  protected static final int LONG = INT - 1;
  protected static final int FLOAT = LONG - 1;
  protected static final int DOUBLE = FLOAT - 1;
  protected static final int STRING = DOUBLE - 1;
  protected static final int BYTES = STRING - 1;
  protected static final int FIXED = BYTES - 1;
  protected static final int ENUM = FIXED - 1;
  protected static final int UNION = ENUM - 1;

  protected static final int ARRAYSTART = UNION - 1;
  protected static final int ARRAYEND = ARRAYSTART - 1;
  protected static final int MAPSTART = ARRAYEND - 1;
  protected static final int MAPEND = MAPSTART - 1;

  protected static final int LASTPRIM = MAPEND;

  private static final String[] terminalNames = {
    "", "null", "boolean", "int", "long", "float", "double", "string", "bytes",
    "union", "array-start", "array-end", "map-start", "map-end",
  };
  
  protected int root;

  /**
   * If {@link #secondPass} <tt> == false</tt> the number entries in
   * {@link #prods} for non-terminals. Otherwise one past the last entry
   * for non-terminals in {@link #prods}.
   */
  protected int nonTerminals;

  /**
   * If {@link #secondPass} <tt> == false</tt> the number entries in
   * {@link #prods} for repeaters. Otherwise one past the last entry
   * for repeaters in {@link #prods}.
   */
  protected int repeaters;

  /**
   * If {@link #secondPass} <tt> == false</tt> the number entries in
   * {@link #prods} for unions. Otherwise one past the last entry
   * for unions in {@link #prods}.
   */
  protected int unions;
  
  /**
   * If {@link #secondPass} <tt> == false</tt> the number entries in
   * {@link #prods} for fixeds. Otherwise one past the last entry
   * for fixeds in {@link #prods}.
   */
  protected int fixeds;
  
  /**
   * If {@link #secondPass} <tt> == false</tt> the number entries in
   * {@link #prods} for enums. Otherwise one past the last entry
   * for enums in {@link #prods}.
   */
  protected int enums;
  
  /**
   * The productions for non-terminals, repeaters and unions.
   */
  protected int[] prods;

  /**
   * In the two-pass construction of the table, <tt>false</tt> during
   * the first pass and <tt>true</tt> during the second pass.
   */
  protected boolean secondPass = false;

  protected ParsingTable() { }

  public ParsingTable(Schema sc) {
    generate(sc, new HashMap<LitS, Integer>());

    int total = nonTerminals + repeaters + unions + fixeds + enums;
    prods = new int[total];
    enums = total - enums;
    fixeds = enums - fixeds;
    unions = fixeds - unions;
    repeaters = unions - repeaters;
    nonTerminals = repeaters - nonTerminals;

    secondPass = true;
    root = generate(sc, new HashMap<LitS, Integer>());
  }
    
  /**
   * Returns the id for the non-terminal that is the start symbol
   * for the given schema <tt>sc</tt>. If there is already an entry
   * for the given schema in the given map <tt>seen</tt> then
   * the id corresponding to that entry is retutuned. Otherwise
   * a new id is generated and an entry is inserted into the map.
   * @param sc
   * @param seen  A map of schema to id mapping done so far.
   * @return
   */
  protected final int generate(Schema sc, Map<LitS, Integer> seen) {
    switch (sc.getType()) {
    case NULL:
      return NULL;
    case BOOLEAN:
      return BOOLEAN;
    case INT:
      return INT;
    case LONG:
      return LONG;
    case FLOAT:
      return FLOAT;
    case DOUBLE:
      return DOUBLE;
    case STRING:
      return STRING;
    case BYTES:
      return BYTES;
    case FIXED:
      return mkNonTerm(FIXED, mkFixed(sc.getFixedSize()));
    case ENUM:
      return mkNonTerm(ENUM, mkEnum(sc.getEnumSymbols().size()));
    case ARRAY:
      int ar_et = generate(sc.getElementType(), seen);
      int r_et = mkRepeater(ar_et);
      return mkNonTerm(ARRAYSTART, r_et, ARRAYEND);
    case MAP:
      int ar_vt = generate(sc.getValueType(), seen);
      int r_vt = mkRepeater(STRING, ar_vt);
      return mkNonTerm(MAPSTART, r_vt, MAPEND);
    case RECORD:
      LitS wsc = new LitS(sc);
      Integer rresult = seen.get(wsc);
      if (rresult == null) {
        int size = sc.getFields().size();
        rresult = allocNonTerm(size);
        int i = rresult + size;
        for (Field f : sc.getFields().values()) {
          set(--i, generate(f.schema(), seen));
        }
        seen.put(wsc, rresult);
      }
      return rresult;

    case UNION:
      List<Schema> subs = sc.getTypes();
      int u = allocUnion(subs.size());
      for (Schema b : sc.getTypes()) {
        set(u++, generate(b, seen));
      }
      return mkNonTerm(UNION, u - subs.size());

    default:
      throw new RuntimeException("Unexpected schema type");
    }
  }

  protected final int set(int index, int value) {
    if (secondPass) {
      prods[index] = value;
    }
    return value;
  }

  /** Allocates a new non-terminal which in turn uses <tt>len</tt>
   * new non-terminals.
   * Each non-terminal has a unique integer id.
   * @param len Number of non-terminals used by the freshly allocated
   * non-terminal.
   * @return  The id for the new non-terminal allocated.
   */
  protected final int allocNonTerm(int len) {
    set(nonTerminals, len);
    nonTerminals += (len + 1);
    return nonTerminals - len;
  }

  protected final int mkNonTerm(int e1) {
    int nt = nonTerminals;
    set(nt, e1);
    nonTerminals += 2;
    return nt;
  }

  protected final int mkNonTerm(int e1, int e2) {
    set(nonTerminals++, 2);
    set(nonTerminals++, e2);
    set(nonTerminals++, e1);
    return nonTerminals - 2;
  }

  protected final int mkNonTerm(int e1, int e2, int e3) {
    set(nonTerminals++, 3);
    set(nonTerminals++, e3);
    set(nonTerminals++, e2);
    set(nonTerminals++, e1);
    return nonTerminals - 3;
  }

  protected final int mkRepeater(int e1) {
    int i = repeaters;
    set(i++, 2);
    set(i, i) /*recursion*/;
    set(i + 1, e1);
    repeaters += 3;
    return i;
  }

  protected final int mkRepeater(int e1, int e2) {
    int i = repeaters;
    set(i++, 3);
    set(i, i) /* recursion */;
    set(i + 1, e2);
    set(i + 2, e1);
    repeaters += 4;
    return i;
  }

  protected final int allocUnion(int len) {
    set(unions, len);
    unions += (len + 1);
    return unions - len;
  }
  
  protected final int mkFixed(int size) {
    set(fixeds++, 1);
    set(fixeds++, size);
    return fixeds - 1;
  }

  protected final int mkEnum(int max) {
    set(enums++, 1);
    set(enums++, max);
    return enums - 1;
  }

  public final int size(int sym) {
    return prods[sym - 1];
  }

  /**
   * Returns <tt>true</tt> iff the given symbol <tt>sym</tt> is terminal.
   * @param sym The symbol that needs 
   * @return
   */
  public final boolean isTerminal(int sym) {
    return sym < 0;
  }

  public final boolean isNonTerminal(int sym) {
    return 0 <= sym && sym < nonTerminals;
  }

  public final boolean isRepeater(int sym) {
    return nonTerminals <= sym && sym < repeaters;
  }

  public final boolean isUnion(int sym) {
    return repeaters <= sym && sym < unions;
  }
  
  public final boolean isFixed(int sym) {
    return unions <= sym && sym < fixeds;
  }
  
  public final boolean isEnum(int sym) {
    return fixeds <= sym && sym < enums;
  }

  public final int getBranch(int union, int unionIndex) {
    // assert isUnion(union);
    if (unionIndex < 0 || size(union) <= unionIndex) {
       throw new AvroTypeException("Union index out of bounds ("
                                   + unionIndex + ")");
    }
    return prods[union + unionIndex];
  }

  public final int getFixedSize(int sym) {
    return prods[sym];
  }

  public final int getEnumMax(int sym) {
    return prods[sym];
  }

  /** A wrapper around Schema that does "==" equality. */
  protected static class LitS {
    public final Schema actual;
    public LitS(Schema actual) { this.actual = actual; }
    
    /**
     * Two LitS are equal if and only if their underlying schema is
     * the same (not merely equal).
     */
    public boolean equals(Object o) {
      if (! (o instanceof LitS)) return false;
      return actual == ((LitS)o).actual;
    }
    
    public int hashCode() {
      return actual.hashCode();
    }
  }

  /**
   * Returns the name for the terminal. Useful for generating diagnostic
   * messages.
   * @param n The terminal symbol for which the name is required.
   * @return
   */
  public static String getTerminalName(int n) {
    assert n < 0 && n >= LASTPRIM;
    return terminalNames[-n];
  }
  
  public String toString() {
    StringBuffer sb = new StringBuffer();
    appendTo(sb, "root", root);
    appendTo(sb, "enums", enums);
    appendTo(sb, "fixeds", fixeds);
    appendTo(sb, "unions", unions);
    appendTo(sb, "repeaters", repeaters);
    
    appendTo(sb, prods);
    return sb.toString();
  }

  protected static void appendTo(StringBuffer sb, int[] prods) {
    sb.append('[');
    for (int i = 0; i < prods.length; i++) {
      if (i != 0) {
        sb.append(", ");
      }
      sb.append(prods[i]);
    }
    sb.append(']');
  }

  protected static void appendTo(StringBuffer sb, String name, int value) {
    sb.append(name);
    sb.append(" = ");
    sb.append(value);
    sb.append(", ");
  }

}
