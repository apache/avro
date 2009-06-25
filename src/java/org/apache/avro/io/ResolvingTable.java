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
import org.apache.avro.Schema.Type;

import java.util.*;

/**
 * The parsing table for "resolving" between two schemas. This class
 * is useful for validating inputs and not outputs. There is a reader's
 * schema and a writer's schema. The reader is interested in data according to
 * the reader's schema. But the data itself has been written according to
 * the writer's schema. While reading the reader gets an impression that
 * the data is in the reader's schema (except one change mentioned below).
 * The schema resolution process does the required transformation(s). Of course,
 * for this to work, the two schemas must be "compatible". Though some
 * of the incompatibilities can be statically detected by looking at the two
 * schemas, we report the incompatibilities only when data is sought to be read.
 * 
 * While reading records, the fileds are returned according to the
 * writer's schema and not the reader's. This apparently un-intuitive method
 * is used in order to avoid caching at the ResolvingDecoder level.  
 */
class ResolvingTable extends ParsingTable {
  public static final int FIELDACTION = LASTPRIM - 1;

  protected int errorActions;
  protected int readerUnionActions;
  protected int resolverActions;
  protected int fieldActions;
  protected int skipActions;
  protected int writerUnionActions;
  protected int enumActions;

  private String[] strings;
  // Index into strings array. Used during construction.
  private int spos;

  public ResolvingTable(Schema writer, Schema reader) {
    generate(writer, reader, new HashMap<LitS,Integer>());

    int total = (errorActions + readerUnionActions + resolverActions
                 + nonTerminals + repeaters + unions + fixeds + enums
                 + fieldActions + skipActions + writerUnionActions
                 + enumActions);

    prods = new int[total];
    strings = new String[errorActions + fieldActions];

    enumActions = total - enumActions;
    writerUnionActions = enumActions - writerUnionActions;
    skipActions = writerUnionActions - skipActions;
    fieldActions = skipActions - fieldActions;
    resolverActions = fieldActions - resolverActions;
    readerUnionActions = resolverActions - readerUnionActions;
    errorActions = readerUnionActions - errorActions;
    enums = errorActions - enums;
    fixeds = enums - fixeds;
    unions = fixeds - unions;
    repeaters = unions - repeaters;
    nonTerminals = repeaters - nonTerminals;
    spos = 0;

    secondPass = true;
    root = generate(writer, reader, new HashMap<LitS,Integer>());
  }

  protected final int generate(Schema writer, Schema reader,
      Map<LitS,Integer> seen) {
    Schema.Type writerType = writer.getType();
    Schema.Type readerType = reader.getType();

    if (writerType == readerType) {
      switch (writerType) {
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
      }
    }

    if (writerType == Schema.Type.UNION && readerType != Schema.Type.UNION) {
      List<Schema> branches = writer.getTypes();
      int u = allocWriterUnionAction(branches.size());
      for (Schema w : branches) {
        set(u++, generate(w, reader, seen));
      }
      return u - branches.size();
    }

    switch (readerType) {
    case NULL:
    case BOOLEAN:
    case INT:
    case STRING:
    case FLOAT:
    case BYTES:
      return mkErrorAction("Found " + writer + ", expecting " + reader);

    case LONG:
      switch (writerType) {
      case INT:
      case DOUBLE:
      case FLOAT:
        return mkResolverAction(generate(writer, seen));
      default:
        return mkErrorAction("Found " + writer + ", expecting " + reader);
      }

    case DOUBLE:
      switch (writerType) {
      case INT:
      case LONG:
      case FLOAT:
        return mkResolverAction(generate(writer, seen));
      default:
        return mkErrorAction("Found " + writer + ", expecting " + reader);
      }

    case FIXED:
      if (writerType != Schema.Type.FIXED
          || (writer.getName() != null
              && ! writer.getName().equals(reader.getName()))) {
        return mkErrorAction("Found " + writer + ", expecting " + reader);
      } else {
        return (writer.getFixedSize() == reader.getFixedSize()) ?
          mkNonTerm(FIXED, mkFixed(reader.getFixedSize())) :
          mkErrorAction("Size mismatch in fixed field: found "
              + writer.getFixedSize() + ", expecting " + reader.getFixedSize());
      }
    case ENUM:
      if (writerType != Schema.Type.ENUM
          || (writer.getName() != null
              && ! writer.getName().equals(reader.getName()))) {
        return mkErrorAction("Found " + writer + ", expecting " + reader);
      } else {
        int result = allocEnumAction(writer.getEnumSymbols().size());
        int u = result;
        for (String n : writer.getEnumSymbols()) {
            set(u++, mkEnumAction(n, reader));
        }
        return mkNonTerm(ENUM, result);
      }

    case ARRAY:
      if (writerType != Schema.Type.ARRAY) {
        return mkErrorAction("Found " + writer + ", expecting " + reader);
      } else {
        int ar_et = generate(writer.getElementType(),
                             reader.getElementType(), seen);
        int r_et = mkRepeater(ar_et);
        return mkNonTerm(ARRAYSTART, r_et, ARRAYEND);
      }

    case MAP:
      if (writerType != Schema.Type.MAP) {
        return mkErrorAction("Found " + writer + ", expecting " + reader);
      } else {
        int ar_vt = generate(writer.getValueType(),
                             reader.getValueType(),
                             seen);
        int r_vt = mkRepeater(STRING, ar_vt);
        return mkNonTerm(MAPSTART, r_vt, MAPEND);
      }

    case RECORD:
      LitS wsc = new LitS2(writer, reader);
      if (seen.get(wsc) == null) {
        int result;
        if (writerType != Schema.Type.RECORD
            || (writer.getName() != null
                && ! writer.getName().equals(reader.getName()))) {
          result = mkErrorAction("Found " + writer + ", expecting " + reader);
        } else {
          outer:
          do {
            Map<String, Field> wfields = writer.getFields();
            Map<String, Field> rfields = reader.getFields();
            /*
             * For now every field in read-record with no default value
             * must be in write-record.
             * Write record may have additional fields, which will be
             * skipped during read.
             */

            boolean useDefault = false;
            int rsize = 0;
            for (Map.Entry<String, Field> e : rfields.entrySet()) {
              Field f = wfields.get(e.getKey());
              if (f == null) {
                Field wf = e.getValue();
                if (wf.defaultValue() == null) {
                  result = mkErrorAction("Found " + writer + ", expecting " + reader);
                  break outer;
                } else {
                  useDefault = true;
                }
              } else {
                rsize++;
              }
            }
            int size = 2 * rsize + (wfields.size() - rsize);
            if (useDefault) {
              rsize++;
            }
            result = allocNonTerm(size);
            int i = result + size;
            for (Map.Entry<String, Field> wf : wfields.entrySet()) {
              String fname = wf.getKey();
              Field rf = rfields.get(fname);
              if (rf == null) {
                set(--i, mkSkipAction(generate(wf.getValue().schema(), seen)));
              } else {
                set(--i, mkFieldAction(rf.pos(), fname));
                set(--i, generate(wf.getValue().schema(), rf.schema(), seen));
              }
            }
            /*
             *  Insert a "special" field action to indicate that there are
             *  no more fields for this record, but some reader fields
             *  need to be filled with default values.
             */
            if (useDefault) {
              set(--i, mkFieldAction(-1, null));
            }
          } while (false);
        }
        seen.put(wsc, result);
      }
      return seen.get(wsc);

    case UNION:
      if (writerType != Schema.Type.UNION) { // Only reader is union
        return mkNonTerm(UNION, mkReaderUnionAction(writer, reader, seen));
      } else { // Both reader and writer are unions
        int result = allocUnion(writer.getTypes().size());
        int u = result;
        for (Schema w : writer.getTypes()) {
          set(u++, mkReaderUnionAction(w, reader, seen));
        }
        return mkNonTerm(UNION, result);
      }

    default:
      throw new RuntimeException("Unexpected schema type: " + readerType);
    }
  }

  protected int mkString(String s) {
    if (secondPass) {
      strings[spos] = s;
    }
    return spos++;
  }

  private final int mkErrorAction(String message) {
    set(errorActions, mkString(message));
    return errorActions++;
  }

  private final int mkReaderUnionAction(Schema w, Schema r, Map<LitS, Integer> s) {
    int j = bestBranch(r, w);
    if (j < 0) {
      return mkErrorAction("Found " + w + ", expecting " + r);
    } else {
      int result = readerUnionActions;
      readerUnionActions += 2;
      set(result, j);
      set(result + 1, generate(w, r.getTypes().get(j), s));
      return result;
    }
  }

  private int bestBranch(Schema r, Schema w) {
	  Schema.Type vt = w.getType();
      // first scan for exact match
      int j = 0;
      for (Schema b : r.getTypes()) {
        if (vt == b.getType())
          if (vt == Type.RECORD) {
            String vname = w.getName();
            if (vname == null || vname.equals(b.getName()))
              return j;
          } else
            return j;
        j++;
      }

      // then scan match via numeric promotion
      j = 0;
      for (Schema b : r.getTypes()) {
        switch (vt) {
        case INT:
          switch (b.getType()) {
          case LONG: case DOUBLE:
            return j;
          }
          break;
        case LONG:
        case FLOAT:
          switch (b.getType()) {
          case DOUBLE:
            return j;
          }
          break;
        }
        j++;
      }
      return -1;
  }

private final int mkResolverAction(int actual) {
    set(resolverActions, actual);
    return resolverActions++;
  }

  private final int mkFieldAction(int index, String name) {
    int result = fieldActions;
    fieldActions += 2;
    set(result, index);
    set(result + 1, mkString(name));
    return result;
  }

  private final int mkSkipAction(int toSkip) {
    set(skipActions, toSkip);
    return skipActions++;
  }

  private final int allocWriterUnionAction(int len) {
    set(writerUnionActions, len);
    writerUnionActions += (len + 1);
    return writerUnionActions - len;
  }
  
  private final int allocEnumAction(int len) {
    set(enumActions, len);
    enumActions += (len + 1);
    return enumActions - len;
  }

  private final int mkEnumAction(String n, Schema r) {
    int k = r.getEnumOrdinal(n);
    if (k < 0) {
      return mkErrorAction("Unknown enum: " + n);
    } else {
      set(enumActions++, 1);
      set(enumActions++, k);
      return enumActions - 1;
    }
  }
  
  public final boolean isAction(int sym) {
    return enums <= sym && sym < enumActions;
  }
  
  public final boolean isErrorAction(int sym) {
    return enums <= sym && sym < errorActions;
  }

  public final boolean isReaderUnionAction(int sym) {
    return errorActions <= sym && sym < readerUnionActions;
  }

  public final boolean isResolverAction(int sym) {
    return readerUnionActions <= sym && sym < resolverActions;
  }

  public final boolean isFieldAction(int sym) {
    return resolverActions <= sym && sym < fieldActions;
  }

  public final boolean isSkipAction(int sym) {
    return fieldActions <= sym && sym < skipActions;
  }

  public final boolean isWriterUnionAction(int sym) {
    return skipActions <= sym && sym < writerUnionActions;
  }

  public final boolean isEnumAction(int sym) {
    return writerUnionActions <= sym && sym < enumActions;
  }

  public final String getMessage(int errorAction) {
    return strings[prods[errorAction]];
  }

  public final int getResolverActual(int resolverAction) {
    return prods[resolverAction];
  }

  public final String getFieldName(int fieldAction) {
    return strings[prods[fieldAction + 1]];
  }

  public final int getFieldIndex(int fieldAction) {
    return prods[fieldAction];
  }

  public final int getProductionToSkip(int skipAction) {
    return prods[skipAction];
  }

  public final int getReaderUnionIndex(int readerUnionAction) {
    return prods[readerUnionAction];
  }

  public final int getReaderUnionSym(int readerUnionAction) {
    return prods[readerUnionAction + 1];
  }

  public final int getEnumAction(int ntsym, int e) {
    return prods[ntsym + e];
  }

  public final int getEnumValue(int enumAction) {
    return prods[enumAction];
  }

  /** Clever trick which differentiates items put into
    * <code>seen</code> by {@link #count(Schema,Map<LitS,Integer>)}
    * from those put in by {@link
    * #count(Schema,Schema,Map<LitS,Integer>)}. */
  protected static class LitS2 extends LitS {
    public Schema expected;
    public LitS2(Schema actual, Schema expected) {
      super(actual);
      this.expected = expected;
    }
    public boolean equals(Object o) {
      if (! (o instanceof LitS2)) return false;
      LitS2 other = (LitS2)o;
      return actual == other.actual && expected == other.expected;
    }
    public int hashCode() {
      return super.hashCode() + expected.hashCode();
    }
  }
  
  public String toString() {
    StringBuffer sb = new StringBuffer();
    appendTo(sb, "root", root);
    appendTo(sb, "errorActions", errorActions);
    appendTo(sb, "readerUnionActions", readerUnionActions);
    appendTo(sb, "resolverActions", resolverActions);
    appendTo(sb, "nonTerminals", nonTerminals);
    appendTo(sb, "unions", unions);
    appendTo(sb, "repeaters", repeaters);
    appendTo(sb, "fieldActions", fieldActions);
    appendTo(sb, "skipActions", skipActions);
    appendTo(sb, "writerUnionActions", writerUnionActions);
    appendTo(sb, "enumActions", enumActions);
    
    appendTo(sb, prods);
    return sb.toString();
  }

}
