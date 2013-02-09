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
package org.apache.avro.io.parsing;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.codehaus.jackson.JsonNode;

/**
 * The class that generates a resolving grammar to resolve between two
 * schemas.
 */
public class ResolvingGrammarGenerator extends ValidatingGrammarGenerator {
  /**
   * Resolves the writer schema <tt>writer</tt> and the reader schema
   * <tt>reader</tt> and returns the start symbol for the grammar generated. 
   * @param writer    The schema used by the writer
   * @param reader    The schema used by the reader
   * @return          The start symbol for the resolving grammar
   * @throws IOException 
   */
  public final Symbol generate(Schema writer, Schema reader)
    throws IOException {
    return Symbol.root(generate(writer, reader, new HashMap<LitS, Symbol>()));
  }
  
  /**
   * Resolves the writer schema <tt>writer</tt> and the reader schema
   * <tt>reader</tt> and returns the start symbol for the grammar generated.
   * If there is already a symbol in the map <tt>seen</tt> for resolving the
   * two schemas, then that symbol is returned. Otherwise a new symbol is
   * generated and returnd. 
   * @param writer    The schema used by the writer
   * @param reader    The schema used by the reader
   * @param seen      The &lt;reader-schema, writer-schema&gt; to symbol
   * map of start symbols of resolving grammars so far.
   * @return          The start symbol for the resolving grammar
   * @throws IOException 
   */
  public Symbol generate(Schema writer, Schema reader,
                                Map<LitS, Symbol> seen) throws IOException
  {
    final Schema.Type writerType = writer.getType();
    final Schema.Type readerType = reader.getType();

    if (writerType == readerType) {
      switch (writerType) {
      case NULL:
        return Symbol.NULL;
      case BOOLEAN:
        return Symbol.BOOLEAN;
      case INT:
        return Symbol.INT;
      case LONG:
        return Symbol.LONG;
      case FLOAT:
        return Symbol.FLOAT;
      case DOUBLE:
        return Symbol.DOUBLE;
      case STRING:
        return Symbol.STRING;
      case BYTES:
        return Symbol.BYTES;
      case FIXED:
        if (writer.getFullName().equals(reader.getFullName())
            && writer.getFixedSize() == reader.getFixedSize()) {
          return Symbol.seq(Symbol.intCheckAction(writer.getFixedSize()),
              Symbol.FIXED);
        }
        break;

      case ENUM:
        if (writer.getFullName() == null
                || writer.getFullName().equals(reader.getFullName())) {
          return Symbol.seq(mkEnumAdjust(writer.getEnumSymbols(),
                  reader.getEnumSymbols()), Symbol.ENUM);
        }
        break;

      case ARRAY:
        return Symbol.seq(Symbol.repeat(Symbol.ARRAY_END,
                generate(writer.getElementType(),
                reader.getElementType(), seen)),
            Symbol.ARRAY_START);
      
      case MAP:
        return Symbol.seq(Symbol.repeat(Symbol.MAP_END,
                generate(writer.getValueType(),
                reader.getValueType(), seen), Symbol.STRING),
            Symbol.MAP_START);
      case RECORD:
        return resolveRecords(writer, reader, seen);
      case UNION:
        return resolveUnion(writer, reader, seen);
      default:
        throw new AvroTypeException("Unkown type for schema: " + writerType);
      }
    } else {  // writer and reader are of different types
      if (writerType == Schema.Type.UNION) {
        return resolveUnion(writer, reader, seen);
      }
  
      switch (readerType) {
      case LONG:
        switch (writerType) {
        case INT:
          return Symbol.resolve(super.generate(writer, seen), Symbol.LONG);
        }
        break;
  
      case FLOAT:
        switch (writerType) {
        case INT:
        case LONG:
          return Symbol.resolve(super.generate(writer, seen), Symbol.FLOAT);
        }
        break;
  
      case DOUBLE:
        switch (writerType) {
        case INT:
        case LONG:
        case FLOAT:
          return Symbol.resolve(super.generate(writer, seen), Symbol.DOUBLE);
        }
        break;
  
      case UNION:
        int j = bestBranch(reader, writer);
        if (j >= 0) {
          Symbol s = generate(writer, reader.getTypes().get(j), seen);
          return Symbol.seq(Symbol.unionAdjustAction(j, s), Symbol.UNION);
        }
        break;
      case NULL:
      case BOOLEAN:
      case INT:
      case STRING:
      case BYTES:
      case ENUM:
      case ARRAY:
      case MAP:
      case RECORD:
        break;
      default:
        throw new RuntimeException("Unexpected schema type: " + readerType);
      }
    }
    return Symbol.error("Found " + writer.getFullName()
                        + ", expecting " + reader.getFullName());
  }

  private Symbol resolveUnion(Schema writer, Schema reader,
      Map<LitS, Symbol> seen) throws IOException {
    List<Schema> alts = writer.getTypes();
    final int size = alts.size();
    Symbol[] symbols = new Symbol[size];
    String[] labels = new String[size];

    /**
     * We construct a symbol without filling the arrays. Please see
     * {@link Symbol#production} for the reason.
     */
    int i = 0;
    for (Schema w : alts) {
      symbols[i] = generate(w, reader, seen);
      labels[i] = w.getFullName();
      i++;
    }
    return Symbol.seq(Symbol.alt(symbols, labels),
                      Symbol.writerUnionAction());
  }

  private Symbol resolveRecords(Schema writer, Schema reader,
      Map<LitS, Symbol> seen) throws IOException {
    LitS wsc = new LitS2(writer, reader);
    Symbol result = seen.get(wsc);
    if (result == null) {
      List<Field> wfields = writer.getFields();
      List<Field> rfields = reader.getFields();

      // First, compute reordering of reader fields, plus
      // number elements in the result's production
      Field[] reordered = new Field[rfields.size()];
      int ridx = 0;
      int count = 1 + wfields.size();

      for (Field f : wfields) {
        Field rdrField = reader.getField(f.name());
        if (rdrField != null) {
          reordered[ridx++] = rdrField;
        }
      }

      for (Field rf : rfields) {
        String fname = rf.name();
        if (writer.getField(fname) == null) {
          if (rf.defaultValue() == null) {
            result = Symbol.error("Found " + writer.getFullName()
                                  + ", expecting " + reader.getFullName());
            seen.put(wsc, result);
            return result;
          } else {
            reordered[ridx++] = rf;
            count += 3;
          }
        }
      }

      Symbol[] production = new Symbol[count];
      production[--count] = Symbol.fieldOrderAction(reordered);

      /**
       * We construct a symbol without filling the array. Please see
       * {@link Symbol#production} for the reason.
       */
      result = Symbol.seq(production);
      seen.put(wsc, result);

      /*
       * For now every field in read-record with no default value
       * must be in write-record.
       * Write record may have additional fields, which will be
       * skipped during read.
       */

      // Handle all the writer's fields
      for (Field wf : wfields) {
        String fname = wf.name();
        Field rf = reader.getField(fname);
        if (rf == null) {
          production[--count] =
            Symbol.skipAction(generate(wf.schema(), wf.schema(), seen));
        } else {
          production[--count] =
            generate(wf.schema(), rf.schema(), seen);
        }
      }

      // Add default values for fields missing from Writer
      for (Field rf : rfields) {
        String fname = rf.name();
        Field wf = writer.getField(fname);
        if (wf == null) {
          byte[] bb = getBinary(rf.schema(), rf.defaultValue());
          production[--count] = Symbol.defaultStartAction(bb);
          production[--count] = generate(rf.schema(), rf.schema(), seen);
          production[--count] = Symbol.DEFAULT_END_ACTION;
        }
      }
    }
    return result;
  }

  private static EncoderFactory factory = new EncoderFactory().configureBufferSize(32);
  /**
   * Returns the Avro binary encoded version of <tt>n</tt> according to
   * the schema <tt>s</tt>.
   * @param s The schema for encoding
   * @param n The Json node that has the value to be encoded.
   * @return  The binary encoded version of <tt>n</tt>.
   * @throws IOException
   */
  private static byte[] getBinary(Schema s, JsonNode n) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder e = factory.binaryEncoder(out, null);
    encode(e, s, n);
    e.flush();
    return out.toByteArray();
  }
  
  /**
   * Encodes the given Json node <tt>n</tt> on to the encoder <tt>e</tt>
   * according to the schema <tt>s</tt>.
   * @param e The encoder to encode into.
   * @param s The schema for the object being encoded.
   * @param n The Json node to encode.
   * @throws IOException
   */
  
  public static void encode(Encoder e, Schema s, JsonNode n)
    throws IOException {
    switch (s.getType()) {
    case RECORD:
      for (Field f : s.getFields()) {
        String name = f.name();
        JsonNode v = n.get(name);
        if (v == null) {
          v = f.defaultValue();
        }
        if (v == null) {
          throw new AvroTypeException("No default value for: " + name);
        }
        encode(e, f.schema(), v);
      }
      break;
    case ENUM:
      e.writeEnum(s.getEnumOrdinal(n.getTextValue()));
      break;
    case ARRAY:
      e.writeArrayStart();
      e.setItemCount(n.size());
      Schema i = s.getElementType();
      for (JsonNode node : n) {
        e.startItem();
        encode(e, i, node);
      }
      e.writeArrayEnd();
      break;
    case MAP:
      e.writeMapStart();
      e.setItemCount(n.size());
      Schema v = s.getValueType();
      for (Iterator<String> it = n.getFieldNames(); it.hasNext();) {
        e.startItem();
        String key = it.next();
        e.writeString(key);
        encode(e, v, n.get(key));
      }
      e.writeMapEnd();
      break;
    case UNION:
      e.writeIndex(0);
      encode(e, s.getTypes().get(0), n);
      break;
    case FIXED:
      if (!n.isTextual())
        throw new AvroTypeException("Non-string default value for fixed: "+n);
      byte[] bb = n.getTextValue().getBytes("ISO-8859-1");
      if (bb.length != s.getFixedSize()) {
        bb = Arrays.copyOf(bb, s.getFixedSize());
      }
      e.writeFixed(bb);
      break;
    case STRING:
      if (!n.isTextual())
        throw new AvroTypeException("Non-string default value for string: "+n);
      e.writeString(n.getTextValue());
      break;
    case BYTES:
      if (!n.isTextual())
        throw new AvroTypeException("Non-string default value for bytes: "+n);
      e.writeBytes(n.getTextValue().getBytes("ISO-8859-1"));
      break;
    case INT:
      if (!n.isNumber())
        throw new AvroTypeException("Non-numeric default value for int: "+n);
      e.writeInt(n.getIntValue());
      break;
    case LONG:
      if (!n.isNumber())
        throw new AvroTypeException("Non-numeric default value for long: "+n);
      e.writeLong(n.getLongValue());
      break;
    case FLOAT:
      if (!n.isNumber())
        throw new AvroTypeException("Non-numeric default value for float: "+n);
      e.writeFloat((float) n.getDoubleValue());
      break;
    case DOUBLE:
      if (!n.isNumber())
        throw new AvroTypeException("Non-numeric default value for double: "+n);
      e.writeDouble(n.getDoubleValue());
      break;
    case BOOLEAN:
      if (!n.isBoolean())
        throw new AvroTypeException("Non-boolean default for boolean: "+n);
      e.writeBoolean(n.getBooleanValue());
      break;
    case NULL:
      if (!n.isNull())
        throw new AvroTypeException("Non-null default value for null type: "+n);
      e.writeNull();
      break;
    }
  }

  private static Symbol mkEnumAdjust(List<String> wsymbols,
      List<String> rsymbols){
    Object[] adjustments = new Object[wsymbols.size()];
    for (int i = 0; i < adjustments.length; i++) {
      int j = rsymbols.indexOf(wsymbols.get(i));
      adjustments[i] = (j == -1 ? "No match for " + wsymbols.get(i)
                                : new Integer(j));
    }
    return Symbol.enumAdjustAction(rsymbols.size(), adjustments);
  }

  private static int bestBranch(Schema r, Schema w) {
    Schema.Type vt = w.getType();
      // first scan for exact match
      int j = 0;
      for (Schema b : r.getTypes()) {
        if (vt == b.getType())
          if (vt == Schema.Type.RECORD || vt == Schema.Type.ENUM || 
              vt == Schema.Type.FIXED) {
            String vname = w.getFullName();
            String bname = b.getFullName();
            if ((vname != null && vname.equals(bname))
                || vname == bname && vt == Schema.Type.RECORD)
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

  /**
   * Clever trick which differentiates items put into
   * <code>seen</code> by {@link ValidatingGrammarGenerator#validating validating()}
   * from those put in by {@link ValidatingGrammarGenerator#resolving resolving()}.
   */
   static class LitS2 extends ValidatingGrammarGenerator.LitS {
     public Schema expected;
     public LitS2(Schema actual, Schema expected) {
       super(actual);
       this.expected = expected;
     }
     public boolean equals(Object o) {
       if (! (o instanceof LitS2)) return false;
       LitS2 other = (LitS2) o;
       return actual == other.actual && expected == other.expected;
     }
     public int hashCode() {
       return super.hashCode() + expected.hashCode();
     }
   }
}

