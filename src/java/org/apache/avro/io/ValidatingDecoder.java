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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;

/** {@link Decoder} that ensures that the sequence of operations conforms
 *  to a schema.
 * <p>See the <a href="doc-files/parsing.html">parser documentation</a> for
 *  information on how this works.
 */
public class ValidatingDecoder extends Decoder {
  protected final Decoder in;
  protected int[] stack;
  protected int pos;
  ParsingTable table;

  ValidatingDecoder(ParsingTable table, Decoder in)
    throws IOException {
    this.in = in;
    this.stack = new int[5]; // Start small to make sure expansion code works
    this.pos = 0;
    this.table = table;
    reset();
  }

  public ValidatingDecoder(Schema schema, Decoder in)
    throws IOException {
    this(new ParsingTable(schema), in);
  }

  @Override
  public void init(InputStream in) {
    this.in.init(in);
  }

  @Override
  public void readNull() throws IOException {
    advance(ParsingTable.NULL);
    in.readNull();
  }
    
  @Override
  public boolean readBoolean() throws IOException {
    advance(ParsingTable.BOOLEAN);
    return in.readBoolean();
  }

  @Override
  public int readInt() throws IOException {
    advance(ParsingTable.INT);
    return in.readInt();
  }
    
  @Override
  public long readLong() throws IOException {
    advance(ParsingTable.LONG);
    return in.readLong();
  }

  @Override
  public float readFloat() throws IOException {
    advance(ParsingTable.FLOAT);
    return in.readFloat();
  }

  @Override
  public double readDouble() throws IOException {
    advance(ParsingTable.DOUBLE);
    return in.readDouble();
  }
    
  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    advance(ParsingTable.STRING);
    return in.readString(old);
  }

  @Override
  public void skipString() throws IOException {
    advance(ParsingTable.STRING);
    in.skipString();
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    advance(ParsingTable.BYTES);
    return in.readBytes(old);
  }

  @Override
  public void skipBytes() throws IOException {
    advance(ParsingTable.BYTES);
    in.skipBytes();
  }

  @Override
  public void readFixed(byte[] bytes, int start, int length) throws IOException {
    advance(ParsingTable.FIXED);
    int top = stack[--pos];
    assert table.isFixed(top);
    if (table.getFixedSize(top) != length) {
      throw new AvroTypeException(
        "Incorrect length for fiexed binary: expected " +
        table.getFixedSize(top) + " but received " + length + " bytes.");
    }
    in.readFixed(bytes, start, length);
  }

  @Override
  public void skipFixed(int length) throws IOException {
    advance(ParsingTable.FIXED);
    int top = stack[--pos];
    assert table.isFixed(top);
    if (table.getFixedSize(top) != length) {
      throw new AvroTypeException(
        "Incorrect length for fiexed binary: expected " +
        table.getFixedSize(top) + " but received " + length + " bytes.");
    }
    in.skipFixed(length);
  }

  @Override
  public int readEnum() throws IOException {
    advance(ParsingTable.ENUM);
    int top = stack[--pos];
    assert table.isEnum(top);
    int result = in.readEnum();
    if (result < 0 || result >= table.getEnumMax(top)) {
      throw new AvroTypeException(
          "Enumeration out of range: max is " +
          table.getEnumMax(top) + " but received " + result);
    }
    return result;
  }

  @Override
  public long readArrayStart() throws IOException {
    advance(ParsingTable.ARRAYSTART);
    long result = in.readArrayStart();
    if (result == 0) {
      advance(ParsingTable.ARRAYEND);
    }
    return result;
  }

  @Override
  public long arrayNext() throws IOException {
    long result = in.readLong();
    if (result == 0) {
      advance(ParsingTable.ARRAYEND);
    }
    return result;
  }

  @Override
  public long skipArray() throws IOException {
    advance(ParsingTable.ARRAYSTART);
    skipFullArray(stack[--pos]);
    advance(ParsingTable.ARRAYEND);
    return 0;
  }

  @Override
  public long readMapStart() throws IOException {
    advance(ParsingTable.MAPSTART);
    long result = in.readMapStart();
    if (result == 0) {
      advance(ParsingTable.MAPEND);
    }
    return result;
  }

  @Override
  public long mapNext() throws IOException {
    long result = in.mapNext();
    if (result == 0) {
      advance(ParsingTable.MAPEND);
    }
    return result;
  }

  @Override
  public long skipMap() throws IOException {
    advance(ParsingTable.MAPSTART);
    skipFullMap(stack[--pos]);
    advance(ParsingTable.MAPEND);
    return 0;
  }

  @Override
  public int readIndex() throws IOException {
    advance(ParsingTable.UNION);
    int top = stack[--pos];
    assert table.isUnion(top);
    int result = in.readIndex();
    stack[pos++] = table.getBranch(top, result);
    return result;
  }

  @Override
  public void reset() throws IOException {
    /*
     * See the design note for ValidatingValueWriter.reset() on why such
     * a method is needed.
     */
    while (pos > 0) {
      skipSymbol(stack[pos], -1);
    }
    stack[pos++] = table.root;
  }

  /** Skip the values described by a production. */
  protected void skipProduction(int ntsym) throws IOException {
    if (! table.isNonTerminal(ntsym) && ! table.isRepeater(ntsym)) {
      throw new IllegalArgumentException("Can't skip a " + ntsym);
    }
    for (int i = table.size(ntsym) - 1; i >= 0; i--) {
      int sym = table.prods[ntsym + i];
      if (table.isRepeater(sym))
        continue; // Don't recurse -- our caller will do that for us

      i = skipSymbol(table.prods[ntsym + i], ntsym + i) - ntsym;
    }
  }

  protected int skipSymbol(int sym, int p) throws IOException {
    switch (sym) {
    case ParsingTable.NULL:
      in.readNull();
      break;
    case ParsingTable.BOOLEAN:
      in.readBoolean();
      break;
    case ParsingTable.INT:
      in.readInt();
      break;
    case ParsingTable.LONG:
      in.readLong();
      break;
    case ParsingTable.FLOAT:
      in.readFloat();
      break;
    case ParsingTable.DOUBLE:
      in.readDouble();
      break;
    case ParsingTable.STRING:
      in.skipString();
      break;
    case ParsingTable.BYTES:
      in.skipBytes();
      break;
    case ParsingTable.FIXED:
      in.skipFixed(table.getFixedSize(table.prods[--p]));
      break;
    case ParsingTable.UNION:
      skipSymbol(table.getBranch(table.prods[--p], in.readIndex()), -1);
      break;
    case ParsingTable.ARRAYSTART:
      while (p-- >= 0) {
        int element = table.prods[p];
        if (table.isRepeater(element)) {
          skipFullArray(element);
          break;
        }
      }
      while (table.prods[p] != ParsingTable.ARRAYEND) {
        p--; // (skip action syms)
      }
      break;
    case ParsingTable.MAPSTART:
      while (p-- >= 0) {
        int element = table.prods[p];
        if (table.isRepeater(element)) {
          skipFullMap(element);
          break;
        }
      }
      while (table.prods[p] != ParsingTable.MAPEND) {
        p--; // (skip action syms)
      }
      break;
    default:  // record
      skipProduction(sym);
    }
    return p;
  }

  private final void skipFullMap(int element) throws IOException {
    for (long c = in.skipMap(); c != 0; c = in.skipMap()) {
      skipElements(element, c);
    }
  }

  private final void skipElements(int element, long count) throws IOException {
    while (count-- > 0) {
      skipProduction(element);
    }
  }

  private final void skipFullArray(int element) throws IOException {
    for (long c = in.skipArray(); c != 0; c = in.skipArray()) {
      skipElements(element, c);
    }
  }

  protected int advance(int input) throws IOException {
    int top = stack[--pos];
    while (! table.isTerminal(top)) {
      if (! table.isRepeater(top)
          || (input != ParsingTable.ARRAYEND && input != ParsingTable.MAPEND)) {
        int plen = table.size(top);
        if (stack.length < pos + plen) {
          stack = expand(stack, pos + plen);
        }
        System.arraycopy(table.prods, top, stack, pos, plen);
        pos += plen;
      }
      top = stack[--pos];
    }
    assert table.isTerminal(top);
    if (top != input) {
      throw new AvroTypeException("Attempt to read a "
          + ParsingTable.getTerminalName(input) + " when a "
          + ParsingTable.getTerminalName(top) + " was expected.");
    }
    return top;
  }

  protected static int[] expand(int[] stack, int len) {
    while (stack.length < len) {
      stack = Arrays.copyOf(stack, stack.length + Math.max(stack.length, 1024));
    }
    return stack;
  }
}
