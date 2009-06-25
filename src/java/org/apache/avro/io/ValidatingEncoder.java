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
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;

/** {@link Encoder} that ensures that the sequence of operations conforms
 * to a schema.
 *
 * <p>See the <a href="doc-files/parsing.html">parser documentation</a> for
 *  information on how this works.*/
public class ValidatingEncoder extends Encoder {
  private final Encoder out;
  protected int[] stack;
  protected int pos;
  protected ParsingTable table;

  public ValidatingEncoder(Schema schema, Encoder out) {
    this.out = out;
    this.stack = new int[5]; // Start small to make sure expansion code works
    this.pos = 0;
    this.table = new ParsingTable(schema);
    stack[pos++] = table.root;
  }

  @Override
  public void init(OutputStream out) throws IOException {
    flush();
    this.out.init(out);
  }

  @Override
  public void flush() throws IOException {
    out.flush();
  }

  @Override
  public void writeNull() throws IOException {
    advance(ParsingTable.NULL);
    out.writeNull();
  }
    
  @Override
  public void writeBoolean(boolean b) throws IOException {
    advance(ParsingTable.BOOLEAN);
    out.writeBoolean(b);
  }

  @Override
  public void writeInt(int n) throws IOException {
    advance(ParsingTable.INT);
    out.writeLong(n);
  }

  @Override
  public void writeLong(long n) throws IOException {
    advance(ParsingTable.LONG);
    out.writeLong(n);
  }
    
  @Override
  public void writeFloat(float f) throws IOException {
    advance(ParsingTable.FLOAT);
    out.writeFloat(f);
  }

  @Override
  public void writeDouble(double d) throws IOException {
    advance(ParsingTable.DOUBLE);
    out.writeDouble(d);
  }

  @Override
  public void writeString(Utf8 utf8) throws IOException {
    advance(ParsingTable.STRING);
    out.writeString(utf8);
  }

  @Override
  public void writeBytes(ByteBuffer bytes) throws IOException {
    advance(ParsingTable.BYTES);
    out.writeBytes(bytes);
  }

  @Override
  public void writeBytes(byte[] bytes, int start, int len) throws IOException {
    advance(ParsingTable.BYTES);
    out.writeBytes(bytes, start, len);
  }

  @Override
  public void writeFixed(byte[] bytes, int start, int length)
    throws IOException {
    advance(ParsingTable.FIXED);
    int top = stack[--pos];
    assert table.isFixed(top);
    if (table.getFixedSize(top) != length) {
      throw new AvroTypeException(
        "Incorrect length for fiexed binary: expected " +
        table.getFixedSize(top) + " but received " + length + " bytes.");
    }
    out.writeFixed(bytes, start, length);
  }

  @Override
  public void writeEnum(int e) throws IOException {
    advance(ParsingTable.ENUM);
    int top = stack[--pos];
    assert table.isEnum(top);
    if (e < 0 || e >= table.getEnumMax(top)) {
      throw new AvroTypeException(
        "Enumeration out of range: max is " +
        table.getEnumMax(top) + " but received " + e);
    }
    out.writeEnum(e);
  }

  @Override
  public void writeArrayStart() throws IOException {
    advance(ParsingTable.ARRAYSTART);
    out.writeArrayStart();
  }

  @Override
  public void setItemCount(long itemCount) throws IOException {
    out.setItemCount(itemCount);
  }

  @Override
  public void startItem() throws IOException {
    out.startItem();
  }

  @Override
  public void writeArrayEnd() throws IOException {
    advance(ParsingTable.ARRAYEND);
    out.writeArrayEnd();
  }

  @Override
  public void writeMapStart() throws IOException {
    advance(ParsingTable.MAPSTART);
    out.writeMapStart();
  }

  @Override
  public void writeMapEnd() throws IOException {
    advance(ParsingTable.MAPEND);
    out.writeMapEnd();
  }

  @Override
  public void writeIndex(int unionIndex) throws IOException {
    advance(ParsingTable.UNION);
    int top = stack[--pos];
    assert table.isUnion(top);
    stack[pos++] = table.getBranch(top, unionIndex);
    out.writeIndex(unionIndex);
  }

  private int advance(int input) {
    int top = stack[--pos];
    while (! table.isTerminal(top)) {
      if (! table.isRepeater(top)
          || (input != ParsingTable.ARRAYEND && input != ParsingTable.MAPEND)) {
        int plen = table.size(top);
        if (stack.length < pos + plen) {
          stack = ValidatingDecoder.expand(stack, pos + plen);
        }
        System.arraycopy(table.prods, top, stack, pos, plen);
        pos += plen;
      }
      top = stack[--pos];
    }
    assert table.isTerminal(top);
    if (top != input) {
      throw new AvroTypeException("Attempt to write a " +
          ParsingTable.getTerminalName(input) + " when a "
          + ParsingTable.getTerminalName(top) + " was expected.");
    }
    return top;
  }

  /**
   * After writing a complete object that conforms to the schema or after an
   * error, if you want to start writing another
   * object, call this method.
   */
  public void reset() {
    /*
     * Design note: Why is this a separate method? Why can't we auto reset
     * in advance() when the stack becomes empty which is an indication that
     * an object has been completed?
     * 
     * (1) advance() is called in an inner loop. Saving cycles there is worthy.
     * (2) The client may actually be writing something beyond the current
     * object and the writer will think it has started writing the next object.
     * This may lead to certain errors not being caught.
     * (3) We need reset() anyway to take the writer out of any error state.
     */
    pos = 0;
    stack[pos++] = table.root;
  }
}
