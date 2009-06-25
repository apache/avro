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

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;

/**
 * {@link Decoder} that peforms type-resolution between the reader's and
 * writer's schemas.
 *
 * <p>When resolving schemas, this class will return the values of fields in
 * _writer's_ order, not the reader's order.  (However, it returns _only_ the
 * reader's fields, not any extra fields the writer may have written.)  To help
 * clients handle fields that appear to be coming out of order, this class
 * defines the methods {@link #readFieldName} and {@link #readFieldIndex}.
 * When called just before reading the value of a record-field, they return the
 * name/index of the field about to be read.  See {@link #readFieldIndex} for
 * usage details.
 *
 * <p>See the <a href="doc-files/parsing.html">parser documentation</a> for
 *  information on how this works.*/
public class ResolvingDecoder extends ValidatingDecoder {
  private ResolvingTable rtable;

  public ResolvingDecoder(Schema writer, Schema reader, Decoder in)
    throws IOException {
    super(new ResolvingTable(writer, reader), in);
    rtable = (ResolvingTable) table;
  }

  /** Returns the name of the next field of the record we're reading.
    * Similar to {@link #readFieldIndex} -- see that method for
    * details.
    *
    * @throws IllegalStateExcpetion If we're not about to read a record-field
    */
  public String readFieldName() throws IOException {
    int actual = advance(ResolvingTable.FIELDACTION);
    return rtable.getFieldName(actual);
  }

  /** Returns the (zero-based) index of the next field of the record
    * we're reading.
    *
    * This method is useful because {@link ResolvingDecoder}
    * returns values in the order written by the writer, rather than
    * the order expected by the reader.  This method allows reader's
    * to figure out what fields to expect.  Let's say the reader is
    * expecting a three-field record, the first field is a long, the
    * second a string, and the third an array.  In this case, a
    * typical usage might be as follows:
    * <pre>
    *   for (int i = 0; i < 3; i++) {
    *     switch (in.readFieldIndex()) {
    *     case 1:
    *       foo(in.readLong());
    *       break;
    *     case 2:
    *       someVariable = in.readString();
    *       break;
    *     case 3:
    *       bar(in); // The code of "bar" will read an array-of-int
    *       break;
    *     }
    * </pre>
    * Note that {@link ResolvingDecoder} will return only the
    * fields expected by the reader, not other fields that may have
    * been written by the writer.  Thus, the iteration-count of "3" in
    * the above loop will always be correct.
    *
    * Throws a runtime exception if we're not just about to read the
    * field of a record.  Also, this method (and {@link
    * #readFieldName}) will <i>consume</i> the field information, and
    * thus may only be called <i>once</i> before reading the field
    * value.  (However, if the client knows the order of incoming
    * fields and does not need to reorder them, then the client does
    * <i>not</i> need to call this or {@link #readFieldName}.)
    *
    * @throws IllegalStateExcpetion If we're not about to read a record-field
    *                               
    */
  public int readFieldIndex() throws IOException {
    int actual = advance(ResolvingTable.FIELDACTION);
    return rtable.getFieldIndex(actual);
  }

  @Override
  public long readLong() throws IOException {
    int actual = advance(ResolvingTable.LONG);
    if (actual == ResolvingTable.INT) {
      return in.readInt();
    } else if (actual == ResolvingTable.DOUBLE) {
      return (long) in.readDouble();
    } else {
      assert actual == ResolvingTable.LONG;
      return in.readLong();
    }
  }
    
  @Override
  public double readDouble() throws IOException {
    int actual = advance(ResolvingTable.DOUBLE);
    if (actual == ResolvingTable.INT) {
      return (double) in.readInt();
    } else if (actual == ResolvingTable.LONG) {
      return (double) in.readLong();
    } else if (actual == ResolvingTable.FLOAT) {
      return (double) in.readFloat();
    } else {
      assert actual == ResolvingTable.DOUBLE;
      return in.readDouble();
    }
  }
  
  @Override
  public int readEnum() throws IOException {
    advance(ResolvingTable.ENUM);
    int top = stack[--pos];
    int n = in.readEnum();
    if (n >= 0 && n < rtable.size(top)) {
      n = rtable.getEnumAction(top, n);
      if (rtable.isEnumAction(n)) {
        return rtable.getEnumValue(n);
      } else {
        assert rtable.isErrorAction(n);
        throw new AvroTypeException(rtable.getMessage(n));
      }
    } else {
      throw new AvroTypeException("Enumeration out of range: " + n
          + " max: " + rtable.size(top));
    }
  }
    
  @Override
  public int readIndex() throws IOException {
    advance(ParsingTable.UNION);
    int actual = stack[--pos];
    if (rtable.isUnion(actual)) {
      actual = rtable.getBranch(actual, in.readInt());
    }
    if (rtable.isReaderUnionAction(actual)) {
      // Both reader and writer where a union.  Based on
      // the writer's actual branch, go get the appropriate
      // readerUnionAction
      stack[pos++] = rtable.getReaderUnionSym(actual);
      return rtable.getReaderUnionIndex(actual);
    } else {
      throw new AvroTypeException("Unexpected index read");
    }
  }

  @Override
  protected int skipSymbol(int sym, int p) throws IOException {
    if (rtable.isResolverAction(sym)) {
      return skipSymbol(rtable.getResolverActual(sym), -1);
    } else {
      return super.skipSymbol(sym, p);
    }
  }

  @Override
  protected int advance(int input) throws IOException {
    int top = stack[--pos];
    while (! rtable.isTerminal(top)) {
      if (rtable.isAction(top)) {
        if (rtable.isFieldAction(top)) {
          if (input == ResolvingTable.FIELDACTION) return top;
        } else if (rtable.isResolverAction(top)) {
          return rtable.getResolverActual(top);
        } else if (rtable.isSkipAction(top)) {
          skipSymbol(rtable.getProductionToSkip(top), -1);
        } else if (rtable.isWriterUnionAction(top)) {
          stack[pos++] = rtable.getBranch(top, in.readIndex());
        } else if (rtable.isErrorAction(top)) {
          throw new AvroTypeException(rtable.getMessage(top));
        }
      } else if (! rtable.isRepeater(top)
                 || (input != ParsingTable.ARRAYEND
                     && input != ParsingTable.MAPEND)) {
        int plen = rtable.size(top);
        if (stack.length < pos + plen) {
          stack = expand(stack, pos + plen);
        }
        System.arraycopy(rtable.prods, top, stack, pos, plen);
        pos += plen;
      }
      top = stack[--pos];
    }
    if (top == input) {
      return top;
    }
    throw new AvroTypeException("Attempt to read " + input
                                + " when a " + top + " was expected.");
  }
  public void reset() throws IOException {
    while (pos > 0) {
      if (rtable.isSkipAction(stack[pos - 1])) {
        skipProduction(rtable.getProductionToSkip(stack[--pos]));
      } else {
        throw new AvroTypeException("Data not fully drained.");
      }
    }
    stack[pos++] = table.root;
  }
}
