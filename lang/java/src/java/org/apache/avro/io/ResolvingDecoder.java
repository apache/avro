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

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.apache.avro.io.parsing.Symbol;

/**
 * {@link Decoder} that performs type-resolution between the reader's and
 * writer's schemas.
 *
 * <p>When resolving schemas, this class will return the values of fields in
 * _writer's_ order, not the reader's order.  (However, it returns _only_ the
 * reader's fields, not any extra fields the writer may have written.)  To help
 * clients handle fields that appear to be coming out of order, this class
 * defines the method {@link #readFieldOrder}.
 *
 * <p>See the <a href="doc-files/parsing.html">parser documentation</a> for
 *  information on how this works.
 */
public class ResolvingDecoder extends ValidatingDecoder {

  private Decoder backup;
  
  public ResolvingDecoder(Schema writer, Schema reader, Decoder in)
    throws IOException {
    super(new ResolvingGrammarGenerator().generate(writer, reader), in);
  }

  /** Returns the actual order in which the reader's fields will be
   * returned to the reader.
   *
   * This method is useful because {@link ResolvingDecoder}
   * returns values in the order written by the writer, rather than
   * the order expected by the reader.  This method allows readers
   * to figure out what fields to expect.  Let's say the reader is
   * expecting a three-field record, the first field is a long, the
   * second a string, and the third an array.  In this case, a
   * typical usage might be as follows:
   * <pre>
   *   Schema.Fields[] fieldOrder = in.readFieldOrder();
   *   for (int i = 0; i &lt; 3; i++) {
   *     switch (fieldOrder[i].pos()) {
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
   * field of a record.  Also, this method will consume the field
   * information, and thus may only be called <em>once</em> before
   * reading the field value.  (However, if the client knows the
   * order of incoming fields, then the client does <em>not</em>
   * need to call this method but rather can just start reading the
   * field values.)
   *
   * @throws AvroTypeException If we're not starting a new record
   *                               
   */
  public final Schema.Field[] readFieldOrder() throws IOException {
    return ((Symbol.FieldOrderAction) parser.advance(Symbol.FIELD_ACTION)).
      fields;
  }

  @Override
  public long readLong() throws IOException {
    Symbol actual = parser.advance(Symbol.LONG);
    if (actual == Symbol.INT) {
      return in.readInt();
    } else if (actual == Symbol.DOUBLE) {
      return (long) in.readDouble();
    } else {
      assert actual == Symbol.LONG;
      return in.readLong();
    }
  }

  @Override
  public double readDouble() throws IOException {
    Symbol actual = parser.advance(Symbol.DOUBLE);
    if (actual == Symbol.INT) {
      return (double) in.readInt();
    } else if (actual == Symbol.LONG) {
      return (double) in.readLong();
    } else if (actual == Symbol.FLOAT) {
      return (double) in.readFloat();
    } else {
      assert actual == Symbol.DOUBLE;
      return in.readDouble();
    }
  }
  
  @Override
  public int readEnum() throws IOException {
    parser.advance(Symbol.ENUM);
    Symbol.EnumAdjustAction top = (Symbol.EnumAdjustAction) parser.popSymbol();
    int n = in.readEnum();
    Object o = top.adjustments[n];
    if (o instanceof Integer) {
      return ((Integer) o).intValue();
    } else {
      throw new AvroTypeException((String) o);
    }
  }
    
  @Override
  public int readIndex() throws IOException {
    parser.advance(Symbol.UNION);
    Symbol.UnionAdjustAction top = (Symbol.UnionAdjustAction) parser.popSymbol();
    parser.pushSymbol(top.symToParse);
    return top.rindex;
  }

  @Override
  public Symbol doAction(Symbol input, Symbol top) throws IOException {
    if (top instanceof Symbol.FieldOrderAction) {
      return input == Symbol.FIELD_ACTION ? top : null;
    } if (top instanceof Symbol.ResolvingAction) {
      Symbol.ResolvingAction t = (Symbol.ResolvingAction) top;
      if (t.reader != input) {
        throw new AvroTypeException("Found " + t.reader + " while looking for "
                                    + input);
      } else {
        return t.writer;
      }
    } else if (top instanceof Symbol.SkipAction) {
      Symbol symToSkip = ((Symbol.SkipAction) top).symToSkip;
      parser.skipSymbol(symToSkip);
    } else if (top instanceof Symbol.WriterUnionAction) {
      Symbol.Alternative branches = (Symbol.Alternative) parser.popSymbol();
      parser.pushSymbol(branches.getSymbol(in.readIndex()));
    } else if (top instanceof Symbol.ErrorAction) {
      throw new AvroTypeException(((Symbol.ErrorAction) top).msg);
    } else if (top instanceof Symbol.DefaultStartAction) {
      Symbol.DefaultStartAction dsa = (Symbol.DefaultStartAction) top;
      backup = in;
      in = new BinaryDecoder(new ByteArrayInputStream(dsa.contents));
    } else if (top == Symbol.DEFAULT_END_ACTION) {
      in = backup;
    } else {
      throw new AvroTypeException("Unknown action: " + top);
    }
    return null;
  }

  @Override
  public void skipAction() throws IOException {
    Symbol top = parser.popSymbol();
    if (top instanceof Symbol.ResolvingAction) {
      parser.pushSymbol(((Symbol.ResolvingAction) top).writer);
    } else if (top instanceof Symbol.SkipAction) {
      parser.pushSymbol(((Symbol.SkipAction) top).symToSkip);
    } else if (top instanceof Symbol.WriterUnionAction) {
      Symbol.Alternative branches = (Symbol.Alternative) parser.popSymbol();
      parser.pushSymbol(branches.getSymbol(in.readIndex()));
    } else if (top instanceof Symbol.ErrorAction) {
      throw new AvroTypeException(((Symbol.ErrorAction) top).msg);
    } else if (top instanceof Symbol.DefaultStartAction) {
      Symbol.DefaultStartAction dsa = (Symbol.DefaultStartAction) top;
      backup = in;
      in = new BinaryDecoder(new ByteArrayInputStream(dsa.contents));
    } else if (top == Symbol.DEFAULT_END_ACTION) {
      in = backup;
    }
  }
}

