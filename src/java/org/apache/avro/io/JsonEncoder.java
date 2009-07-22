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
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.parsing.JsonGrammarGenerator;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.util.Utf8;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;

/** An {@link Encoder} for Avro's JSON data encoding. */
public class JsonEncoder extends ParsingEncoder implements Parser.ActionHandler {
  final Parser parser;
  private JsonGenerator out;
  /**
   * Has anything been written into the collections?
   */
  protected BitSet isEmpty = new BitSet();

  public JsonEncoder(Schema sc, OutputStream out) throws IOException {
    this(sc, new JsonFactory().createJsonGenerator(out, JsonEncoding.UTF8));
  }

  public JsonEncoder(Schema sc, JsonGenerator out) throws IOException {
    this.out = out;
    this.parser =
      new Parser(new JsonGrammarGenerator().generate(sc), this);
  }

  @Override
  public void flush() throws IOException {
    if (parser.depth() > 1) {
      parser.advance(Symbol.END);
    }
    out.flush();
  }

  @Override
  public void init(OutputStream out) throws IOException {
    if (this.out != null) {
      flush();
    }
    this.out = new JsonFactory().createJsonGenerator(out, JsonEncoding.UTF8);
  }

  @Override
  public void writeNull() throws IOException {
    parser.advance(Symbol.NULL);
    out.writeNull();
  }

  @Override
  public void writeBoolean(boolean b) throws IOException {
    parser.advance(Symbol.BOOLEAN);
    out.writeBoolean(b);
  }

  @Override
  public void writeInt(int n) throws IOException {
    parser.advance(Symbol.INT);
    out.writeNumber(n);
  }

  @Override
  public void writeLong(long n) throws IOException {
    parser.advance(Symbol.LONG);
    out.writeNumber(n);
  }

  @Override
  public void writeFloat(float f) throws IOException {
    parser.advance(Symbol.FLOAT);
    out.writeNumber(f);
  }

  @Override
  public void writeDouble(double d) throws IOException {
    parser.advance(Symbol.DOUBLE);
    out.writeNumber(d);
  }

  @Override
  public void writeString(Utf8 utf8) throws IOException {
    parser.advance(Symbol.STRING);
    if (parser.topSymbol() == Symbol.MAP_KEY_MARKER) {
      parser.advance(Symbol.MAP_KEY_MARKER);
      out.writeFieldName(utf8.toString());
    } else {
      out.writeString(utf8.toString());
    }
  }

  @Override
  public void writeBytes(ByteBuffer bytes) throws IOException {
    if (bytes.hasArray()) {
      writeBytes(bytes.array(), bytes.position(), bytes.remaining());
    } else {
      byte[] b = new byte[bytes.remaining()];
      for (int i = 0; i < b.length; i++) {
        b[i] = bytes.get();
      }
      writeBytes(b);
    }
  }

  @Override
  public void writeBytes(byte[] bytes, int start, int len) throws IOException {
    parser.advance(Symbol.BYTES);
    writeByteArray(bytes, start, len);
  }

  private void writeByteArray(byte[] bytes, int start, int len)
      throws IOException, JsonGenerationException, UnsupportedEncodingException {
    out.writeString(
        new String(bytes, start, len, JsonDecoder.CHARSET));
  }

  @Override
  public void writeFixed(byte[] bytes, int start, int len) throws IOException {
    parser.advance(Symbol.FIXED);
    Symbol.IntCheckAction top = (Symbol.IntCheckAction) parser.popSymbol();
    if (len != top.size) {
      throw new AvroTypeException(
        "Incorrect length for fixed binary: expected " +
        top.size + " but received " + len + " bytes.");
    }
    writeByteArray(bytes, start, len);
  }

  @Override
  public void writeEnum(int e) throws IOException {
    parser.advance(Symbol.ENUM);
    Symbol.EnumLabelsAction top = (Symbol.EnumLabelsAction) parser.popSymbol();
    if (e < 0 || e >= top.size) {
      throw new AvroTypeException(
          "Enumeration out of range: max is " +
          top.size + " but received " + e);
    }
    out.writeString(top.getLabel(e));
  }

  @Override
  public void writeArrayStart() throws IOException {
    parser.advance(Symbol.ARRAY_START);
    out.writeStartArray();
    push();
    isEmpty.set(depth());
  }

  @Override
  public void writeArrayEnd() throws IOException {
    if (! isEmpty.get(pos)) {
      parser.advance(Symbol.ITEM_END);
    }
    pop();
    parser.advance(Symbol.ARRAY_END);
    out.writeEndArray();
  }

  @Override
  public void writeMapStart() throws IOException {
    push();
    isEmpty.set(depth());

    parser.advance(Symbol.MAP_START);
    out.writeStartObject();
  }

  @Override
  public void writeMapEnd() throws IOException {
    if (! isEmpty.get(pos)) {
      parser.advance(Symbol.ITEM_END);
    }
    pop();

    parser.advance(Symbol.MAP_END);
    out.writeEndObject();
  }

  @Override
  public void startItem() throws IOException {
    if (! isEmpty.get(pos)) {
      parser.advance(Symbol.ITEM_END);
    }
    super.startItem();
    isEmpty.clear(depth());
  }

  @Override
  public void writeIndex(int unionIndex) throws IOException {
    parser.advance(Symbol.UNION);
    Symbol.Alternative top = (Symbol.Alternative) parser.popSymbol();
    out.writeStartObject();
    out.writeFieldName(top.getLabel(unionIndex));
    parser.pushSymbol(Symbol.UNION_END);
    parser.pushSymbol(top.getSymbol(unionIndex));
  }

  @Override
  public Symbol doAction(Symbol input, Symbol top) throws IOException {
    if (top instanceof Symbol.FieldAdjustAction) {
      Symbol.FieldAdjustAction fa = (Symbol.FieldAdjustAction) top;
      out.writeFieldName(fa.fname);
    } else if (top == Symbol.RECORD_START) {
      out.writeStartObject();
    } else if (top == Symbol.RECORD_END || top == Symbol.UNION_END) {
      out.writeEndObject();
    } else {
      throw new AvroTypeException("Unknown action symbol " + top);
    }
    return Symbol.CONTINUE;
  }
}
