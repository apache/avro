package org.apache.avro.io;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.parsing.Symbol;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import static org.apache.avro.io.JsonDecoder.CHARSET;
import org.apache.avro.logicalTypes.Decimal;

/**
 * This class extends the JsonDecoder to work with the ExtendedGenericDatumReader class to write the more
 * compact PIMCO JSON format. The AVRO codec mechanism builds a weird parse tree of the schema and then
 * iterates over that and the json string looking for the next expected field. As our extended encoding
 * deliberately skips fields that match the default we need to start mucking about with the stack when the
 * next field isn't there! More info to come as we remember how it works!
 */
public final class ExtendedJsonDecoder extends JsonDecoder {


    public ExtendedJsonDecoder(final Schema schema, final InputStream in)
            throws IOException {
        super(schema, in);
    }

    public ExtendedJsonDecoder(final Schema schema, final String in)
            throws IOException {
        this(schema, new ByteArrayInputStream(in.getBytes(Charset.forName("UTF-8"))));
    }

    /**
     * Overwrite this function to optime json decoding of union {null, type}.
     *
     * @return
     * @throws IOException
     */
    @Override
    public int readIndex() throws IOException {
        try {
            super.advance(Symbol.UNION);
            JsonParser lin = this.in;
            Symbol.Alternative a = (Symbol.Alternative) parser.popSymbol();

            String label;
            final JsonToken currentToken = lin.getCurrentToken();
            if (currentToken == JsonToken.VALUE_NULL) {
                label = "null";
            } else if (ExtendedJsonEncoder.isNullableSingle(a)) {
                label = ExtendedJsonEncoder.getNullableSingle(a);
            } else if (currentToken == JsonToken.START_OBJECT
                    && lin.nextToken() == JsonToken.FIELD_NAME) {
                label = lin.getText();
                lin.nextToken();
                parser.pushSymbol(Symbol.UNION_END);
            } else {
                throw (AvroTypeException) error("start-union");
            }
            int n = a.findLabel(label);
            if (n < 0) {
                throw new AvroTypeException("Unknown union branch " + label);
            }
            parser.pushSymbol(a.getSymbol(n));
            return n;
        } catch (IllegalArgumentException ex) {
          throw new RuntimeException(ex);
        }
    }

    /**
     * Overwrite to inject default values.
     *
     * @param input
     * @param top
     * @return
     * @throws IOException
     */

    @Override
    public Symbol doAction(final Symbol input, final Symbol top) throws IOException {
        try {
            JsonParser in = this.in;
            if (top instanceof Symbol.FieldAdjustAction) {
                Symbol.FieldAdjustAction fa = (Symbol.FieldAdjustAction) top;
                String name = fa.fname;
                if (currentReorderBuffer != null) {
                    List<JsonDecoder.JsonElement> node = currentReorderBuffer.savedFields.get(name);
                    if (node != null) {
                        currentReorderBuffer.savedFields.remove(name);
                        currentReorderBuffer.origParser = in;
                        this.in = makeParser(node);
                        return null;
                    }
                }
                if (in.getCurrentToken() == JsonToken.FIELD_NAME) {
                    do {
                        String fn = in.getText();
                        in.nextToken();
                        if (name.equals(fn)) {
                            return null;
                        } else {
                            if (currentReorderBuffer == null) {
                                currentReorderBuffer = new JsonDecoder.ReorderBuffer();
                            }
                            currentReorderBuffer.savedFields.put(fn, getValueAsTree(in));
                        }
                    } while (in.getCurrentToken() == JsonToken.FIELD_NAME);
                    if (injectDefaultValueIfAvailable(in, fa)) {
                        return null;
                    }
                    throw new AvroTypeException("Expected field name not found: " + fa.fname);
                } else {
                    if (injectDefaultValueIfAvailable(in, fa)) {
                        return null;
                    }
                    throw new AvroTypeException("Expected field name not found: " + fa.fname);
                }
            } else if (top == Symbol.FIELD_END) {
                if (currentReorderBuffer != null && currentReorderBuffer.origParser != null) {
                    this.in = currentReorderBuffer.origParser;
                    currentReorderBuffer.origParser = null;
                }
            } else if (top == Symbol.RECORD_START) {
                if (in.getCurrentToken() == JsonToken.START_OBJECT) {
                    in.nextToken();
                    reorderBuffers.push(currentReorderBuffer);
                    currentReorderBuffer = null;
                } else {
                    throw error("record-start");
                }
            } else if (top == Symbol.RECORD_END || top == Symbol.UNION_END) {
                if (in.getCurrentToken() == JsonToken.END_OBJECT) {
                    in.nextToken();
                    if (top == Symbol.RECORD_END) {
                        if (currentReorderBuffer != null && !currentReorderBuffer.savedFields.isEmpty()) {
                            throw error("Unknown fields: " + currentReorderBuffer.savedFields.keySet());
                        }
                        currentReorderBuffer = reorderBuffers.pop();
                    }
                } else {
                    throw error(top == Symbol.RECORD_END ? "record-end" : "union-end");
                }
            } else {
                throw new AvroTypeException("Unknown action symbol " + top);
            }
            return null;
        } catch (IllegalAccessException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static final JsonElement NULL_JSON_ELEMENT = new JsonElement(null);

    private boolean injectDefaultValueIfAvailable(final JsonParser in, Symbol.FieldAdjustAction action)
            throws IOException, IllegalAccessException {
        JsonNode defVal = action.defaultValue;
        if (null != defVal) {
            List<JsonElement> result = new ArrayList<JsonElement>(2);
              JsonParser traverse = defVal.traverse();
              JsonToken nextToken;
              while ((nextToken = traverse.nextToken()) != null) {
                if (nextToken.isScalarValue()) {
                  result.add(new JsonElement(nextToken, traverse.getText()));
                } else {
                  result.add(new JsonElement(nextToken));
                }
              }
            result.add(NULL_JSON_ELEMENT);
            if (currentReorderBuffer == null) {
                currentReorderBuffer = new ReorderBuffer();
            }
            currentReorderBuffer.origParser = in;
            this.in = makeParser(result);
            return true;
        }
        return false;
    }

  /**
   * Overwrite to support decimal json decoding
   */
  @Override
  public String readString() throws IOException {
    advance(Symbol.STRING);
    if (parser.topSymbol() == Symbol.MAP_KEY_MARKER) {
      parser.advance(Symbol.MAP_KEY_MARKER);
      if (in.getCurrentToken() != JsonToken.FIELD_NAME) {
        throw error("map-key");
      }
    } else {
      JsonToken currentToken = in.getCurrentToken();
      if (currentToken != JsonToken.VALUE_STRING && currentToken != JsonToken.VALUE_NUMBER_INT
              && currentToken != JsonToken.VALUE_NUMBER_FLOAT) {
        throw error("string");
      }
    }
    String result = in.getText();
    in.nextToken();
    return result;
  }

  /**
   * Overwrite to support decimal json decoding
   */
  @Override
  public void skipString() throws IOException {
    advance(Symbol.STRING);
    if (parser.topSymbol() == Symbol.MAP_KEY_MARKER) {
      parser.advance(Symbol.MAP_KEY_MARKER);
      if (in.getCurrentToken() != JsonToken.FIELD_NAME) {
        throw error("map-key");
      }
    } else {
      JsonToken currentToken = in.getCurrentToken();
      if (currentToken != JsonToken.VALUE_STRING && currentToken != JsonToken.VALUE_NUMBER_INT
              && currentToken != JsonToken.VALUE_NUMBER_FLOAT) {
        throw error("string");
      }
    }
    in.nextToken();
  }

  /**
   * Overwrite to support decimal json decoding
   */
  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    advance(Symbol.BYTES);
    JsonToken currentToken = in.getCurrentToken();
    switch (currentToken) {
      case VALUE_STRING:
        byte[] result = readByteArray();
        in.nextToken();
        return ByteBuffer.wrap(result);
      case VALUE_NUMBER_INT:
        BigInteger bigIntegerValue = in.getBigIntegerValue();
        in.nextToken();
        return Decimal.toBytes(bigIntegerValue);
      case VALUE_NUMBER_FLOAT:
        BigDecimal decimalValue = in.getDecimalValue();
        in.nextToken();
        return Decimal.toBytes(decimalValue);
      default:
        throw error("bytes");
    }
  }

  private byte[] readByteArray() throws IOException {
    byte[] result = in.getText().getBytes(CHARSET);
    return result;
  }

  @Override
  public void skipBytes() throws IOException {
    advance(Symbol.BYTES);
    JsonToken currentToken = in.getCurrentToken();
    if (currentToken == JsonToken.VALUE_STRING
            || currentToken == JsonToken.VALUE_NUMBER_INT || currentToken == JsonToken.VALUE_NUMBER_FLOAT) {
      in.nextToken();
    } else {
      throw error("bytes");
    }
  }


}
