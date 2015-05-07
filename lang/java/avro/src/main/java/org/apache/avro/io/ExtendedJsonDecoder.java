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
import org.apache.avro.generic.DefaultValueProvider;
import org.apache.avro.io.parsing.Symbol;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * This class extends the JsonDecoder to work with the ExtendedGenericDatumReader class to write the more
 * compact PIMCO JSON format. The AVRO codec mechanism builds a weird parse tree of the schema and then
 * iterates over that and the json string looking for the next expected field. As our extended encoding
 * deliberately skips fields that match the default we need to start mucking about with the stack when the
 * next field isn't there! More info to come as we remember how it works!
 */
public final class ExtendedJsonDecoder extends JsonDecoder {

    private static final Method ADVANCE;
    private static final Method ERROR;
    private static final Field IN;

    static {
        try {
            ADVANCE = JsonDecoder.class.getDeclaredMethod("advance", Symbol.class);
            ERROR = JsonDecoder.class.getDeclaredMethod("error", String.class);
            IN = JsonDecoder.class.getDeclaredField("in");
            ADVANCE.setAccessible(true);
            ERROR.setAccessible(true);
            IN.setAccessible(true);
        } catch (NoSuchMethodException ex) {
            throw new RuntimeException(ex);
        } catch (SecurityException ex) {
            throw new RuntimeException(ex);
        } catch (NoSuchFieldException ex) {
            throw new RuntimeException(ex);
        }
    }

    private final DefaultValueProvider provider;

    public ExtendedJsonDecoder(final Schema schema, final InputStream in, final DefaultValueProvider provider)
            throws IOException {
        super(schema, in);
        this.provider = provider;
    }

    public ExtendedJsonDecoder(final Schema schema, final String in, final DefaultValueProvider provider)
            throws IOException {
        this(schema, new ByteArrayInputStream(in.getBytes(Charset.forName("UTF-8"))), provider);
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
            ADVANCE.invoke(this, Symbol.UNION);
            JsonParser lin = getParser();
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
                throw (AvroTypeException) ERROR.invoke(this, "start-union");
            }
            int n = a.findLabel(label);
            if (n < 0) {
                throw new AvroTypeException("Unknown union branch " + label);
            }
            parser.pushSymbol(a.getSymbol(n));
            return n;
        } catch (IllegalAccessException ex) {
          throw new RuntimeException(ex);
        } catch (IllegalArgumentException ex) {
          throw new RuntimeException(ex);
        } catch (InvocationTargetException ex) {
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
            JsonParser in = getParser();
            if (top instanceof Symbol.FieldAdjustAction) {
                Symbol.FieldAdjustAction fa = (Symbol.FieldAdjustAction) top;
                String name = fa.fname;
                if (currentReorderBuffer != null) {
                    List<JsonDecoder.JsonElement> node = currentReorderBuffer.savedFields.get(name);
                    if (node != null) {
                        currentReorderBuffer.savedFields.remove(name);
                        currentReorderBuffer.origParser = in;
                        setParser(makeParser(node));
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
                    if (injectDefaultValueIfAvailable(in)) {
                        return null;
                    }
                    throw new AvroTypeException("Expected field name not found: " + fa.fname);
                } else {
                    if (injectDefaultValueIfAvailable(in)) {
                        return null;
                    }
                    throw new AvroTypeException("Expected field name not found: " + fa.fname);
                }
            } else if (top == Symbol.FIELD_END) {
                if (currentReorderBuffer != null && currentReorderBuffer.origParser != null) {
                    setParser(currentReorderBuffer.origParser);
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

    private boolean injectDefaultValueIfAvailable(final JsonParser in) throws IOException, IllegalAccessException {
        JsonNode defVal = provider.getCurrentFieldDefault();
        if (null != defVal) {
            List<JsonElement> result = new ArrayList<JsonElement>(2);
              JsonParser traverse = defVal.traverse();
              JsonToken nextToken;
              while ((nextToken = traverse.nextToken()) != null) {
                result.add(new JsonElement(nextToken));
              }
            result.add(NULL_JSON_ELEMENT);
            if (currentReorderBuffer == null) {
                currentReorderBuffer = new ReorderBuffer();
            }
            currentReorderBuffer.origParser = in;
            setParser(makeParser(result));
            return true;
        }
        return false;
    }

    private JsonParser getParser() throws IllegalAccessException {
        return (JsonParser) IN.get(this);
    }

    private void setParser(final JsonParser parser) throws IllegalAccessException {
        IN.set(this, parser);
    }
}
