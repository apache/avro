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
package org.apache.avro.data;

import java.io.IOException;
import java.util.Iterator;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.LongNode;
import org.codehaus.jackson.node.DoubleNode;
import org.codehaus.jackson.node.TextNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.NullNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.ResolvingDecoder;

/** Utilities for reading and writing arbitrary Json data in Avro format. */
public class Json {
  private Json() {}                               // singleton: no public ctor

  /** The schema for Json data. */
  public static final Schema SCHEMA;
  static {
    try {
      SCHEMA = Schema.parse
        (Json.class.getResourceAsStream("/org/apache/avro/data/Json.avsc"));
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  /** {@link DatumWriter} for arbitrary Json data. */
  public static class Writer implements DatumWriter<JsonNode> {

    @Override public void setSchema(Schema schema) {
      if (!SCHEMA.equals(schema))
        throw new RuntimeException("Not the Json schema: "+schema);
    }
    
    @Override
    public void write(JsonNode datum, Encoder out) throws IOException {
      Json.write(datum, out);
    }
  }

  /** {@link DatumReader} for arbitrary Json data. */
  public static class Reader implements DatumReader<JsonNode> {
    private Schema written;
    private ResolvingDecoder resolver;

    @Override public void setSchema(Schema schema) {
      this.written = SCHEMA.equals(written) ? null : schema;
    }

    @Override
    public JsonNode read(JsonNode reuse, Decoder in) throws IOException {
      if (written == null)                        // same schema
        return Json.read(in);

      // use a resolver to adapt alternate version of Json schema
      if (resolver == null)
        resolver = DecoderFactory.get().resolvingDecoder(written, SCHEMA, null);
      resolver.configure(in);
      JsonNode result = Json.read(resolver);
      resolver.drain();
      return result;
    }
  }

  /** Note: this enum must be kept aligned with the union in Json.avsc. */
  private enum JsonType { LONG, DOUBLE, STRING, BOOLEAN, NULL, ARRAY, OBJECT }
  
  /** Write Json data as Avro data. */
  public static void write(JsonNode node, Encoder out) throws IOException {
    switch(node.asToken()) {
    case VALUE_NUMBER_INT:
      out.writeIndex(JsonType.LONG.ordinal());
      out.writeLong(node.getLongValue());
      break;
    case VALUE_NUMBER_FLOAT:
      out.writeIndex(JsonType.DOUBLE.ordinal());
      out.writeDouble(node.getDoubleValue());
      break;
    case VALUE_STRING:
      out.writeIndex(JsonType.STRING.ordinal());
      out.writeString(node.getTextValue());
      break;
    case VALUE_TRUE:
      out.writeIndex(JsonType.BOOLEAN.ordinal());
      out.writeBoolean(true);
      break;
    case VALUE_FALSE:
      out.writeIndex(JsonType.BOOLEAN.ordinal());
      out.writeBoolean(false);
      break;
    case VALUE_NULL:
      out.writeIndex(JsonType.NULL.ordinal());
      out.writeNull();
      break;
    case START_ARRAY:
      out.writeIndex(JsonType.ARRAY.ordinal());
      out.writeArrayStart();
      out.setItemCount(node.size());
      for (JsonNode element : node) {
        out.startItem();
        write(element, out);
      }
      out.writeArrayEnd();
      break;
    case START_OBJECT:
      out.writeIndex(JsonType.OBJECT.ordinal());
      out.writeMapStart();
      out.setItemCount(node.size());
      Iterator<String> i = node.getFieldNames();
      while (i.hasNext()) {
        out.startItem();
        String name = i.next();
        out.writeString(name);
        write(node.get(name), out);
      }
      out.writeMapEnd();
      break;
    default:
      throw new AvroRuntimeException(node.asToken()+" unexpected: "+node);
    }
  }

  /** Read Json data from Avro data. */
  public static JsonNode read(Decoder in) throws IOException {
    switch (JsonType.values()[in.readIndex()]) {
    case LONG:
      return new LongNode(in.readLong());
    case DOUBLE:
      return new DoubleNode(in.readDouble());
    case STRING:
      return new TextNode(in.readString());
    case BOOLEAN:
      return in.readBoolean() ? BooleanNode.TRUE : BooleanNode.FALSE;
    case NULL:
      in.readNull();
      return NullNode.getInstance();
    case ARRAY:
      ArrayNode array = JsonNodeFactory.instance.arrayNode();
      for (long l = in.readArrayStart(); l > 0; l = in.arrayNext())
        for (long i = 0; i < l; i++)
          array.add(read(in));
      return array;
    case OBJECT:
      ObjectNode object = JsonNodeFactory.instance.objectNode();
      for (long l = in.readMapStart(); l > 0; l = in.mapNext())
        for (long i = 0; i < l; i++)
          object.put(in.readString(), read(in));
      return object;
    default:
      throw new AvroRuntimeException("Unexpected Json node type");
    }
  }

}
