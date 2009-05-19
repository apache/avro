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
package org.apache.avro;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.JsonNode;

import org.apache.avro.Schema.Field;

/** A set of messages forming an application protocol.
 * <p> A protocol consists of:
 * <ul>
 * <li>a <i>name</i> for the protocol;
 * <li>an optional <i>namespace</i>, further qualifying the name;
 * <li>a list of <i>types</i>, or named {@link Schema schemas};
 * <li>a list of <i>errors</i>, or named {@link Schema schemas} for exceptions;
 * <li>a list of named <i>messages</i>, each of which specifies,
 *   <ul>
 *   <li><i>request</i>, the parameter schemas;
 *   <li><i>response</i>, the response schema;
 *   <li><i>errors</i>, a list of potential error schema names.
 *   </ul>
 * </ul>
 */
public class Protocol {
  /** The version of the protocol specification implemented here. */
  public static final long VERSION = 1;

  /** A protocol message. */
  public class Message {
    private String name;
    private Schema request;
    private Schema response;
    private Schema errors;
    
    /** Construct a message. */
    private Message(String name, Schema request,
                    Schema response, Schema errors) {
      this.name = name;
      this.request = request;
      this.response = response;
      this.errors = errors;
    }

    /** The name of this message. */
    public String getName() { return name; }
    /** The parameters of this message. */
    public Schema getRequest() { return request; }
    /** The returned data. */
    public Schema getResponse() { return response; }
    /** Errors that might be thrown. */
    public Schema getErrors() { return errors; }
    
    public String toString() {
      StringBuilder buffer = new StringBuilder();
      buffer.append("{\"request\": [");
      int count = 0;
      for (Map.Entry<String, Schema> entry : request.getFieldSchemas()) {
        buffer.append("{\"name\": \"");
        buffer.append(entry.getKey());
        buffer.append("\", \"type\": ");
        buffer.append(entry.getValue().toString(types));
        buffer.append("}");
        if (++count < request.getFields().size())
          buffer.append(", ");
      }
      buffer.append("], \"response\": "+response.toString(types));

      List<Schema> errTypes = errors.getTypes();  // elide system error
      if (errTypes.size() > 1) {
        Schema errs = Schema.createUnion(errTypes.subList(1, errTypes.size()));
        buffer.append(", \"errors\": "+errs.toString(types));
      }

      buffer.append("}");
      return buffer.toString();
    }

    public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof Message)) return false;
      Message that = (Message)o;
      return this.name.equals(that.name)
        && this.request.equals(that.request)
        && this.response.equals(that.response)
        && this.errors.equals(that.errors);
    }

    public int hashCode() {
      return name.hashCode()
        + request.hashCode() + response.hashCode() + errors.hashCode();
    }

  }

  private String name;
  private String namespace;

  private Schema.Names types = new Schema.Names();
  private Map<String,Message> messages = new LinkedHashMap<String,Message>();

  /** An error that can be thrown by any message. */
  public static final Schema SYSTEM_ERROR = Schema.create(Schema.Type.STRING);

  /** Union type for generating system errors. */
  public static final Schema SYSTEM_ERRORS;
  static {
    List<Schema> errors = new ArrayList<Schema>();
    errors.add(SYSTEM_ERROR);
    SYSTEM_ERRORS = Schema.createUnion(errors);
  }

  private Protocol() {}

  public Protocol(String name, String namespace) {
    this.name = name;
    this.namespace = namespace;
  }

  /** The name of this protocol. */
  public String getName() { return name; }

  /** The namespace of this protocol.  Qualifies its name. */
  public String getNamespace() { return namespace; }

  /** The types of this protocol. */
  public LinkedHashMap<String,Schema> getTypes() { return types; }

  /** The messages of this protocol. */
  public Map<String,Message> getMessages() { return messages; }

  public Message createMessage(String name, Schema request,
                               Schema response, Schema errors) {
    return new Message(name, request, response, errors);
  }


  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof Protocol)) return false;
    Protocol that = (Protocol)o;
    return this.name.equals(that.name)
      && this.namespace.equals(that.namespace)
      && this.messages.equals(that.messages);
  }
  
  public int hashCode() {
    return name.hashCode() + namespace.hashCode() + messages.hashCode();
  }

  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("{\n");
    buffer.append("\"protocol\": \""+name+"\", \n");
    buffer.append("\"namespace\": \""+namespace+"\", \n");
    buffer.append("\"types\": [\n");
    int count = 0;
    int size = types.size();
    for (Schema type : types.values()) {
      buffer.append(type.toString(types.except(type.getName()))+"\n");
      if (++count < size) buffer.append(",\n");
    }
    buffer.append("], \"messages\": {\n");
    count = 0;
    for (Map.Entry<String,Message> e : messages.entrySet()) {
      buffer.append("\""+e.getKey()+"\": "+e.getValue());
      if (++count < messages.size())
        buffer.append(",\n");
    }
    buffer.append("}\n}");
    return buffer.toString();
  }

  /** Read a protocol from a Json file. */
  public static Protocol parse(File file) throws IOException {
    return parse(Schema.FACTORY.createJsonParser(file));
  }

  /** Read a protocol from a Json string. */
  public static Protocol parse(String string) {
    try {
      return parse(Schema.FACTORY.createJsonParser
                   (new ByteArrayInputStream(string.getBytes("UTF-8"))));
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  private static Protocol parse(JsonParser parser) {
    try {
      Protocol protocol = new Protocol();
      protocol.parse(Schema.MAPPER.read(parser));
      return protocol;
    } catch (IOException e) {
      throw new SchemaParseException(e);
    }
  }

  private void parse(JsonNode json) {
    parseNamespace(json);
    parseName(json);
    parseTypes(json);
    parseMessages(json);
  }

  private void parseNamespace(JsonNode json) {
    JsonNode nameNode = json.getFieldValue("namespace");
    if (nameNode == null) return;                 // no namespace defined
    this.namespace = nameNode.getTextValue();
    types.space(this.namespace);
  }

  private void parseName(JsonNode json) {
    JsonNode nameNode = json.getFieldValue("protocol");
    if (nameNode == null)
      throw new SchemaParseException("No protocol name specified: "+json);
    this.name = nameNode.getTextValue();
  }

  private void parseTypes(JsonNode json) {
    JsonNode defs = json.getFieldValue("types");
    if (defs == null) return;                    // no types defined
    if (!defs.isArray())
      throw new SchemaParseException("Types not an array: "+defs);
    for (JsonNode type : defs) {
      if (!type.isObject())
        throw new SchemaParseException("Type not an object: "+type);
      Schema.parse(type, types);
    }
  }

  private void parseMessages(JsonNode json) {
    JsonNode defs = json.getFieldValue("messages");
    if (defs == null) return;                    // no messages defined
    for (Iterator<String> i = defs.getFieldNames(); i.hasNext();) {
      String prop = i.next();
      this.messages.put(prop, parseMessage(prop, defs.getFieldValue(prop)));
    }
  }

  private Message parseMessage(String messageName, JsonNode json) {
    JsonNode requestNode = json.getFieldValue("request");
    if (requestNode == null || !requestNode.isArray())
      throw new SchemaParseException("No request specified: "+json);
    LinkedHashMap<String,Field> fields = new LinkedHashMap<String,Field>();
    for (JsonNode field : requestNode) {
      JsonNode fieldNameNode = field.getFieldValue("name");
      if (fieldNameNode == null)
        throw new SchemaParseException("No param name: "+field);
      JsonNode fieldTypeNode = field.getFieldValue("type");
      if (fieldTypeNode == null)
        throw new SchemaParseException("No param type: "+field);
      fields.put(fieldNameNode.getTextValue(),
                 new Field(Schema.parse(fieldTypeNode,types),
                           field.getFieldValue("default")));
    }
    Schema request = Schema.createRecord(fields);
    
    JsonNode responseNode = json.getFieldValue("response");
    if (responseNode == null)
      throw new SchemaParseException("No response specified: "+json);
    Schema response = Schema.parse(responseNode, types);

    List<Schema> errs = new ArrayList<Schema>();
    errs.add(SYSTEM_ERROR);                       // every method can throw
    JsonNode decls = json.getFieldValue("errors");
    if (decls != null) {
      if (!decls.isArray())
        throw new SchemaParseException("Errors not an array: "+json);
      for (JsonNode decl : decls) {
        String name = decl.getTextValue();
        Schema schema = this.types.get(name);
        if (schema == null)
          throw new SchemaParseException("Undefined error: "+name);
        if (!schema.isError())
          throw new SchemaParseException("Not an error: "+name);
        errs.add(schema);
      }
    }
    return new Message(messageName, request, response,
                       Schema.createUnion(errs));
  }

  public static void main(String args[]) throws Exception {
    System.out.println(Protocol.parse(new File(args[0])));
  }

}
