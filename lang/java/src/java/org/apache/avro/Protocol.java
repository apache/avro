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
import java.io.StringWriter;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Collection;

import org.apache.avro.Schema.Field;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonGenerator;

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
    private String doc;
    private Schema request;
    private Schema response;
    private Schema errors;
    
    /** Construct a message. */
    private Message(String name, String doc, Schema request,
                    Schema response, Schema errors) {
      this.name = name;
      this.doc = doc;
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
      try {
        StringWriter writer = new StringWriter();
        JsonGenerator gen = Schema.FACTORY.createJsonGenerator(writer);
        toJson(gen);
        gen.flush();
        return writer.toString();
      } catch (IOException e) {
        throw new AvroRuntimeException(e);
      }
    }
    void toJson(JsonGenerator gen) throws IOException {
      gen.writeStartObject();

      gen.writeFieldName("request");
      request.fieldsToJson(types, gen);

      gen.writeFieldName("response");
      response.toJson(types, gen);

      List<Schema> errTypes = errors.getTypes();  // elide system error
      if (errTypes.size() > 1) {
        Schema errs = Schema.createUnion(errTypes.subList(1, errTypes.size()));
        gen.writeFieldName("errors");
        errs.toJson(types, gen);
      }

      gen.writeEndObject();
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

    public String getDoc() {
      return doc;
    }

  }

  private String name;
  private String namespace;
  private String doc;

  private Schema.Names types = new Schema.Names();
  private Map<String,Message> messages = new LinkedHashMap<String,Message>();
  private byte[] md5;

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
  
  /** Doc string for this protocol. */
  public String getDoc() { return doc; }

  /** The types of this protocol. */
  public Collection<Schema> getTypes() { return types.values(); }

  /** Returns the named type. */
  public Schema getType(String name) { return types.get(name); }

  /** Set the types of this protocol. */
  public void setTypes(Collection<Schema> newTypes) {
    types = new Schema.Names();
    for (Schema s : newTypes)
      types.add(s);
  }

  /** The messages of this protocol. */
  public Map<String,Message> getMessages() { return messages; }

  public Message createMessage(String name, String doc, Schema request,
                               Schema response, Schema errors) {
    return new Message(name, doc, request, response, errors);
  }


  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof Protocol)) return false;
    Protocol that = (Protocol)o;
    return this.name.equals(that.name)
      && this.namespace.equals(that.namespace)
      && this.types.equals(that.types)
      && this.messages.equals(that.messages);
  }
  
  public int hashCode() {
    return name.hashCode() + namespace.hashCode()
      + types.hashCode() + messages.hashCode();
  }

  /** Render this as <a href="http://json.org/">JSON</a>.*/
  @Override
  public String toString() { return toString(false); }

  /** Render this as <a href="http://json.org/">JSON</a>.
   * @param pretty if true, pretty-print JSON.
   */
  public String toString(boolean pretty) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator gen = Schema.FACTORY.createJsonGenerator(writer);
      if (pretty) gen.useDefaultPrettyPrinter();
      toJson(gen);
      gen.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }
  void toJson(JsonGenerator gen) throws IOException {
    types.space(namespace);

    gen.writeStartObject();
    gen.writeStringField("protocol", name);
    gen.writeStringField("namespace", namespace);
    
    gen.writeArrayFieldStart("types");
    Schema.Names resolved = new Schema.Names(namespace);
    for (Schema type : types.values())
      if (!resolved.contains(type))
        type.toJson(resolved, gen);
    gen.writeEndArray();
    
    gen.writeObjectFieldStart("messages");
    for (Map.Entry<String,Message> e : messages.entrySet()) {
      gen.writeFieldName(e.getKey());
      e.getValue().toJson(gen);
    }
    gen.writeEndObject();
    gen.writeEndObject();
  }

  /** Return the MD5 hash of the text of this protocol. */
  public byte[] getMD5() {
    if (md5 == null)
      try {
        md5 = MessageDigest.getInstance("MD5")
          .digest(this.toString().getBytes("UTF-8"));
      } catch (Exception e) {
        throw new AvroRuntimeException(e);
      }
    return md5;
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
      protocol.parse(Schema.MAPPER.readTree(parser));
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
    parseDoc(json);
  }

  private void parseNamespace(JsonNode json) {
    JsonNode nameNode = json.get("namespace");
    if (nameNode == null) return;                 // no namespace defined
    this.namespace = nameNode.getTextValue();
    types.space(this.namespace);
  }
  
  private void parseDoc(JsonNode json) {
    this.doc = parseDocNode(json);
  }

  private String parseDocNode(JsonNode json) {
    JsonNode nameNode = json.get("doc");
    if (nameNode == null) return null;                 // no doc defined
    return nameNode.getTextValue();
  }

  private void parseName(JsonNode json) {
    JsonNode nameNode = json.get("protocol");
    if (nameNode == null)
      throw new SchemaParseException("No protocol name specified: "+json);
    this.name = nameNode.getTextValue();
  }

  private void parseTypes(JsonNode json) {
    JsonNode defs = json.get("types");
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
    JsonNode defs = json.get("messages");
    if (defs == null) return;                    // no messages defined
    for (Iterator<String> i = defs.getFieldNames(); i.hasNext();) {
      String prop = i.next();
      this.messages.put(prop, parseMessage(prop, defs.get(prop)));
    }
  }

  private Message parseMessage(String messageName, JsonNode json) {
    JsonNode requestNode = json.get("request");
    if (requestNode == null || !requestNode.isArray())
      throw new SchemaParseException("No request specified: "+json);
    LinkedHashMap<String,Field> fields = new LinkedHashMap<String,Field>();
    for (JsonNode field : requestNode) {
      JsonNode fieldNameNode = field.get("name");
      if (fieldNameNode == null)
        throw new SchemaParseException("No param name: "+field);
      JsonNode fieldTypeNode = field.get("type");
      if (fieldTypeNode == null)
        throw new SchemaParseException("No param type: "+field);
      String name = fieldNameNode.getTextValue();
      fields.put(name,
          new Field(name, Schema.parse(fieldTypeNode,types),
                           null /* message fields don't have docs */,
                           field.get("default")));
    }
    Schema request = Schema.createRecord(fields);
    
    JsonNode responseNode = json.get("response");
    if (responseNode == null)
      throw new SchemaParseException("No response specified: "+json);
    Schema response = Schema.parse(responseNode, types);

    List<Schema> errs = new ArrayList<Schema>();
    errs.add(SYSTEM_ERROR);                       // every method can throw
    JsonNode decls = json.get("errors");
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
    String doc = parseDocNode(json);
    return new Message(messageName, doc, request, response,
                       Schema.createUnion(errs));
  }

  public static void main(String[] args) throws Exception {
    System.out.println(Protocol.parse(new File(args[0])));
  }

}

