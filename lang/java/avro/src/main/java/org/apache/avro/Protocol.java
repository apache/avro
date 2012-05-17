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
import java.io.InputStream;
import java.io.StringWriter;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;

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
 *   <li>one of either;
 *     <ul><li>one-way</li></ul>
 *   or
 *     <ul>
 *       <li><i>response</i>, the response schema;
 *       <li><i>errors</i>, an optional list of potential error schema names.
 *     </ul>
 *   </ul>
 * </ul>
 */
public class Protocol {
  /** The version of the protocol specification implemented here. */
  public static final long VERSION = 1;

  // Support properties for both Protocol and Message objects
  private static final Set<String> MESSAGE_RESERVED = new HashSet<String>();
  static {
    Collections.addAll(MESSAGE_RESERVED,
                       "doc", "response","request", "errors", "one-way");
  }

  /** A protocol message. */
  public class Message {
    private String name;
    private String doc;
    private Schema request;
    private final Schema.Props props = new Schema.Props(MESSAGE_RESERVED);

    /** Construct a message. */
    private Message(String name, String doc,
                    Map<String,String> propMap, Schema request) {
      this.name = name;
      this.doc = doc;
      this.request = request;

      if (propMap != null)                        // copy props
        for (Map.Entry<String,String> prop : propMap.entrySet())
          this.addProp(prop.getKey(), prop.getValue());
    }

    /** The name of this message. */
    public String getName() { return name; }
    /** The parameters of this message. */
    public Schema getRequest() { return request; }
    /** The returned data. */
    public Schema getResponse() { return Schema.create(Schema.Type.NULL); }
    /** Errors that might be thrown. */
    public Schema getErrors() {
      return Schema.createUnion(new ArrayList<Schema>());
    }
    
    /** Returns true if this is a one-way message, with no response or errors.*/
    public boolean isOneWay() { return true; }

    /** Return the value of the named property in this field or null. */
    public synchronized String getProp(String name) { return props.get(name); }
    /** Add a property with the given name to this field. */
    public synchronized void addProp(String name, String value) {
      props.add(name, value);
    }
    /** Return the defined properties as an unmodifieable Map. */
    public Map<String,String> getProps() {
      return Collections.unmodifiableMap(props);
    }

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
      if (doc != null) gen.writeStringField("doc", doc);
      props.write(gen);                           // write out properties
      gen.writeFieldName("request");
      request.fieldsToJson(types, gen);

      toJson1(gen);
      gen.writeEndObject();
    }

    void toJson1(JsonGenerator gen) throws IOException {
      gen.writeStringField("response", "null");
      gen.writeBooleanField("one-way", true);
    }

    public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof Message)) return false;
      Message that = (Message)o;
      return this.name.equals(that.name)
        && this.request.equals(that.request)
        && props.equals(that.props);
    }

    public int hashCode() {
      return name.hashCode() + request.hashCode() + props.hashCode();
    }

    public String getDoc() { return doc; }

  }

  private class TwoWayMessage extends Message {
    private Schema response;
    private Schema errors;
    
    /** Construct a message. */
    private TwoWayMessage(String name, String doc, Map<String,String> propMap,
                          Schema request, Schema response, Schema errors) {
      super(name, doc, propMap, request);
      this.response = response;
      this.errors = errors;
    }

    @Override public Schema getResponse() { return response; }
    @Override public Schema getErrors() { return errors; }
    @Override public boolean isOneWay() { return false; }

    @Override public boolean equals(Object o) {
      if (!super.equals(o)) return false;
      if (!(o instanceof TwoWayMessage)) return false;
      TwoWayMessage that = (TwoWayMessage)o;
      return this.response.equals(that.response)
        && this.errors.equals(that.errors);
    }

    @Override public int hashCode() {
      return super.hashCode() + response.hashCode() + errors.hashCode();
    }

    @Override void toJson1(JsonGenerator gen) throws IOException {
      gen.writeFieldName("response");
      response.toJson(types, gen);

      List<Schema> errs = errors.getTypes();  // elide system error
      if (errs.size() > 1) {
        Schema union = Schema.createUnion(errs.subList(1, errs.size()));
        gen.writeFieldName("errors");
        union.toJson(types, gen);
      }
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

  private static final Set<String> PROTOCOL_RESERVED = new HashSet<String>();
  static {
    Collections.addAll(PROTOCOL_RESERVED,
       "namespace", "protocol", "doc",
       "messages","types", "errors");
  }
  Schema.Props props = new Schema.Props(PROTOCOL_RESERVED);

  private Protocol() {}

  public Protocol(String name, String doc, String namespace) {
    this.name = name;
    this.doc = doc;
    this.namespace = namespace;
  }
  public Protocol(String name, String namespace) {
    this(name, null, namespace);
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

  /** Create a one-way message. */
  @Deprecated
  public Message createMessage(String name, String doc, Schema request) {
    return createMessage(name, doc, new LinkedHashMap<String,String>(),request);
  }
  /** Create a one-way message. */
  public Message createMessage(String name, String doc,
                               Map<String,String> propMap, Schema request) {
    return new Message(name, doc, propMap, request);
  }

  /** Create a two-way message. */
  @Deprecated
  public Message createMessage(String name, String doc, Schema request,
                               Schema response, Schema errors) {
    return createMessage(name, doc, new LinkedHashMap<String,String>(),
                         request, response, errors);
  }
  /** Create a two-way message. */
  public Message createMessage(String name, String doc,
                               Map<String,String> propMap, Schema request,
                               Schema response, Schema errors) {
    return new TwoWayMessage(name, doc, propMap, request, response, errors);
  }

  /**
   * Returns the value of the named property in this schema.
   * Returns <tt>null</tt> if there is no property with that name.
   * @param name
   */
  public synchronized String getProp(String name) {
    return props.get(name);
  }

  /**
   * Adds a property with the given name <tt>name</tt> and
   * value <tt>value</tt>. Neither <tt>name</tt> nor <tt>value</tt> can be
   * <tt>null</tt>. It is illegal to add a property if another with
   * the same name but different value already exists in this schema.
   *
   * @param name The name of the property to add
   * @param value The value for the property to add
   */
  public synchronized void addProp(String name, String value) {
    props.add(name, value);
  }

  /** Return the defined properties as an unmodifieable Map. */
  public Map<String,String> getProps() {
    return Collections.unmodifiableMap(props);
  }

  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof Protocol)) return false;
    Protocol that = (Protocol)o;
    return this.name.equals(that.name)
      && this.namespace.equals(that.namespace)
      && this.types.equals(that.types)
      && this.messages.equals(that.messages)
      && this.props.equals(that.props);
  }
  
  public int hashCode() {
    return name.hashCode() + namespace.hashCode()
      + types.hashCode() + messages.hashCode() + props.hashCode();
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

    if (doc != null) gen.writeStringField("doc", doc);
    props.write(gen);
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

  /** Read a protocol from a Json stream. */
  public static Protocol parse(InputStream stream) throws IOException {
    return parse(Schema.FACTORY.createJsonParser(stream));
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
    parseProps(json);
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

  private void parseProps(JsonNode json) {
    for (Iterator<String> i = json.getFieldNames(); i.hasNext();) {
      String p = i.next();                        // add non-reserved as props
      if (!PROTOCOL_RESERVED.contains(p)) {
        JsonNode prop = json.get(p);
        if (prop.isValueNode() && prop.isTextual())
          this.addProp(p,prop.getTextValue());
      }
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
    String doc = parseDocNode(json);

    Map<String,String> mProps = new LinkedHashMap<String,String>();
    for (Iterator<String> i = json.getFieldNames(); i.hasNext();) {
      String p = i.next();                        // add non-reserved as props
      if (!MESSAGE_RESERVED.contains(p)) {
        JsonNode prop = json.get(p);
        if (prop.isValueNode() && prop.isTextual())
          mProps.put(p,prop.getTextValue());
      }
    }

    JsonNode requestNode = json.get("request");
    if (requestNode == null || !requestNode.isArray())
      throw new SchemaParseException("No request specified: "+json);
    List<Field> fields = new ArrayList<Field>();
    for (JsonNode field : requestNode) {
      JsonNode fieldNameNode = field.get("name");
      if (fieldNameNode == null)
        throw new SchemaParseException("No param name: "+field);
      JsonNode fieldTypeNode = field.get("type");
      if (fieldTypeNode == null)
        throw new SchemaParseException("No param type: "+field);
      String name = fieldNameNode.getTextValue();
      fields.add(new Field(name, Schema.parse(fieldTypeNode,types),
                           null /* message fields don't have docs */,
                           field.get("default")));
    }
    Schema request = Schema.createRecord(fields);
    
    boolean oneWay = false;
    JsonNode oneWayNode = json.get("one-way");
    if (oneWayNode != null) {
      if (!oneWayNode.isBoolean())
        throw new SchemaParseException("one-way must be boolean: "+json);
      oneWay = oneWayNode.getBooleanValue();
    }

    JsonNode responseNode = json.get("response");
    if (!oneWay && responseNode == null)
      throw new SchemaParseException("No response specified: "+json);

    JsonNode decls = json.get("errors");

    if (oneWay) {
      if (decls != null)
        throw new SchemaParseException("one-way can't have errors: "+json);
      if (responseNode != null
          && Schema.parse(responseNode, types).getType() != Schema.Type.NULL)
        throw new SchemaParseException("One way response must be null: "+json);
      return new Message(messageName, doc, mProps, request);
    }

    Schema response = Schema.parse(responseNode, types);

    List<Schema> errs = new ArrayList<Schema>();
    errs.add(SYSTEM_ERROR);                       // every method can throw
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

    return new TwoWayMessage(messageName, doc, mProps, request, response,
                             Schema.createUnion(errs));
  }

  public static void main(String[] args) throws Exception {
    System.out.println(Protocol.parse(new File(args[0])));
  }

}

