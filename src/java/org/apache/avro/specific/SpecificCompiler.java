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
package org.apache.avro.specific;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.Protocol.Message;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.JsonTypeMapper;

/** Generate specific Java interfaces and classes for protocols and schemas. */
public class SpecificCompiler {
  private static final JsonTypeMapper MAPPER = new JsonTypeMapper();
  private static final JsonFactory FACTORY = new JsonFactory();

  private String namespace;
  private StringBuilder buffer = new StringBuilder();

  private SpecificCompiler() {}                        // no public ctor

  /** Returns generated Java interface for a protocol. */
  public static SpecificCompiler compileProtocol(File file) throws IOException {
    SpecificCompiler compiler = new SpecificCompiler();
    Protocol protocol = Protocol.parse(file);
    compiler.compile(protocol);
    return compiler;
  }

  /** Returns generated Java class for a schema. */
  public static SpecificCompiler compileSchema(File file) throws IOException {
    SpecificCompiler compiler = new SpecificCompiler();
    Schema schema = Schema.parse(file);
    compiler.header(schema.getNamespace());
    compiler.namespace = schema.getNamespace();
    compiler.compile(schema, schema.getName(), 0);
    return compiler;
  }

  /** Return namespace for compiled code. */
  public String getNamespace() { return namespace; }

  /** Return generated code. */
  public String getCode() { return buffer.toString(); }
  
  private void compile(Protocol protocol) {
    namespace = protocol.getNamespace();
    header(namespace);

    // define an interface
    line(0, "public interface "+protocol.getName()+" {");

    // nest type classes
    for (Schema schema : protocol.getTypes().values())
      compile(schema, schema.getName(), 1);

    // define methods
    buffer.append("\n");
    for (Map.Entry<String,Message> entry : protocol.getMessages().entrySet()) {
      String name = entry.getKey();
      Message message = entry.getValue();
      Schema request = message.getRequest();
      Schema response = message.getResponse();
      line(1, type(response, name+"Return")+" "+name+"("+params(request)+")");
      line(2,"throws AvroRemoteException"+errors(message.getErrors())+";");
    }
    line(0, "}");
  }

  private void header(String namespace) {
    if(namespace != null) {
      line(0, "package "+namespace+";\n");
    }
    line(0, "import java.nio.ByteBuffer;");
    line(0, "import java.util.Map;");
    line(0, "import org.apache.avro.Protocol;");
    line(0, "import org.apache.avro.Schema;");
    line(0, "import org.apache.avro.AvroRuntimeException;");
    line(0, "import org.apache.avro.Protocol;");
    line(0, "import org.apache.avro.util.Utf8;");
    line(0, "import org.apache.avro.ipc.AvroRemoteException;");
    line(0, "import org.apache.avro.generic.GenericArray;");
    line(0, "import org.apache.avro.specific.SpecificRecord;");
    line(0, "import org.apache.avro.specific.SpecificFixed;");
    line(0, "import org.apache.avro.reflect.FixedSize;");
    buffer.append("\n");
  }

  private String params(Schema request) {
    StringBuilder b = new StringBuilder();
    int count = 0;
    for (Map.Entry<String, Schema> param : request.getFieldSchemas()) {
      String paramName = param.getKey();
      b.append(type(param.getValue(), paramName));
      b.append(" ");
      b.append(paramName);
      if (++count < request.getFields().size())
        b.append(", ");
    }
    return b.toString();
  }

  private String errors(Schema errs) {
    StringBuilder b = new StringBuilder();
    for (Schema error : errs.getTypes().subList(1, errs.getTypes().size())) {
      b.append(", ");
      b.append(error.getName());
    }
    return b.toString();
  }

  private void compile(Schema schema, String name, int d) {
    String type = type(schema, name);
    switch (schema.getType()) {
    case RECORD:
      buffer.append("\n");
      line(d, ((d==0)?"public ":"")
           +((d>1)?"static ":"")+"class "+type
           +(schema.isError()?" extends AvroRemoteException":"")
           +" implements SpecificRecord {");
      // schema definition
      line(d+1, "private static final Schema _SCHEMA = Schema.parse(\""
           +esc(schema)+"\");");
      // field declations
      for (Map.Entry<String, Schema> field : schema.getFieldSchemas()) {
        String fieldName = field.getKey();
        line(d+1, "public "+type(field.getValue(),fieldName)+" "+fieldName+";");
      }
      // schema method
      line(d+1, "public Schema schema() { return _SCHEMA; }");
      // get method
      line(d+1, "public Object get(int _field) {");
      line(d+2, "switch (_field) {");
      int i = 0;
      for (Map.Entry<String, Schema> field : schema.getFieldSchemas())
        line(d+2, "case "+(i++)+": return "+field.getKey()+";");
      line(d+2, "default: throw new AvroRuntimeException(\"Bad index\");");
      line(d+2, "}");
      line(d+1, "}");
      // set method
      line(d+1, "@SuppressWarnings(value=\"unchecked\")");
      line(d+1, "public void set(int _field, Object _value) {");
      line(d+2, "switch (_field) {");
      i = 0;
      for (Map.Entry<String, Schema> field : schema.getFieldSchemas())
        line(d+2, "case "+(i++)+": "+field.getKey()+" = ("+
             type(field.getValue(),field.getKey())+")_value; break;");
      line(d+2, "default: throw new AvroRuntimeException(\"Bad index\");");
      line(d+2, "}");
      line(d+1, "}");
      line(d, "}");

      // nested classes
      if (d == 0)
        for (Map.Entry<String, Schema> field : schema.getFieldSchemas())
          compile(field.getValue(), null, d+1);

      break;
    case ENUM:
      buffer.append("\n");
      line(d, ((d==0)?"public ":"")+"enum "+type+" { ");
      StringBuilder b = new StringBuilder();
      int count = 0;
      for (String symbol : schema.getEnumSymbols()) {
        b.append(symbol);
        if (++count < schema.getEnumSymbols().size())
          b.append(", ");
      }
      line(d+1, b.toString());
      line(d, "}");
      break;
    case ARRAY:
      compile(schema.getElementType(), name+"Element", d);
      break;
    case MAP:
      compile(schema.getValueType(), name+"Value", d);
      break;
    case FIXED:
      buffer.append("\n");
      line(d, "@FixedSize("+schema.getFixedSize()+")");
      line(d, ((d==0)?"public ":"")
           +((d>1)?"static ":"")+"class "+type
           +" extends SpecificFixed {}");
      break;
    case UNION:
      int choice = 0;
      for (Schema t : schema.getTypes())
        compile(t, name+"Choice"+choice++, d);
      break;

    case STRING: case BYTES:
    case INT: case LONG:
    case FLOAT: case DOUBLE:
    case BOOLEAN: case NULL:
      break;
    default: throw new RuntimeException("Unknown type: "+schema);
    }
  }

  private String type(Schema schema, String name) {
    switch (schema.getType()) {
    case RECORD:
    case ENUM:
    case FIXED:
      return schema.getName() == null ? cap(name) : schema.getName();
    case ARRAY:
      return "GenericArray<"+type(schema.getElementType(),name+"Element")+">";
    case MAP:
      return "Map<Utf8,"+type(schema.getValueType(),name+"Value")+">";
    case UNION:   return "Object";
    case STRING:  return "Utf8";
    case BYTES:   return "ByteBuffer";
    case INT:     return "Integer";
    case LONG:    return "Long";
    case FLOAT:   return "Float";
    case DOUBLE:  return "Double";
    case BOOLEAN: return "Boolean";
    case NULL:    return "Void";
    default: throw new RuntimeException("Unknown type: "+schema);
    }
  }

  private void line(int indent, String text) {
    for (int i = 0; i < indent; i ++) {
      buffer.append("  ");
    }
    buffer.append(text);
    buffer.append("\n");
  }

  static String cap(String name) {
    return name.substring(0,1).toUpperCase()+name.substring(1,name.length());
  }

  private static String esc(Object o) {
    return o.toString().replace("\"", "\\\"");
  }

  public static void main(String args[]) throws Exception {
    System.out.println(compileProtocol(new File(args[0])).getCode());
  }

}
