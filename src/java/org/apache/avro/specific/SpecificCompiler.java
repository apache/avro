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
import java.io.FileOutputStream;
import java.io.Writer;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.HashSet;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.Protocol.Message;

/** Generate specific Java interfaces and classes for protocols and schemas. */
public class SpecificCompiler {
  private File dest;
  private Writer out;
  private Set<Schema> queue = new HashSet<Schema>();

  private SpecificCompiler(File dest) {
    this.dest = dest;                             // root directory for output
  }

  /** Generates Java interface and classes for a protocol.
   * @param src the source Avro protocol file
   * @param dest the directory to place generated files in
   */
  public static void compileProtocol(File src, File dest) throws IOException {
    SpecificCompiler compiler = new SpecificCompiler(dest);
    Protocol protocol = Protocol.parse(src);
    for (Schema s : protocol.getTypes())          // enqueue types
      compiler.enqueue(s);
    compiler.compileInterface(protocol);          // generate interface
    compiler.compile();                           // generate classes for types
  }

  /** Generates Java classes for a schema. */
  public static void compileSchema(File src, File dest) throws IOException {
    SpecificCompiler compiler = new SpecificCompiler(dest);
    compiler.enqueue(Schema.parse(src));          // enqueue types
    compiler.compile();                           // generate classes for types
  }

  /** Recursively enqueue schemas that need a class generated. */
  private void enqueue(Schema schema) throws IOException {
    if (queue.contains(schema)) return;
    switch (schema.getType()) {
    case RECORD:
      queue.add(schema);
      for (Map.Entry<String, Schema> field : schema.getFieldSchemas())
        enqueue(field.getValue());
      break;
    case MAP:
      enqueue(schema.getValueType());
      break;
    case ARRAY:
      enqueue(schema.getElementType());
      break;
    case UNION:
      for (Schema s : schema.getTypes())
        enqueue(s);
      break;
    case ENUM:
    case FIXED:
      queue.add(schema);
      break;
    case STRING: case BYTES:
    case INT: case LONG:
    case FLOAT: case DOUBLE:
    case BOOLEAN: case NULL:
      break;
    default: throw new RuntimeException("Unknown type: "+schema);
    }
  }

  /** Generate java classes for enqueued schemas. */
  private void compile() throws IOException {
    for (Schema schema : queue)
      compile(schema);
  }

  private void compileInterface(Protocol protocol) throws IOException {
    startFile(protocol.getName(), protocol.getNamespace());
    try {
      line(0, "public interface "+protocol.getName()+" {");

      out.append("\n");
      for (Map.Entry<String,Message> e : protocol.getMessages().entrySet()) {
        String name = e.getKey();
        Message message = e.getValue();
        Schema request = message.getRequest();
        Schema response = message.getResponse();
        line(1, type(response)+" "+name+"("+params(request)+")");
        line(2,"throws AvroRemoteException"+errors(message.getErrors())+";");
      }
      line(0, "}");
    } finally {
      out.close();
    }
  }

  private void startFile(String name, String space) throws IOException {
    File dir = new File(dest, space.replace('.', File.separatorChar));
    if (!dir.exists())
      if (!dir.mkdirs())
        throw new IOException("Unable to create " + dir);
    name = cap(name) + ".java";
    out = new OutputStreamWriter(new FileOutputStream(new File(dir, name)));
    header(space);
  }

  private void header(String namespace) throws IOException {
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
    line(0, "import org.apache.avro.specific.SpecificExceptionBase;");
    line(0, "import org.apache.avro.specific.SpecificRecordBase;");
    line(0, "import org.apache.avro.specific.SpecificRecord;");
    line(0, "import org.apache.avro.specific.SpecificFixed;");
    line(0, "import org.apache.avro.reflect.FixedSize;");
    for (Schema s : queue)
      if (namespace == null
          ? (s.getNamespace() != null)
          : !namespace.equals(s.getNamespace()))
        line(0, "import "+SpecificData.get().getClassName(s)+";");
    line(0, "");
  }

  private String params(Schema request) throws IOException {
    StringBuilder b = new StringBuilder();
    int count = 0;
    for (Map.Entry<String, Schema> param : request.getFieldSchemas()) {
      String paramName = param.getKey();
      b.append(type(param.getValue()));
      b.append(" ");
      b.append(paramName);
      if (++count < request.getFields().size())
        b.append(", ");
    }
    return b.toString();
  }

  private String errors(Schema errs) throws IOException {
    StringBuilder b = new StringBuilder();
    for (Schema error : errs.getTypes().subList(1, errs.getTypes().size())) {
      b.append(", ");
      b.append(error.getName());
    }
    return b.toString();
  }

  private void compile(Schema schema) throws IOException {
    startFile(schema.getName(), schema.getNamespace());
    try {
      switch (schema.getType()) {
      case RECORD:
        line(0, "public class "+type(schema)+
             (schema.isError()
              ? " extends SpecificExceptionBase"
               : " extends SpecificRecordBase")
             +" implements SpecificRecord {");
        // schema definition
        line(1, "public static final Schema _SCHEMA = Schema.parse(\""
             +esc(schema)+"\");");
        // field declations
        for (Map.Entry<String, Schema> field : schema.getFieldSchemas())
          line(1,"public "+unbox(field.getValue())+" "+field.getKey()+";");
        // schema method
        line(1, "public Schema getSchema() { return _SCHEMA; }");
        // get method
        line(1, "public Object get(int _field) {");
        line(2, "switch (_field) {");
        int i = 0;
        for (Map.Entry<String, Schema> field : schema.getFieldSchemas())
          line(2, "case "+(i++)+": return "+field.getKey()+";");
        line(2, "default: throw new AvroRuntimeException(\"Bad index\");");
        line(2, "}");
        line(1, "}");
        // set method
        line(1, "@SuppressWarnings(value=\"unchecked\")");
        line(1, "public void set(int _field, Object _value) {");
        line(2, "switch (_field) {");
        i = 0;
        for (Map.Entry<String, Schema> field : schema.getFieldSchemas())
          line(2, "case "+(i++)+": "+field.getKey()+" = ("+
               type(field.getValue())+")_value; break;");
        line(2, "default: throw new AvroRuntimeException(\"Bad index\");");
        line(2, "}");
        line(1, "}");
        line(0, "}");
        break;
      case ENUM:
        line(0, "public enum "+type(schema)+" { ");
        StringBuilder b = new StringBuilder();
        int count = 0;
        for (String symbol : schema.getEnumSymbols()) {
          b.append(symbol);
          if (++count < schema.getEnumSymbols().size())
            b.append(", ");
        }
        line(1, b.toString());
        line(0, "}");
        break;
      case FIXED:
        line(0, "@FixedSize("+schema.getFixedSize()+")");
        line(0, "public class "+type(schema)+" extends SpecificFixed {}");
        break;
      case MAP: case ARRAY: case UNION: case STRING: case BYTES:
      case INT: case LONG: case FLOAT: case DOUBLE: case BOOLEAN: case NULL:
        break;
      default: throw new RuntimeException("Unknown type: "+schema);
      }
    } finally {
      out.close();
    }
  }

  private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);

  private String type(Schema schema) {
    switch (schema.getType()) {
    case RECORD:
    case ENUM:
    case FIXED:
      return schema.getName();
    case ARRAY:
      return "GenericArray<"+type(schema.getElementType())+">";
    case MAP:
      return "Map<Utf8,"+type(schema.getValueType())+">";
    case UNION:
      List<Schema> types = schema.getTypes();     // elide unions with null
      if ((types.size() == 2) && types.contains(NULL_SCHEMA))
        return type(types.get(types.get(0).equals(NULL_SCHEMA) ? 1 : 0));
      return "Object";
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

  private String unbox(Schema schema) {
    switch (schema.getType()) {
    case INT:     return "int";
    case LONG:    return "long";
    case FLOAT:   return "float";
    case DOUBLE:  return "double";
    case BOOLEAN: return "boolean";
    default:      return type(schema);
    }
  }

  private void line(int indent, String text) throws IOException {
    for (int i = 0; i < indent; i ++) {
      out.append("  ");
    }
    out.append(text);
    out.append("\n");
  }

  static String cap(String name) {
    return name.substring(0,1).toUpperCase()+name.substring(1,name.length());
  }

  private static String esc(Object o) {
    return o.toString().replace("\"", "\\\"");
  }

  public static void main(String[] args) throws Exception {
    //compileSchema(new File(args[0]), new File(args[1]));
    compileProtocol(new File(args[0]), new File(args[1]));
  }

}

