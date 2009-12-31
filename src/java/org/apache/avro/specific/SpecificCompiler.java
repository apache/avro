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
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.Protocol.Message;
import org.apache.avro.tool.Tool;

/**
 * Generate specific Java interfaces and classes for protocols and schemas.
 *
 * Java reserved keywords are mangled to preserve compilation.
 */
public class SpecificCompiler {
  private final Set<Schema> queue = new HashSet<Schema>();
  private final Protocol protocol;

  /* List of Java reserved words from
   * http://java.sun.com/docs/books/jls/third_edition/html/lexical.html. */
  private static final Set<String> RESERVED_WORDS = new HashSet<String>(
      Arrays.asList(new String[] {
          "abstract", "assert", "boolean", "break", "byte", "case", "catch",
          "char", "class", "const", "continue", "default", "do", "double",
          "else", "enum", "extends", "false", "final", "finally", "float",
          "for", "goto", "if", "implements", "import", "instanceof", "int",
          "interface", "long", "native", "new", "null", "package", "private",
          "protected", "public", "return", "short", "static", "strictfp",
          "super", "switch", "synchronized", "this", "throw", "throws",
          "transient", "true", "try", "void", "volatile", "while"
        }));

  public SpecificCompiler(Protocol protocol) {
    // enqueue all types
    for (Schema s : protocol.getTypes()) {
      enqueue(s);
    }
    this.protocol = protocol;
  }

  public SpecificCompiler(Schema schema) {
    enqueue(schema);
    this.protocol = null;
  }

  /**
   * Captures output file path and contents.
   */
  static class OutputFile {
    String path;
    String contents;

    /**
     * Writes output to path destination directory, creating directories as
     * necessary.  Returns the created file.
     */
    File writeToDestination(File destDir) throws IOException {
      File f = new File(destDir, path);
      f.getParentFile().mkdirs();
      FileWriter fw = new FileWriter(f);
      try {
        fw.write(contents);
      } finally {
        fw.close();
      }
      return f;
    }
  }

  /**
   * Generates Java interface and classes for a protocol.
   * @param src the source Avro protocol file
   * @param dest the directory to place generated files in
   */
  public static void compileProtocol(File src, File dest) throws IOException {
    Protocol protocol = Protocol.parse(src);
    SpecificCompiler compiler = new SpecificCompiler(protocol);
    compiler.compileToDestination(dest);
  }

  /** Generates Java classes for a schema. */
  public static void compileSchema(File src, File dest) throws IOException {
    Schema schema = Schema.parse(src);
    SpecificCompiler compiler = new SpecificCompiler(schema);
    compiler.compileToDestination(dest);
  }

  static String mangle(String word) {
    if (RESERVED_WORDS.contains(word)) {
      return word + "$";
    }
    return word;
  }

  /** Recursively enqueue schemas that need a class generated. */
  private void enqueue(Schema schema) {
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
  Collection<OutputFile> compile() {
    List<OutputFile> out = new ArrayList<OutputFile>();
    for (Schema schema : queue) {
      out.add(compile(schema));
    }
    if (protocol != null) {
      out.add(compileInterface(protocol));
    }
    return out;
  }

  private void compileToDestination(File dst) throws IOException {
    for (Schema schema : queue) {
      OutputFile o = compile(schema);
      o.writeToDestination(dst);
    }
    if (protocol != null) {
      compileInterface(protocol).writeToDestination(dst);
    }
  }

  private OutputFile compileInterface(Protocol protocol) {
    OutputFile outputFile = new OutputFile();
    String mangledName = mangle(protocol.getName());
    outputFile.path = makePath(mangledName, protocol.getNamespace());
    StringBuilder out = new StringBuilder();
    header(out, protocol.getNamespace());
    line(out, 0, "public interface " + mangledName + " {");
    line(out, 1, "public static final org.apache.avro.Protocol PROTOCOL = org.apache.avro.Protocol.parse(\""
           +esc(protocol)+"\");");
    for (Map.Entry<String,Message> e : protocol.getMessages().entrySet()) {
      String name = e.getKey();
      Message message = e.getValue();
      Schema request = message.getRequest();
      Schema response = message.getResponse();
      line(out, 1, unbox(response)+" "+ mangle(name)+"("+params(request)+")");
      line(out, 2,"throws org.apache.avro.ipc.AvroRemoteException"+errors(message.getErrors())+";");
    }
    line(out, 0, "}");

    outputFile.contents = out.toString();
    return outputFile;
  }

  static String makePath(String name, String space) {
    if (space == null || space.isEmpty()) {
      return name + ".java";
    } else {
      return space.replace('.', File.separatorChar) + File.separatorChar
        + name + ".java";
    }
  }

  private void header(StringBuilder out, String namespace) {
    if(namespace != null) {
      line(out, 0, "package "+namespace+";\n");
    }
    line(out, 0, "@SuppressWarnings(\"all\")");
  }

  private String params(Schema request) {
    StringBuilder b = new StringBuilder();
    int count = 0;
    for (Map.Entry<String, Schema> param : request.getFieldSchemas()) {
      String paramName = mangle(param.getKey());
      b.append(unbox(param.getValue()));
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
      b.append(mangle(error.getFullName()));
    }
    return b.toString();
  }

  private OutputFile compile(Schema schema) {
    OutputFile outputFile = new OutputFile();
    String name = mangle(schema.getName());
    outputFile.path = makePath(name, schema.getNamespace());
    StringBuilder out = new StringBuilder();
    header(out, schema.getNamespace());
    switch (schema.getType()) {
    case RECORD:
      line(out, 0, "public class "+name+
           (schema.isError()
            ? " extends org.apache.avro.specific.SpecificExceptionBase"
             : " extends org.apache.avro.specific.SpecificRecordBase")
           +" implements org.apache.avro.specific.SpecificRecord {");
      // schema definition
      line(out, 1, "public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse(\""
           +esc(schema)+"\");");
      // field declations
      for (Map.Entry<String,Schema.Field> field: schema.getFields().entrySet())
        line(out, 1, "public " + unbox(field.getValue().schema()) + " "
             + mangle(field.getKey()) + ";");
      // schema method
      line(out, 1, "public org.apache.avro.Schema getSchema() { return SCHEMA$; }");
      // get method
      line(out, 1, "public java.lang.Object get(int field$) {");
      line(out, 2, "switch (field$) {");
      int i = 0;
      for (Map.Entry<String, Schema> field : schema.getFieldSchemas())
        line(out, 2, "case "+(i++)+": return "+mangle(field.getKey())+";");
      line(out, 2, "default: throw new org.apache.avro.AvroRuntimeException(\"Bad index\");");
      line(out, 2, "}");
      line(out, 1, "}");
      // set method
      line(out, 1, "@SuppressWarnings(value=\"unchecked\")");
      line(out, 1, "public void set(int field$, java.lang.Object value$) {");
      line(out, 2, "switch (field$) {");
      i = 0;
      for (Map.Entry<String, Schema> field : schema.getFieldSchemas())
        line(out, 2, "case "+(i++)+": "+mangle(field.getKey())+" = ("+
             type(field.getValue())+")value$; break;");
      line(out, 2, "default: throw new org.apache.avro.AvroRuntimeException(\"Bad index\");");
      line(out, 2, "}");
      line(out, 1, "}");
      line(out, 0, "}");
      break;
    case ENUM:
      line(out, 0, "public enum "+name+" { ");
      StringBuilder b = new StringBuilder();
      int count = 0;
      for (String symbol : schema.getEnumSymbols()) {
        b.append(mangle(symbol));
        if (++count < schema.getEnumSymbols().size())
          b.append(", ");
      }
      line(out, 1, b.toString());
      line(out, 0, "}");
      break;
    case FIXED:
      line(out, 0, "@org.apache.avro.specific.FixedSize("+schema.getFixedSize()+")");
      line(out, 0, "public class "+name+" extends org.apache.avro.specific.SpecificFixed {}");
      break;
    case MAP: case ARRAY: case UNION: case STRING: case BYTES:
    case INT: case LONG: case FLOAT: case DOUBLE: case BOOLEAN: case NULL:
      break;
    default: throw new RuntimeException("Unknown type: "+schema);
    }

    outputFile.contents = out.toString();
    return outputFile;
  }

  private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);

  private String type(Schema schema) {
    switch (schema.getType()) {
    case RECORD:
    case ENUM:
    case FIXED:
      return mangle(schema.getFullName());
    case ARRAY:
      return "org.apache.avro.generic.GenericArray<"+type(schema.getElementType())+">";
    case MAP:
      return "java.util.Map<org.apache.avro.util.Utf8,"+type(schema.getValueType())+">";
    case UNION:
      List<Schema> types = schema.getTypes();     // elide unions with null
      if ((types.size() == 2) && types.contains(NULL_SCHEMA))
        return type(types.get(types.get(0).equals(NULL_SCHEMA) ? 1 : 0));
      return "java.lang.Object";
    case STRING:  return "org.apache.avro.util.Utf8";
    case BYTES:   return "java.nio.ByteBuffer";
    case INT:     return "java.lang.Integer";
    case LONG:    return "java.lang.Long";
    case FLOAT:   return "java.lang.Float";
    case DOUBLE:  return "java.lang.Double";
    case BOOLEAN: return "java.lang.Boolean";
    case NULL:    return "java.lang.Void";
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

  private void line(StringBuilder out, int indent, String text) {
    for (int i = 0; i < indent; i ++) {
      out.append("  ");
    }
    out.append(text);
    out.append("\n");
  }

  static String esc(Object o) {
    return o.toString().replace("\"", "\\\"");
  }

  public static void main(String[] args) throws Exception {
    //compileSchema(new File(args[0]), new File(args[1]));
    compileProtocol(new File(args[0]), new File(args[1]));
  }

  /**
   * Implementation of Tool for inclusion by the "avroj"
   * runner.
   */
  public static class SpecificCompilerTool implements Tool {
    @Override
    public int run(InputStream in, PrintStream out, PrintStream err,
        List<String> args) throws Exception {
      if (args.size() != 3) {
        System.err.println("Expected 3 arguments: (schema|protocol) inputfile outputdir");
        return 1;
      }
      String method = args.get(0);
      File input = new File(args.get(1));
      File output = new File(args.get(2));
      if ("schema".equals(method)) {
        compileSchema(input, output);
      } else if ("protocol".equals(method)) {
        compileProtocol(input, output);
      } else {
        System.err.println("Expected \"schema\" or \"protocol\".");
        return 1;
      }
      return 0;
    }

    @Override
    public String getName() {
      return "compile";
    }

    @Override
    public String getShortDescription() {
      return "Generates Java code for the given schema.";
    }
  }

}

