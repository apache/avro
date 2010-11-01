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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificCompiler.OutputFile;

/**
 * Copy of "legacy" Avro 1.4 Specific Compiler.
 * This is used by a test to ensure that the
 * templated version corresponds to the 
 * new version.  After the template version proves
 * itself out, it would be reasonable to remove both
 * this class and the comparison.
 * 
 * The logic to handle multiple schemas has
 * been stripped out, since this is used
 * exclusively for comparing the outputs.
 */
public class Avro14SpecificCompiler {
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

  static String mangle(String word) {
    if (RESERVED_WORDS.contains(word)) {
      return word + "$";
    }
    return word;
  }

  OutputFile compileInterface(Protocol protocol) {
    OutputFile outputFile = new OutputFile();
    String mangledName = mangle(protocol.getName());
    outputFile.path = makePath(mangledName, protocol.getNamespace());
    StringBuilder out = new StringBuilder();
    header(out, protocol.getNamespace());
    doc(out, 1, protocol.getDoc());
    line(out, 0, "public interface " + mangledName + " {");
    line(out, 1, "public static final org.apache.avro.Protocol PROTOCOL = org.apache.avro.Protocol.parse(\""
           +esc(protocol)+"\");");
    for (Map.Entry<String,Message> e : protocol.getMessages().entrySet()) {
      String name = e.getKey();
      Message message = e.getValue();
      Schema request = message.getRequest();
      String response = message.isOneWay() ? "void"
        : unbox(message.getResponse());
      doc(out, 1, e.getValue().getDoc());
      line(out, 1, response+" "+ mangle(name)+"("+params(request)+")"
           + (message.isOneWay() ? ""
              : (" throws org.apache.avro.ipc.AvroRemoteException"
                 +errors(message.getErrors())))
           +";");
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
    for (Schema.Field param : request.getFields()) {
      String paramName = mangle(param.name());
      b.append(unbox(param.schema()));
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

  OutputFile compile(Schema schema) {
    OutputFile outputFile = new OutputFile();
    String name = mangle(schema.getName());
    outputFile.path = makePath(name, schema.getNamespace());
    StringBuilder out = new StringBuilder();
    header(out, schema.getNamespace());
    switch (schema.getType()) {
    case RECORD:
      doc(out, 0, schema.getDoc());
      line(out, 0, "public class "+name+
           (schema.isError()
            ? " extends org.apache.avro.specific.SpecificExceptionBase"
             : " extends org.apache.avro.specific.SpecificRecordBase")
           +" implements org.apache.avro.specific.SpecificRecord {");
      // schema definition
      line(out, 1, "public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse(\""
           +esc(schema)+"\");");
      // field declations
      for (Schema.Field field : schema.getFields()) {
        doc(out, 1, field.doc());
        line(out, 1, "public " + unbox(field.schema()) + " "
             + mangle(field.name()) + ";");
      }
      // schema method
      line(out, 1, "public org.apache.avro.Schema getSchema() { return SCHEMA$; }");
      // get method
      line(out, 1, "// Used by DatumWriter.  Applications should not call. ");
      line(out, 1, "public java.lang.Object get(int field$) {");
      line(out, 2, "switch (field$) {");
      int i = 0;
      for (Schema.Field field : schema.getFields())
        line(out, 2, "case "+(i++)+": return "+mangle(field.name())+";");
      line(out, 2, "default: throw new org.apache.avro.AvroRuntimeException(\"Bad index\");");
      line(out, 2, "}");
      line(out, 1, "}");
      // put method
      line(out, 1, "// Used by DatumReader.  Applications should not call. ");
      line(out, 1, "@SuppressWarnings(value=\"unchecked\")");
      line(out, 1, "public void put(int field$, java.lang.Object value$) {");
      line(out, 2, "switch (field$) {");
      i = 0;
      for (Schema.Field field : schema.getFields())
        line(out, 2, "case "+(i++)+": "+mangle(field.name())+" = ("+
             type(field.schema())+")value$; break;");
      line(out, 2, "default: throw new org.apache.avro.AvroRuntimeException(\"Bad index\");");
      line(out, 2, "}");
      line(out, 1, "}");
      line(out, 0, "}");
      break;
    case ENUM:
      doc(out, 0, schema.getDoc());
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
      doc(out, 0, schema.getDoc());
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

  private void doc(StringBuilder out, int indent, String doc) {
    if (doc != null) {
      line(out, indent, "/** " + escapeForJavaDoc(doc) + " */");
    }
  }

  /** Be sure that generated code will compile by replacing
   * end-comment markers with the appropriate HTML entity. */
  private String escapeForJavaDoc(String doc) {
    return doc.replace("*/", "*&#47;");
  }

  private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);

  private String type(Schema schema) {
    switch (schema.getType()) {
    case RECORD:
    case ENUM:
    case FIXED:
      return mangle(schema.getFullName());
    case ARRAY:
      return "java.util.List<"+type(schema.getElementType())+">";
    case MAP:
      return "java.util.Map<java.lang.CharSequence,"+type(schema.getValueType())+">";
    case UNION:
      List<Schema> types = schema.getTypes();     // elide unions with null
      if ((types.size() == 2) && types.contains(NULL_SCHEMA))
        return type(types.get(types.get(0).equals(NULL_SCHEMA) ? 1 : 0));
      return "java.lang.Object";
    case STRING:  return "java.lang.CharSequence";
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

}

