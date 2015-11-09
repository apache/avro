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
package org.apache.avro.compiler.specific;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.velocity.VelocityContext;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Generate specific Java interfaces and classes for protocols and schemas.
 *
 * In addition to generating avro pojos, this compiler also generates interfaces.
 *
 * Java reserved keywords are mangled to preserve compilation.
 */
public class CommonCompiler extends SpecificCompiler {

  public CommonCompiler(Protocol protocol) {
    this();
    // enqueue all types
    for (Schema s : protocol.getTypes()) {
      enqueue(s);
    }
    this.protocol = protocol;
  }

  public CommonCompiler(Schema schema) {
    this();
    enqueue(schema);
    this.protocol = null;
  }

  public CommonCompiler() {
    this.templateDir =
            System.getProperty("org.apache.avro.specific.templates",
                    "/org/apache/avro/compiler/specific/templates/java/interfaces/");
    initializeVelocity();
  }

  public String getStringType() {
    return getStringType(null);
  }

  public String getTemplateDir() {
    return templateDir;
  }

  @Override
  OutputFile compileInterface(Protocol protocol) {
    String content = renderInterface(protocol, templateDir);
    String name = mangle(protocol.getName());
    String location = "avro" + File.separatorChar + makePath(name, protocol.getNamespace());
    return makeOutput(content, location);
  }

  @Override
  List<OutputFile> compile(Schema schema) {
    final List<OutputFile> outputs;
    String content, name, location;
    switch (schema.getType()) {
      case RECORD:
      case ENUM:
      case FIXED:
        String filePrefix = schema.getType() == Schema.Type.ENUM ? "" : "I";
        outputs = new ArrayList<OutputFile>(2);

        content = renderSchema(schema, templateDir + "avro" + File.separatorChar);
        name = mangle(schema.getName());
        location = "avro" + File.separatorChar + makePath(name, schema.getNamespace());
        outputs.add(makeOutput(content, location));

        content = renderSchema(schema, templateDir + "common" + File.separatorChar);
        name = filePrefix +  mangle(schema.getName());
        location = "common" + File.separatorChar + makePath(name, schema.getNamespace());
        outputs.add(makeOutput(content, location));
        break;
      default:
        return super.compile(schema);
    }
    return outputs;
  }

  /** Utility for template use.  Returns the java interface type for a Schema. */
  public String ifaceType(Schema schema) {
    switch (schema.getType()) {
      case RECORD:
      case FIXED:
        return mangle((schema.getNamespace() == null) ? "I"+schema.getName() : schema.getNamespace()+".common.I"+schema.getName());
      case ENUM:
        return mangle((schema.getNamespace() == null) ? schema.getName() : schema.getNamespace()+".common."+schema.getName());
      case ARRAY:
        return "java.util.List<" + ifaceCovariantType(schema.getElementType()) + ">";
      case MAP:
        return "java.util.Map<"
                + getStringType(schema.getJsonProp(SpecificData.KEY_CLASS_PROP))+","
                + ifaceCovariantType(schema.getValueType()) + ">";
      case UNION:
        List<Schema> types = schema.getTypes(); // elide unions with null
        if ((types.size() == 2) && types.contains(NULL_SCHEMA))
          return ifaceType(types.get(types.get(0).equals(NULL_SCHEMA) ? 1 : 0));
        return "java.lang.Object";
      case STRING:
        return getStringType(schema.getJsonProp(SpecificData.CLASS_PROP));
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

  protected String ifaceCovariantType(Schema schema) {
    switch (schema.getType()) {
      case RECORD:
      case FIXED:
        return "? extends " + mangle((schema.getNamespace() == null) ? "I"+schema.getName() : schema.getNamespace()+".common.I"+schema.getName());
      case ENUM:
        return "? extends " + mangle((schema.getNamespace() == null) ? schema.getName() : schema.getNamespace()+".common."+schema.getName());
      case ARRAY:
        return "? extends java.util.List<" + ifaceCovariantType(schema.getElementType()) + ">";
      case MAP:
        return "? extends java.util.Map<"
                + getStringType(schema.getJsonProp(SpecificData.KEY_CLASS_PROP))+","
                + ifaceCovariantType(schema.getValueType()) + ">";
      case UNION:
        List<Schema> types = schema.getTypes(); // elide unions with null
        if ((types.size() == 2) && types.contains(NULL_SCHEMA))
          return "? extends " + ifaceType(types.get(types.get(0).equals(NULL_SCHEMA) ? 1 : 0));
        return "? extends java.lang.Object";
      case STRING:
        String stringType = getStringType(schema.getJsonProp(SpecificData.CLASS_PROP));
        return stringType.equals("java.lang.String")
                ? stringType
                : "? extends " + stringType;
      case BYTES:   return "? extends java.nio.ByteBuffer";
      case INT:     return "java.lang.Integer";
      case LONG:    return "java.lang.Long";
      case FLOAT:   return "java.lang.Float";
      case DOUBLE:  return "java.lang.Double";
      case BOOLEAN: return "java.lang.Boolean";
      case NULL:    return "java.lang.Void";
      default: throw new RuntimeException("Unknown type: "+schema);
    }
  }

  public String ifaceRawType(Schema schema) {
    switch (schema.getType()) {
      case RECORD:
      case ENUM:
      case FIXED:
        return mangle((schema.getNamespace() == null) ? "I"+schema.getName() : schema.getNamespace()+".I"+schema.getName());
      case ARRAY:
        return "java.util.List";
      case MAP:
        return "java.util.Map";
      case UNION:
        List<Schema> types = schema.getTypes(); // elide unions with null
        if ((types.size() == 2) && types.contains(NULL_SCHEMA))
          return ifaceRawType(types.get(types.get(0).equals(NULL_SCHEMA) ? 1 : 0));
        return "java.lang.Object";
      case STRING:
        return getStringType(schema.getJsonProp(SpecificData.CLASS_PROP));
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

  public String javaRawType(Schema schema) {
    switch (schema.getType()) {
      case RECORD:
      case ENUM:
      case FIXED:
        return mangle(schema.getFullName());
      case ARRAY:
        return "java.util.List";
      case MAP:
        return "java.util.Map";
      case UNION:
        List<Schema> types = schema.getTypes(); // elide unions with null
        if ((types.size() == 2) && types.contains(NULL_SCHEMA))
          return javaRawType(types.get(types.get(0).equals(NULL_SCHEMA) ? 1 : 0));
        return "java.lang.Object";
      case STRING:
        return getStringType(schema.getJsonProp(SpecificData.CLASS_PROP));
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

  /** Utility for template use.  Returns the unboxed java interface type for a Schema. */
  public String ifaceUnbox(Schema schema) {
    switch (schema.getType()) {
      case INT:     return "int";
      case LONG:    return "long";
      case FLOAT:   return "float";
      case DOUBLE:  return "double";
      case BOOLEAN: return "boolean";
      default:      return ifaceType(schema);
    }
  }

}

