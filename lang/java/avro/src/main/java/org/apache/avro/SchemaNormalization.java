/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro;

import org.apache.avro.util.internal.JacksonUtils;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeSet;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Collection of static methods for generating the parser canonical form of
 * schemas (see {@link #toParsingForm}), standard canonical form of schemas (see
 * {@link #toCanonicalForm}) with user defined properties and fingerprints of
 * canonical forms ({@link #fingerprint}).
 */
public class SchemaNormalization {

  private SchemaNormalization() {
  }

  /**
   * Returns "Parsing Canonical Form" of a schema as defined by Avro spec.
   */
  public static String toParsingForm(Schema s) {
    return toNormalizedForm(s, true, new LinkedHashSet<>());
  }

  /**
   * Returns "Standard Canonical Form" of a schema as defined by Avro spec.
   */
  public static String toCanonicalForm(Schema s) {
    return toCanonicalForm(s, new LinkedHashSet<>());
  }

  /**
   * Returns "Standard Canonical Form" of a schema as defined by Avro spec with
   * additional user standard properties.
   */
  public static String toCanonicalForm(Schema s, LinkedHashSet<String> properties) {
    LinkedHashSet<String> reservedProperties = new LinkedHashSet<>(Arrays.asList("name", "type", "fields", "symbols",
        "items", "values", "logicalType", "size", "order", "doc", "aliases", "default"));
    properties.removeAll(reservedProperties);
    return toNormalizedForm(s, false, properties);
  }

  private static String toNormalizedForm(Schema s, Boolean ps, LinkedHashSet<String> aps) {
    try {
      Map<String, String> env = new HashMap<>();
      return build(env, s, new StringBuilder(), ps, aps).toString();
    } catch (IOException e) {
      // Shouldn't happen, b/c StringBuilder can't throw IOException
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns a fingerprint of a string of bytes. This string is presumed to
   * contain a canonical form of a schema. The algorithm used to compute the
   * fingerprint is selected by the argument <i>fpName</i>. If <i>fpName</i>
   * equals the string <code>"CRC-64-AVRO"</code>, then the result of
   * {@link #fingerprint64} is returned in little-endian format. Otherwise,
   * <i>fpName</i> is used as an algorithm name for
   * {@link MessageDigest#getInstance(String)}, which will throw
   * <code>NoSuchAlgorithmException</code> if it doesn't recognize the name.
   * <p>
   * Recommended Avro practice dictates that <code>"CRC-64-AVRO"</code> is used
   * for 64-bit fingerprints, <code>"MD5"</code> is used for 128-bit fingerprints,
   * and <code>"SHA-256"</code> is used for 256-bit fingerprints.
   */
  public static byte[] fingerprint(String fpName, byte[] data) throws NoSuchAlgorithmException {
    if (fpName.equals("CRC-64-AVRO")) {
      long fp = fingerprint64(data);
      byte[] result = new byte[8];
      for (int i = 0; i < 8; i++) {
        result[i] = (byte) fp;
        fp >>= 8;
      }
      return result;
    }

    MessageDigest md = MessageDigest.getInstance(fpName);
    return md.digest(data);
  }

  /**
   * Returns the 64-bit Rabin Fingerprint (as recommended in the Avro spec) of a
   * byte string.
   */
  public static long fingerprint64(byte[] data) {
    long result = EMPTY64;
    for (byte b : data)
      result = (result >>> 8) ^ FP64.FP_TABLE[(int) (result ^ b) & 0xff];
    return result;
  }

  /**
   * Returns {@link #fingerprint} applied to the parsing canonical form of the
   * supplied schema.
   */
  public static byte[] parsingFingerprint(String fpName, Schema s) throws NoSuchAlgorithmException {
    return fingerprint(fpName, toParsingForm(s).getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Returns {@link #fingerprint64} applied to the parsing canonical form of the
   * supplied schema.
   */
  public static long parsingFingerprint64(Schema s) {
    return fingerprint64(toParsingForm(s).getBytes(StandardCharsets.UTF_8));
  }

  private static Appendable build(Map<String, String> env, Schema s, Appendable o, Boolean ps,
      LinkedHashSet<String> aps) throws IOException {
    boolean firstTime = true;
    Schema.Type st = s.getType();
    LogicalType lt = null;
    if (!ps)
      lt = s.getLogicalType();
    switch (st) {
    default: // boolean, bytes, double, float, int, long, null, string
      if (!ps && lt != null)
        return writeLogicalType(s, lt, o, aps);
      else
        return o.append('"').append(st.getName()).append('"');

    case UNION:
      o.append('[');
      for (Schema b : s.getTypes()) {
        if (!firstTime)
          o.append(',');
        else
          firstTime = false;
        build(env, b, o, ps, aps);
      }
      return o.append(']');

    case ARRAY:
    case MAP:
      o.append("{\"type\":\"").append(st.getName()).append("\"");
      if (st == Schema.Type.ARRAY)
        build(env, s.getElementType(), o.append(",\"items\":"), ps, aps);
      else
        build(env, s.getValueType(), o.append(",\"values\":"), ps, aps);
      if (!ps)
        setSimpleProps(o, s.getObjectProps(), aps); // adding the reserved property if not parser canonical schema
      return o.append("}");

    case ENUM:
    case FIXED:
    case RECORD:
      String name = s.getFullName();
      if (env.get(name) != null)
        return o.append(env.get(name));
      String qname = "\"" + name + "\"";
      env.put(name, qname);
      o.append("{\"name\":").append(qname);
      o.append(",\"type\":\"").append(st.getName()).append("\"");
      if (st == Schema.Type.ENUM) {
        o.append(",\"symbols\":[");
        for (String enumSymbol : s.getEnumSymbols()) {
          if (!firstTime)
            o.append(',');
          else
            firstTime = false;
          o.append('"').append(enumSymbol).append('"');
        }
        o.append("]");
      } else if (st == Schema.Type.FIXED) {
        o.append(",\"size\":").append(Integer.toString(s.getFixedSize()));
        lt = s.getLogicalType();
        // adding the logical property
        if (!ps && lt != null)
          setLogicalProps(o, lt);
      } else { // st == Schema.Type.RECORD
        o.append(",\"fields\":[");
        for (Schema.Field f : s.getFields()) {
          if (!firstTime)
            o.append(',');
          else
            firstTime = false;
          o.append("{\"name\":\"").append(f.name()).append("\"");
          build(env, f.schema(), o.append(",\"type\":"), ps, aps);
          if (!ps)
            setFieldProps(o, f, aps); // if standard canonical form then add reserved properties
          o.append("}");
        }
        o.append("]");
      }
      if (!ps) {
        setComplexProps(o, s);
        setSimpleProps(o, s.getObjectProps(), aps);
      } // adding the reserved property if not parser canonical schema
      return o.append("}");
    }
  }

  private static Appendable writeLogicalType(Schema s, LogicalType lt, Appendable o, LinkedHashSet<String> aps)
      throws IOException {
    o.append("{\"type\":\"").append(s.getType().getName()).append("\"");
    // adding the logical property
    setLogicalProps(o, lt);
    // adding the reserved property
    setSimpleProps(o, s.getObjectProps(), aps);
    return o.append("}");
  }

  private static void setLogicalProps(Appendable o, LogicalType lt) throws IOException {
    o.append(",\"").append(LogicalType.LOGICAL_TYPE_PROP).append("\":\"").append(lt.getName()).append("\"");
    if (lt.getName().equals("decimal")) {
      LogicalTypes.Decimal dlt = (LogicalTypes.Decimal) lt;
      o.append(",\"precision\":").append(Integer.toString(dlt.getPrecision()));
      if (dlt.getScale() != 0)
        o.append(",\"scale\":").append(Integer.toString(dlt.getScale()));
    }
  }

  private static void setSimpleProps(Appendable o, Map<String, Object> schemaProps, LinkedHashSet<String> aps)
      throws IOException {
    for (String propKey : aps) {
      if (schemaProps.containsKey(propKey)) {
        String propValue = JacksonUtils.toJsonNode(schemaProps.get(propKey)).toString();
        o.append(",\"").append(propKey).append("\":").append(propValue);
      }
    }
  }

  private static void setComplexProps(Appendable o, Schema s) throws IOException {
    if (s.getDoc() != null && !s.getDoc().isEmpty())
      o.append(",\"doc\":\"").append(s.getDoc()).append("\"");
    if (s.getAliases() != null && !s.getAliases().isEmpty())
      o.append(",\"aliases\":").append(JacksonUtils.toJsonNode(new TreeSet<String>(s.getAliases())).toString());
    if (s.getType() == Schema.Type.ENUM && s.getEnumDefault() != null) {
      o.append(",\"default\":").append(JacksonUtils.toJsonNode(s.getEnumDefault()).toString());
    }
  }

  private static void setFieldProps(Appendable o, Schema.Field f, LinkedHashSet<String> aps) throws IOException {
    if (f.order() != null)
      o.append(",\"order\":\"").append(f.order().toString()).append("\"");
    if (f.doc() != null)
      o.append(",\"doc\":\"").append(f.doc()).append("\"");
    if (!f.aliases().isEmpty())
      o.append(",\"aliases\":").append(JacksonUtils.toJsonNode(new TreeSet<String>(f.aliases())).toString());
    if (f.defaultVal() != null)
      o.append(",\"default\":").append(JacksonUtils.toJsonNode(f.defaultVal()).toString());
    setSimpleProps(o, f.getObjectProps(), aps);
  }

  final static long EMPTY64 = 0xc15d213aa4d7a795L;

  /* An inner class ensures that FP_TABLE initialized only when needed. */
  private static class FP64 {
    private static final long[] FP_TABLE = new long[256];
    static {
      for (int i = 0; i < 256; i++) {
        long fp = i;
        for (int j = 0; j < 8; j++) {
          long mask = -(fp & 1L);
          fp = (fp >>> 1) ^ (EMPTY64 & mask);
        }
        FP_TABLE[i] = fp;
      }
    }
  }
}
