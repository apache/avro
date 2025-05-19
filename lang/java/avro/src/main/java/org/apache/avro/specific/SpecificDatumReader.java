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
package org.apache.avro.specific;

import org.apache.avro.Conversion;
import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.util.ClassUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashSet;
import java.util.Set;

/**
 * {@link org.apache.avro.io.DatumReader DatumReader} for generated Java
 * classes.
 */
public class SpecificDatumReader<T> extends GenericDatumReader<T> {

  /**
   * @deprecated prefer to use {@link #SERIALIZABLE_CLASSES} instead.
   */
  @Deprecated
  public static final String[] SERIALIZABLE_PACKAGES;

  public static final String[] SERIALIZABLE_CLASSES;

  static {
    // no serializable classes by default
    String serializableClassesProp = System.getProperty("org.apache.avro.SERIALIZABLE_CLASSES");
    SERIALIZABLE_CLASSES = (serializableClassesProp == null) ? new String[0] : serializableClassesProp.split(",");

    // no serializable packages by default
    String serializablePackagesProp = System.getProperty("org.apache.avro.SERIALIZABLE_PACKAGES");
    SERIALIZABLE_PACKAGES = (serializablePackagesProp == null) ? new String[0] : serializablePackagesProp.split(",");
  }

  // The primitive "class names" based on Class.isPrimitive()
  private static final Set<String> PRIMITIVES = new HashSet<>(Arrays.asList(Boolean.TYPE.getName(),
      Character.TYPE.getName(), Byte.TYPE.getName(), Short.TYPE.getName(), Integer.TYPE.getName(), Long.TYPE.getName(),
      Float.TYPE.getName(), Double.TYPE.getName(), Void.TYPE.getName()));

  private final List<String> trustedPackages = new ArrayList<>();
  private final List<String> trustedClasses = new ArrayList<>();

  public SpecificDatumReader() {
    this(null, null, SpecificData.get());
  }

  /** Construct for reading instances of a class. */
  public SpecificDatumReader(Class<T> c) {
    this(SpecificData.getForClass(c));
    setSchema(getSpecificData().getSchema(c));
  }

  /** Construct where the writer's and reader's schemas are the same. */
  public SpecificDatumReader(Schema schema) {
    this(schema, schema, SpecificData.getForSchema(schema));
  }

  /** Construct given writer's and reader's schema. */
  public SpecificDatumReader(Schema writer, Schema reader) {
    this(writer, reader, SpecificData.getForSchema(reader));
  }

  /**
   * Construct given writer's schema, reader's schema, and a {@link SpecificData}.
   */
  public SpecificDatumReader(Schema writer, Schema reader, SpecificData data) {
    super(writer, reader, data);
    trustedPackages.addAll(Arrays.asList(SERIALIZABLE_PACKAGES));
    trustedClasses.addAll(Arrays.asList(SERIALIZABLE_CLASSES));
  }

  /** Construct given a {@link SpecificData}. */
  public SpecificDatumReader(SpecificData data) {
    super(data);
    trustedPackages.addAll(Arrays.asList(SERIALIZABLE_PACKAGES));
    trustedClasses.addAll(Arrays.asList(SERIALIZABLE_CLASSES));
  }

  /** Return the contained {@link SpecificData}. */
  public SpecificData getSpecificData() {
    return (SpecificData) getData();
  }

  @Override
  public void setSchema(Schema actual) {
    // if expected is unset and actual is a specific record,
    // then default expected to schema of currently loaded class
    if (getExpected() == null && actual != null && actual.getType() == Schema.Type.RECORD) {
      SpecificData data = getSpecificData();
      Class c = data.getClass(actual);
      if (c != null && SpecificRecord.class.isAssignableFrom(c))
        setExpected(data.getSchema(c));
    }
    super.setSchema(actual);
  }

  @Override
  protected Class findStringClass(Schema schema) {
    Class stringClass = null;
    switch (schema.getType()) {
    case STRING:
      stringClass = getPropAsClass(schema, SpecificData.CLASS_PROP);
      break;
    case MAP:
      stringClass = getPropAsClass(schema, SpecificData.KEY_CLASS_PROP);
      break;
    }
    if (stringClass != null)
      return stringClass;
    return super.findStringClass(schema);
  }

  private Class getPropAsClass(Schema schema, String prop) {
    String name = schema.getProp(prop);
    if (name == null)
      return null;
    try {
      checkSecurity(name);
      Class clazz = ClassUtils.forName(getData().getClassLoader(), name);
      return clazz;
    } catch (ClassNotFoundException e) {
      throw new AvroRuntimeException(e);
    }
  }

  private boolean trustAllPackages() {
    return (trustedPackages.size() == 1 && "*".equals(trustedPackages.get(0)));
  }

  private void checkSecurity(String className) throws ClassNotFoundException {
    if (trustAllPackages() || PRIMITIVES.contains(className)) {
      return;
    }

    for (String trustedClass : getTrustedClasses()) {
      if (className.equals(trustedClass)) {
        return;
      }
    }

    for (String trustedPackage : getTrustedPackages()) {
      if (className.startsWith(trustedPackage)) {
        return;
      }
    }

    throw new SecurityException("Forbidden " + className
        + "! This class is not trusted to be included in Avro schema using java-class. Please set org.apache.avro.SERIALIZABLE_CLASSES system property with the class you trust or org.apache.avro.SERIALIZABLE_PACKAGES system property with the packages you trust.");
  }

  /**
   * @deprecated Use getTrustedClasses() instead
   */
  @Deprecated
  public final List<String> getTrustedPackages() {
    return trustedPackages;
  }

  public final List<String> getTrustedClasses() {
    return trustedClasses;
  }

  @Override
  protected Object readRecord(Object old, Schema expected, ResolvingDecoder in) throws IOException {
    SpecificData data = getSpecificData();
    if (data.useCustomCoders()) {
      old = data.newRecord(old, expected);
      if (old instanceof SpecificRecordBase) {
        SpecificRecordBase d = (SpecificRecordBase) old;
        if (d.hasCustomCoders()) {
          d.customDecode(in);
          return d;
        }
      }
    }
    return super.readRecord(old, expected, in);
  }

  @Override
  protected void readField(Object record, Schema.Field field, Object oldDatum, ResolvingDecoder in, Object state)
      throws IOException {
    if (record instanceof SpecificRecordBase) {
      Conversion<?> conversion = ((SpecificRecordBase) record).getConversion(field.pos());

      Object datum;
      if (conversion != null) {
        datum = readWithConversion(oldDatum, field.schema(), field.schema().getLogicalType(), conversion, in);
      } else {
        datum = readWithoutConversion(oldDatum, field.schema(), in);
      }

      getData().setField(record, field.name(), field.pos(), datum);

    } else {
      super.readField(record, field, oldDatum, in, state);
    }
  }
}
