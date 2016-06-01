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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.avro;

import java.util.ArrayList;

import org.apache.avro.reflect.ReflectData;
import org.junit.Assert;
import org.junit.Test;

public class TestSchemaValidation {

  SchemaValidatorBuilder builder = new SchemaValidatorBuilder();

  Schema rec = SchemaBuilder.record("test.Rec").fields()
      .name("a").type().intType().intDefault(1)
      .name("b").type().longType().noDefault()
      .endRecord();

  Schema rec2 = SchemaBuilder.record("test.Rec").fields()
      .name("a").type().intType().intDefault(1)
      .name("b").type().longType().noDefault()
      .name("c").type().intType().intDefault(0)
      .endRecord();

  Schema rec3 = SchemaBuilder.record("test.Rec").fields()
      .name("b").type().longType().noDefault()
      .name("c").type().intType().intDefault(0)
      .endRecord();

  Schema rec4 = SchemaBuilder.record("test.Rec").fields()
      .name("b").type().longType().noDefault()
      .name("c").type().intType().noDefault()
      .endRecord();

  Schema rec5 = SchemaBuilder.record("test.Rec").fields()
      .name("a").type().stringType().stringDefault("") // different type from original
      .name("b").type().longType().noDefault()
      .name("c").type().intType().intDefault(0)
      .endRecord();

  @Test
  public void testAllTypes() throws SchemaValidationException {
    Schema s = SchemaBuilder.record("r").fields()
        .requiredBoolean("boolF")
        .requiredInt("intF")
        .requiredLong("longF")
        .requiredFloat("floatF")
        .requiredDouble("doubleF")
        .requiredString("stringF")
        .requiredBytes("bytesF")
        .name("fixedF1").type().fixed("F1").size(1).noDefault()
        .name("enumF").type().enumeration("E1").symbols("S").noDefault()
        .name("mapF").type().map().values().stringType().noDefault()
        .name("arrayF").type().array().items().stringType().noDefault()
        .name("recordF").type().record("inner").fields()
        .name("f").type().intType().noDefault()
        .endRecord().noDefault()
        .optionalBoolean("boolO")
        .endRecord();
    testValidatorPasses(builder.mutualReadStrategy().validateLatest(), s, s);
  }

  @Test
  public void testReadOnePrior() throws SchemaValidationException {
    testValidatorPasses(builder.canReadStrategy().validateLatest(), rec3, rec);
    testValidatorPasses(builder.canReadStrategy().validateLatest(), rec5, rec3);
    testValidatorFails(builder.canReadStrategy().validateLatest(), rec4, rec);
  }

  @Test
  public void testReadAllPrior() throws SchemaValidationException {
    testValidatorPasses(builder.canReadStrategy().validateAll(), rec3, rec, rec2);
    testValidatorFails(builder.canReadStrategy().validateAll(), rec4, rec, rec2, rec3);
    testValidatorFails(builder.canReadStrategy().validateAll(), rec5, rec, rec2, rec3);
  }

  @Test
  public void testOnePriorCanRead() throws SchemaValidationException {
    testValidatorPasses(builder.canBeReadStrategy().validateLatest(), rec, rec3);
    testValidatorFails(builder.canBeReadStrategy().validateLatest(), rec, rec4);
  }

  @Test
  public void testAllPriorCanRead() throws SchemaValidationException {
    testValidatorPasses(builder.canBeReadStrategy().validateAll(), rec, rec3, rec2);
    testValidatorFails(builder.canBeReadStrategy().validateAll(), rec, rec4, rec3, rec2);
  }

  @Test
  public void testOnePriorCompatible() throws SchemaValidationException {
    testValidatorPasses(builder.mutualReadStrategy().validateLatest(), rec, rec3);
    testValidatorFails(builder.mutualReadStrategy().validateLatest(), rec, rec4);
  }

  @Test
  public void testAllPriorCompatible() throws SchemaValidationException {
    testValidatorPasses(builder.mutualReadStrategy().validateAll(), rec, rec3, rec2);
    testValidatorFails(builder.mutualReadStrategy().validateAll(), rec, rec4, rec3, rec2);
  }

  @Test(expected=AvroRuntimeException.class)
  public void testInvalidBuild() {
    builder.strategy(null).validateAll();
  }

  public static class Point {
    double x;
    double y;
  }

  public static class Circle {
    Point center;
    double radius;
  }

  public static final Schema circleSchema = SchemaBuilder.record("Circle")
      .fields()
      .name("center").type().record("Point")
          .fields()
          .requiredDouble("x")
          .requiredDouble("y")
          .endRecord().noDefault()
      .requiredDouble("radius")
      .endRecord();

  public static final Schema circleSchemaDifferentNames = SchemaBuilder
      .record("crcl").fields()
      .name("center").type().record("pt")
      .fields()
      .requiredDouble("x")
      .requiredDouble("y")
      .endRecord().noDefault()
      .requiredDouble("radius")
      .endRecord();

  @Test
  public void testReflectMatchStructure() throws SchemaValidationException {
    testValidatorPasses(builder.canBeReadStrategy().validateAll(),
        circleSchemaDifferentNames, ReflectData.get().getSchema(Circle.class));
  }

  @Test
  public void testReflectWithAllowNullMatchStructure() throws SchemaValidationException {
    testValidatorPasses(builder.canBeReadStrategy().validateAll(),
        circleSchemaDifferentNames, ReflectData.AllowNull.get().getSchema(Circle.class));
  }

  private void testValidatorPasses(SchemaValidator validator,
      Schema schema, Schema... prev) throws SchemaValidationException {
    ArrayList<Schema> prior = new ArrayList<Schema>();
    for(int i = prev.length - 1; i >= 0; i--) {
      prior.add(prev[i]);
    }
    validator.validate(schema, prior);
  }

  private void testValidatorFails(SchemaValidator validator,
      Schema schemaFails, Schema... prev) throws SchemaValidationException {
    ArrayList<Schema> prior = new ArrayList<Schema>();
    for(int i = prev.length - 1; i >= 0; i--) {
      prior.add(prev[i]);
    }
    boolean threw = false;
    try {
      // should fail
      validator.validate(schemaFails, prior);
    } catch (SchemaValidationException sve) {
      threw = true;
    }
    Assert.assertTrue(threw);
  }

}
