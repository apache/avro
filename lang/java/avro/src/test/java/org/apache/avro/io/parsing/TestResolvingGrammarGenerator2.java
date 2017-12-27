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
package org.apache.avro.io.parsing;

import java.util.Arrays;

import org.apache.avro.LogicalTypes;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidatorBuilder;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

/** ResolvingGrammarGenerator tests that are not Parameterized.*/
public class TestResolvingGrammarGenerator2 {
  @Test public void testFixed() throws java.io.IOException {
    new ResolvingGrammarGenerator().generate
      (Schema.createFixed("MyFixed", null, null, 10),
       Schema.create(Schema.Type.BYTES));
    new ResolvingGrammarGenerator().generate
      (Schema.create(Schema.Type.BYTES),
       Schema.createFixed("MyFixed", null, null, 10));
  }

  Schema point2dFullname = SchemaBuilder.record("Point").namespace("written")
      .fields()
      .requiredDouble("x")
      .requiredDouble("y")
      .endRecord();

  Schema point3dNoDefault = SchemaBuilder.record("Point").fields()
      .requiredDouble("x")
      .requiredDouble("y")
      .requiredDouble("z")
      .endRecord();

  Schema point2d = SchemaBuilder.record("Point2D")
      .fields()
      .requiredDouble("x")
      .requiredDouble("y")
      .endRecord();

  Schema point3d = SchemaBuilder.record("Point3D").fields()
      .requiredDouble("x")
      .requiredDouble("y")
      .name("z").type().doubleType().doubleDefault(0.0)
      .endRecord();

  Schema point3dMatchName = SchemaBuilder.record("Point").fields()
      .requiredDouble("x")
      .requiredDouble("y")
      .name("z").type().doubleType().doubleDefault(0.0)
      .endRecord();

  private static final Schema FIXED_DECIMAL_3_3_SCHEMA =
    LogicalTypes.decimal(3, 3).addToSchema(Schema.createFixed("decimal", "", "", 5));
  private static final Schema FIXED_DECIMAL_4_3_SCHEMA =
    LogicalTypes.decimal(4, 3).addToSchema(Schema.createFixed("decimal", "", "", 5));
  private static final Schema FIXED_DECIMAL_3_2_SCHEMA =
    LogicalTypes.decimal(3, 2).addToSchema(Schema.createFixed("decimal", "", "", 5));

  private static final Schema BYTES_DECIMAL_3_3_SCHEMA =
    LogicalTypes.decimal(3, 3).addToSchema(Schema.create(Schema.Type.BYTES));
  private static final Schema BYTES_DECIMAL_4_3_SCHEMA =
    LogicalTypes.decimal(4, 3).addToSchema(Schema.create(Schema.Type.BYTES));
  private static final Schema BYTES_DECIMAL_3_2_SCHEMA =
    LogicalTypes.decimal(3, 2).addToSchema(Schema.create(Schema.Type.BYTES));

  @Test(expected=SchemaValidationException.class)
  public void testUnionResolutionNoStructureMatch() throws Exception {
    // there is a short name match, but the structure does not match
    Schema read = Schema.createUnion(Arrays.asList(
        Schema.create(Schema.Type.NULL),
        point3dNoDefault));

    new SchemaValidatorBuilder().canBeReadStrategy().validateAll()
        .validate(point2dFullname, Arrays.asList(read));
  }

  @Test
  public void testUnionResolutionFirstStructureMatch2d() throws Exception {
    // multiple structure matches with no short or full name matches
    Schema read = Schema.createUnion(Arrays.asList(
        Schema.create(Schema.Type.NULL),
        point3dNoDefault, point2d, point3d));

    Symbol grammar = new ResolvingGrammarGenerator().generate(
        point2dFullname, read);
    Assert.assertTrue(grammar.production[1] instanceof Symbol.UnionAdjustAction);

    Symbol.UnionAdjustAction action = (Symbol.UnionAdjustAction)
        grammar.production[1];
    Assert.assertEquals(2, action.rindex);
  }

  @Test
  public void testUnionResolutionFirstStructureMatch3d() throws Exception {
    // multiple structure matches with no short or full name matches
    Schema read = Schema.createUnion(Arrays.asList(
        Schema.create(Schema.Type.NULL),
        point3dNoDefault, point3d, point2d));

    Symbol grammar = new ResolvingGrammarGenerator().generate(
        point2dFullname, read);
    Assert.assertTrue(grammar.production[1] instanceof Symbol.UnionAdjustAction);

    Symbol.UnionAdjustAction action = (Symbol.UnionAdjustAction)
        grammar.production[1];
    Assert.assertEquals(2, action.rindex);
  }

  @Test
  public void testUnionResolutionNamedStructureMatch() throws Exception {
    // multiple structure matches with a short name match
    Schema read = Schema.createUnion(Arrays.asList(
        Schema.create(Schema.Type.NULL),
        point2d, point3dMatchName, point3d));

    Symbol grammar = new ResolvingGrammarGenerator().generate(
        point2dFullname, read);
    Assert.assertTrue(grammar.production[1] instanceof Symbol.UnionAdjustAction);

    Symbol.UnionAdjustAction action = (Symbol.UnionAdjustAction)
        grammar.production[1];
    Assert.assertEquals(2, action.rindex);
  }

  @Test
  public void testUnionResolutionFullNameMatch() throws Exception {
    // there is a full name match, so it should be chosen
    Schema read = Schema.createUnion(Arrays.asList(
        Schema.create(Schema.Type.NULL),
        point2d, point3dMatchName, point3d, point2dFullname));

    Symbol grammar = new ResolvingGrammarGenerator().generate(
        point2dFullname, read);
    Assert.assertTrue(grammar.production[1] instanceof Symbol.UnionAdjustAction);

    Symbol.UnionAdjustAction action = (Symbol.UnionAdjustAction)
        grammar.production[1];
    Assert.assertEquals(4, action.rindex);
  }

  @Test(expected=SchemaValidationException.class)
  public void testFixedDecimalWithDifferentPrecision() throws Exception {
    new SchemaValidatorBuilder().canBeReadStrategy().validateAll()
      .validate(FIXED_DECIMAL_3_3_SCHEMA, Arrays.asList(FIXED_DECIMAL_4_3_SCHEMA));
  }

  @Test(expected=SchemaValidationException.class)
  public void testFixedDecimalWithDifferentWithDifferentScale() throws Exception {
    new SchemaValidatorBuilder().canBeReadStrategy().validateAll()
      .validate(FIXED_DECIMAL_3_3_SCHEMA, Arrays.asList(FIXED_DECIMAL_3_2_SCHEMA));
  }

  @Test
  public void testFixedDecimalWithSameScaleAndPrecision() throws Exception {
    new SchemaValidatorBuilder().canBeReadStrategy().validateAll()
      .validate(FIXED_DECIMAL_3_3_SCHEMA, Arrays.asList(FIXED_DECIMAL_3_3_SCHEMA));
  }

  @Test(expected=SchemaValidationException.class)
  public void testBytesDecimalWithDifferentPrecision() throws Exception {
    new SchemaValidatorBuilder().canBeReadStrategy().validateAll()
      .validate(BYTES_DECIMAL_3_3_SCHEMA, Arrays.asList(BYTES_DECIMAL_4_3_SCHEMA));
  }

  @Test(expected=SchemaValidationException.class)
  public void testBytesDecimalWithDifferentWithDifferentScale() throws Exception {
    new SchemaValidatorBuilder().canBeReadStrategy().validateAll()
      .validate(BYTES_DECIMAL_3_3_SCHEMA, Arrays.asList(BYTES_DECIMAL_3_2_SCHEMA));
  }

  @Test
  public void testBytesDecimalWithSameScaleAndPrecision() throws Exception {
    new SchemaValidatorBuilder().canBeReadStrategy().validateAll()
      .validate(BYTES_DECIMAL_3_3_SCHEMA, Arrays.asList(BYTES_DECIMAL_3_3_SCHEMA));
  }
}
