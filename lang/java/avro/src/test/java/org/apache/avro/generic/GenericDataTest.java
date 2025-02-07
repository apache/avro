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
package org.apache.avro.generic;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class GenericDataTest {

  static Schema createSchema(Schema.Type type) {
    switch (type) {
    case FIXED:
      return Schema.createFixed("foo", null, null, 4);
    case UNION:
      return Schema.createUnion(Schema.create(Schema.Type.FLOAT), Schema.create(Schema.Type.STRING));
    case MAP:
      return Schema.createMap(Schema.create(Schema.Type.FLOAT));
    case ARRAY:
      return Schema.createArray(Schema.create(Schema.Type.STRING));
    case RECORD:
      return Schema.createRecord("record", null, null, false);
    case ENUM:
      return Schema.createEnum("myEnum", null, null, Collections.emptyList());
    default:
      return Schema.create(type);
    }
  }
  static Object sampleValue(Schema schema) {
    if (schema.getLogicalType() != null) {
      return new Object();
    }
    switch (schema.getElementType().getType()) {
    case BOOLEAN:
      return true;
    case INT:
      return Integer.MAX_VALUE;
    case LONG:
      return Long.MAX_VALUE;
    case FLOAT:
      return Float.MAX_VALUE;
    case DOUBLE:
      return Double.MAX_VALUE;
    default:
      return "foo";
    }
  }
  static Schema createArraySchema(Schema.Type type) {
    return Schema.createArray(createSchema(type));
  }
  static Schema createArraySchemaWithLogicalType(Schema.Type type) {
    final LogicalType logicalType = new LogicalType("Mike");
    Schema schema = logicalType.addToSchema(createSchema(type));
    return Schema.createArray(schema);
  }
  public static Stream<Arguments> testNewArrayData() {
    Map<Schema.Type, GenericData.AbstractArray<?>> validMappings = new EnumMap<>(Schema.Type.class);

    for (Schema.Type type : Schema.Type.values()) {
      switch (type) {
        case INT:
          validMappings.put(type, new PrimitivesArrays.IntArray(0, createArraySchema(type)));
          break;
        case LONG:
          validMappings.put(type, new PrimitivesArrays.LongArray(0, createArraySchema(type)));
          break;
        case DOUBLE:
          validMappings.put(type, new PrimitivesArrays.DoubleArray(0, createArraySchema(type)));
          break;
        case FLOAT:
          validMappings.put(type, new PrimitivesArrays.FloatArray(0, createArraySchema(type)));
          break;
      case BOOLEAN:
        validMappings.put(type, new PrimitivesArrays.BooleanArray(0, createArraySchema(type)));
        break;
      default:
        validMappings.put(type, new GenericData.Array<>(0, createArraySchema(type)));
        break;
      }
    }

    List<Arguments> data = new ArrayList<>();

    validMappings.forEach((validKey, optimalValue) -> {
      Class<?> optimalValueType = optimalValue.getClass();
      //cant reuse null, or a string
      data.add(Arguments.of("null input, "+ validKey, createArraySchema(validKey), null, optimalValueType));
      data.add(Arguments.of("String input, "+ validKey, createArraySchema(validKey), "foo", optimalValueType));
      //should reuse arraylist
      data.add(Arguments.of("ArrayList input, "+ validKey, createArraySchema(validKey), new ArrayList<>(), ArrayList.class));
      if (validKey != Schema.Type.UNION) {
        data.add(Arguments.of("null (with logical type) input, " + validKey, createArraySchemaWithLogicalType(validKey), null, GenericData.Array.class));
        data.add(Arguments.of("String (with logical type) input, " + validKey, createArraySchemaWithLogicalType(validKey), "foo", GenericData.Array.class));
        data.add(Arguments.of("ArrayList (with logical type) input, " + validKey, createArraySchema(validKey), new ArrayList<>(), ArrayList.class));
      }

      validMappings.forEach((suppliedValueType, suppliedValue) -> {
        data.add(Arguments.of(suppliedValueType +" input "+ validKey, createArraySchema(validKey), suppliedValue, optimalValueType));
        if (validKey != Schema.Type.UNION)
          data.add(Arguments.of(suppliedValueType +" (with logical type) input "+ validKey, createArraySchemaWithLogicalType(validKey), suppliedValue, GenericData.Array.class));
      });
    });
    return data.stream();
  }

  @ParameterizedTest
  @MethodSource("testNewArrayData")
  void testNewArray(String description, Schema schema, Object initial, Class<? extends Collection<?>> expectedType) {
    GenericData underTest = new GenericData();
    Object result = underTest.newArray(initial, 10, schema);
    //never null
    assertNotNull(result, description);
    //should always be the best fit type, or a generic array
    assertTrue(expectedType.isInstance(result) || result instanceof GenericData.Array, result.getClass() + " when expected generic or "+expectedType.getName()+ " - " +description);

    //must be a collection from the above list
    Collection <Object> resultCollection = (Collection<Object>) result;

    //the result should be empty
    assertEquals(0, resultCollection.size(), "not empty - " + description);

    //is the supplied type matched the return type, then we should not have allocated a new object
    if (initial != null && initial.getClass() == result.getClass() &&
        (!(initial instanceof GenericContainer) || ((GenericContainer) initial).getSchema().getElementType() == schema.getElementType())) {
      //if the result type is the same as the initial type, it should be reused, so we should not have allocated a new object
      assertSame(initial, result, "not reused - " + description);
    }
    //is the supplied type matched the return type, then we should not have allocated a new object
    if (initial == null) {
      //if we did allocate a not object, we should have allocated the optimal type
      assertSame(expectedType, result.getClass(), "not optimal - " + description);
    }
    //check the schema was set correctly
    if (result instanceof GenericContainer) {
      GenericContainer resultArray = (GenericContainer) result;
      assertEquals(schema.getElementType(), resultArray.getSchema().getElementType(), "wrong element type - " + description);
    }

    //for primitive arrays, we should not have a logical type, and the underlying array should be the correct type
    if (result instanceof PrimitivesArrays.PrimitiveArray) {
      assertSame(expectedType, resultCollection.getClass(), "wrong type for primitive - " + description);
      assertNull(schema.getElementType().getLogicalType(), "Primitive array for logical type - " + description);
    }

    final Object sample = sampleValue(schema);
    resultCollection.add(sample);
    assertEquals(1, resultCollection.size(), "not added - " + description);
    assertEquals(sample, resultCollection.iterator().next(), "wrong value - " + description);
    assertEquals(1, resultCollection.size(), "disappeared - " + description);

    Object result2 = underTest.newArray(resultCollection, 10, schema);
    assertSame(result, result2, "not reused - " + description);

    assertEquals(0, resultCollection.size(), "not reset - " + description);
  }

}
