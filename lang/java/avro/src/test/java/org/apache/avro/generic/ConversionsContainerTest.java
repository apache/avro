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

import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.data.TimeConversions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConversionsContainerTest {

  @Test
  void getConversions() {
    ConversionsContainer conversions = new ConversionsContainer();

    // test for empty container.
    Assertions.assertNull(conversions.getConversionByClass(String.class));
    Assertions.assertNull(conversions.forClass(String.class));
    Assertions.assertTrue(conversions.getConversions().isEmpty());
    Assertions.assertNull(conversions.getConversionFor(LogicalTypes.timeMicros()));
    Assertions.assertNull(conversions.getConversionByClass(Long.class, LogicalTypes.timeMicros()));

    // add conversions
    conversions.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
    conversions.addLogicalTypeConversion(new Conversions.UUIDConversion());
    conversions.addLogicalTypeConversion(new MyConversion1());
    conversions.addLogicalTypeConversion(new MyConversion2());

    // test with conversions.
    Assertions.assertEquals(4, conversions.getConversions().size());
    Conversion<Object> conversion1 = conversions.getConversionFor(new LogicalType("MyConversion1"));
    Assertions.assertEquals("MyConversion1", conversion1.getLogicalTypeName());

    ConversionsContainer.ClassConversions classConversions = conversions.forClass(String.class);
    Assertions.assertNotNull(classConversions);
    Assertions.assertTrue(classConversions.containsKey("MyConversion2"));
    Assertions.assertTrue(classConversions.containsKey("MyConversion1"));
    Assertions.assertFalse(classConversions.containsKey("Other"));
    Conversion<?> myConversion1 = classConversions.get("MyConversion1");
    Assertions.assertSame(conversion1, myConversion1);
  }

  static class MyConversion1 extends Conversion<String> {

    @Override
    public Class<String> getConvertedType() {
      return String.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "MyConversion1";
    }
  }

  static class MyConversion2 extends Conversion<String> {

    @Override
    public Class<String> getConvertedType() {
      return String.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "MyConversion2";
    }
  }
}
