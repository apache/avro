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

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Foo;
import org.apache.avro.Interop;
import org.apache.avro.Kind;
import org.apache.avro.MD5;
import org.apache.avro.Node;
import org.apache.avro.ipc.specific.PageView;
import org.apache.avro.ipc.specific.Person;
import org.apache.avro.ipc.specific.ProductPage;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Unit test for the SpecificRecordBuilder class.
 */
public class TestSpecificRecordBuilder {
  @Test
  void specificBuilder() {
    // Create a new builder, and leave some fields with default values empty:
    Person.Builder builder = Person.newBuilder().setName("James Gosling").setYearOfBirth(1955).setState("CA");
    assertTrue(builder.hasName());
    assertEquals("James Gosling", builder.getName());
    assertTrue(builder.hasYearOfBirth());
    assertEquals(1955, builder.getYearOfBirth());
    assertFalse(builder.hasCountry());
    assertNull(builder.getCountry());
    assertTrue(builder.hasState());
    assertEquals("CA", builder.getState());
    assertFalse(builder.hasFriends());
    assertNull(builder.getFriends());
    assertFalse(builder.hasLanguages());
    assertNull(builder.getLanguages());

    Person person = builder.build();
    assertEquals("James Gosling", person.getName());
    assertEquals(1955, person.getYearOfBirth());
    assertEquals("US", person.getCountry()); // country should default to "US"
    assertEquals("CA", person.getState());
    assertNotNull(person.getFriends()); // friends should default to an empty list
    assertEquals(0, person.getFriends().size());
    assertNotNull(person.getLanguages()); // Languages should now be "English" and "Java"
    assertEquals(2, person.getLanguages().size());
    assertEquals("English", person.getLanguages().get(0));
    assertEquals("Java", person.getLanguages().get(1));

    // Test copy constructors:
    assertEquals(builder, Person.newBuilder(builder));
    assertEquals(person, Person.newBuilder(person).build());

    Person.Builder builderCopy = Person.newBuilder(person);
    assertEquals("James Gosling", builderCopy.getName());
    assertEquals(1955, builderCopy.getYearOfBirth());
    assertEquals("US", builderCopy.getCountry()); // country should default to "US"
    assertEquals("CA", builderCopy.getState());
    assertNotNull(builderCopy.getFriends()); // friends should default to an empty list
    assertEquals(0, builderCopy.getFriends().size());

    // Test clearing fields:
    builderCopy.clearFriends().clearCountry();
    assertFalse(builderCopy.hasFriends());
    assertFalse(builderCopy.hasCountry());
    assertNull(builderCopy.getFriends());
    assertNull(builderCopy.getCountry());
    Person person2 = builderCopy.build();
    assertNotNull(person2.getFriends());
    assertTrue(person2.getFriends().isEmpty());
  }

  @Test
  void unions() {
    long datetime = 1234L;
    String product = "widget";
    PageView p = PageView.newBuilder().setDatetime(1234L)
        .setPageContext(ProductPage.newBuilder().setProduct(product).build()).build();
    assertEquals(datetime, p.getDatetime());
    assertEquals(ProductPage.class, p.getPageContext().getClass());
    assertEquals(product, ((ProductPage) p.getPageContext()).getProduct());

    PageView p2 = PageView.newBuilder(p).build();

    assertEquals(datetime, p2.getDatetime());
    assertEquals(ProductPage.class, p2.getPageContext().getClass());
    assertEquals(product, ((ProductPage) p2.getPageContext()).getProduct());

    assertEquals(p, p2);

  }

  @Test
  void interop() {
    Interop interop = Interop.newBuilder().setNullField(null).setArrayField(Arrays.asList(3.14159265, 6.022))
        .setBoolField(true).setBytesField(ByteBuffer.allocate(4).put(new byte[] { 3, 2, 1, 0 })).setDoubleField(1.41421)
        .setEnumField(Kind.C).setFixedField(new MD5(new byte[] { 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3 }))
        .setFloatField(1.61803f).setIntField(64).setLongField(1024)
        .setMapField(Collections.singletonMap("Foo1", new Foo())).setRecordField(new Node()).setStringField("MyInterop")
        .setUnionField(2.71828).build();

    Interop copy = Interop.newBuilder(interop).build();
    assertEquals(interop.getArrayField().size(), copy.getArrayField().size());
    assertEquals(interop.getArrayField(), copy.getArrayField());
    assertEquals(interop.getBoolField(), copy.getBoolField());
    assertEquals(interop.getBytesField(), copy.getBytesField());
    assertEquals(interop.getDoubleField(), copy.getDoubleField(), 0.001);
    assertEquals(interop.getEnumField(), copy.getEnumField());
    assertEquals(interop.getFixedField(), copy.getFixedField());
    assertEquals(interop.getFloatField(), copy.getFloatField(), 0.001);
    assertEquals(interop.getIntField(), copy.getIntField());
    assertEquals(interop.getLongField(), copy.getLongField());
    assertEquals(interop.getMapField(), copy.getMapField());
    assertEquals(interop.getRecordField(), copy.getRecordField());
    assertEquals(interop.getStringField(), copy.getStringField());
    assertEquals(interop.getUnionField(), copy.getUnionField());
    assertEquals(interop, copy);
  }

  @Test
  void attemptToSetNonNullableFieldToNull() {
    assertThrows(org.apache.avro.AvroRuntimeException.class, () -> {
      Person.newBuilder().setName(null);
    });
  }

  @Test
  void buildWithoutSettingRequiredFields1() {
    assertThrows(org.apache.avro.AvroRuntimeException.class, () -> {
      Person.newBuilder().build();
    });
  }

  @Test
  void buildWithoutSettingRequiredFields2() {
    // Omit required non-primitive field
    try {
      Person.newBuilder().setYearOfBirth(1900).setState("MA").build();
      fail("Should have thrown " + AvroRuntimeException.class.getCanonicalName());
    } catch (AvroRuntimeException e) {
      // Exception should mention that the 'name' field has not been set
      assertTrue(e.getMessage().contains("name"));
    }
  }

  @Test
  void buildWithoutSettingRequiredFields3() {
    // Omit required primitive field
    try {
      Person.newBuilder().setName("Anon").setState("CA").build();
      fail("Should have thrown " + AvroRuntimeException.class.getCanonicalName());
    } catch (AvroRuntimeException e) {
      // Exception should mention that the 'year_of_birth' field has not been set
      assertTrue(e.getMessage().contains("year_of_birth"));
    }
  }

  @Disabled
  @Test
  void builderPerformance() {
    int count = 1000000;
    List<Person> friends = new ArrayList<>(0);
    List<String> languages = new ArrayList<>(Arrays.asList("English", "Java"));
    long startTimeNanos = System.nanoTime();
    for (int ii = 0; ii < count; ii++) {
      Person.newBuilder().setName("James Gosling").setYearOfBirth(1955).setCountry("US").setState("CA")
          .setFriends(friends).setLanguages(languages).build();
    }
    long durationNanos = System.nanoTime() - startTimeNanos;
    double durationMillis = durationNanos / 1e6d;
    System.out.println("Built " + count + " records in " + durationMillis + "ms (" + (count / (durationMillis / 1000d))
        + " records/sec, " + (durationMillis / count) + "ms/record");
  }

  @Disabled
  @Test
  void builderPerformanceWithDefaultValues() {
    int count = 1000000;
    long startTimeNanos = System.nanoTime();
    for (int ii = 0; ii < count; ii++) {
      Person.newBuilder().setName("James Gosling").setYearOfBirth(1955).setState("CA").build();
    }
    long durationNanos = System.nanoTime() - startTimeNanos;
    double durationMillis = durationNanos / 1e6d;
    System.out.println("Built " + count + " records in " + durationMillis + "ms (" + (count / (durationMillis / 1000d))
        + " records/sec, " + (durationMillis / count) + "ms/record");
  }

  @Disabled
  @Test
  @SuppressWarnings("deprecation")
  void manualBuildPerformance() {
    int count = 1000000;
    List<Person> friends = new ArrayList<>(0);
    List<String> languages = new ArrayList<>(Arrays.asList("English", "Java"));
    long startTimeNanos = System.nanoTime();
    for (int ii = 0; ii < count; ii++) {
      Person person = new Person();
      person.setName("James Gosling");
      person.setYearOfBirth(1955);
      person.setState("CA");
      person.setCountry("US");
      person.setFriends(friends);
      person.setLanguages(languages);
    }
    long durationNanos = System.nanoTime() - startTimeNanos;
    double durationMillis = durationNanos / 1e6d;
    System.out.println("Built " + count + " records in " + durationMillis + "ms (" + (count / (durationMillis / 1000d))
        + " records/sec, " + (durationMillis / count) + "ms/record");
  }
}
