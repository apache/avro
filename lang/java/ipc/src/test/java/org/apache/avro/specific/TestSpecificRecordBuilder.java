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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.apache.avro.ipc.specific.Person;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Unit test for the SpecificRecordBuilder class.
 */
public class TestSpecificRecordBuilder {
  @Test
  public void testSpecificBuilder() {
    // Create a new builder, and leave some fields with default values empty:
    Person.Builder builder = Person.newBuilder().setName("James Gosling").setYearOfBirth(1955).setState("CA");
    Assert.assertTrue(builder.hasName());
    Assert.assertEquals("James Gosling", builder.getName().toString());
    Assert.assertTrue(builder.hasYearOfBirth());
    Assert.assertEquals(new Integer(1955), builder.getYearOfBirth());
    Assert.assertFalse(builder.hasCountry());
    Assert.assertNull(builder.getCountry());
    Assert.assertTrue(builder.hasState());
    Assert.assertEquals("CA", builder.getState().toString());
    Assert.assertFalse(builder.hasFriends());
    Assert.assertNull(builder.getFriends());
    Assert.assertFalse(builder.hasLanguages());
    Assert.assertNull(builder.getLanguages());
    
    Person person = builder.build();
    Assert.assertEquals("James Gosling", person.getName().toString());
    Assert.assertEquals(new Integer(1955), person.getYearOfBirth());
    Assert.assertEquals("US", person.getCountry().toString());  // country should default to "US"
    Assert.assertEquals("CA", person.getState().toString());
    Assert.assertNotNull(person.getFriends());  // friends should default to an empty list
    Assert.assertEquals(0, person.getFriends().size());
    Assert.assertNotNull(person.getLanguages()); // Languages should now be "English" and "Java"
    Assert.assertEquals(2, person.getLanguages().size());
    Assert.assertEquals("English", person.getLanguages().get(0).toString());
    Assert.assertEquals("Java", person.getLanguages().get(1).toString());
    
    // Test copy constructors:
    Assert.assertEquals(builder, Person.newBuilder(builder));
    Assert.assertEquals(person, Person.newBuilder(person).build());
    
    Person.Builder builderCopy = Person.newBuilder(person);
    Assert.assertEquals("James Gosling", builderCopy.getName().toString());
    Assert.assertEquals(new Integer(1955), builderCopy.getYearOfBirth());
    Assert.assertEquals("US", builderCopy.getCountry().toString());  // country should default to "US"
    Assert.assertEquals("CA", builderCopy.getState().toString());
    Assert.assertNotNull(builderCopy.getFriends());  // friends should default to an empty list
    Assert.assertEquals(0, builderCopy.getFriends().size());
    
    // Test clearing fields:
    builderCopy.clearFriends().clearCountry();
    Assert.assertFalse(builderCopy.hasFriends());
    Assert.assertFalse(builderCopy.hasCountry());
    Assert.assertNull(builderCopy.getFriends());
    Assert.assertNull(builderCopy.getCountry());
    Person person2 = builderCopy.build();
    Assert.assertNotNull(person2.getFriends());
    Assert.assertTrue(person2.getFriends().isEmpty());
  }
  
  @Test(expected=org.apache.avro.AvroRuntimeException.class)
  public void attemptToSetNonNullableFieldToNull() {
    Person.newBuilder().setName(null);
  }
  
  @Ignore
  @Test
  public void testBuilderPerformance() {
    int count = 1000000;
    List<Person> friends = new ArrayList<Person>(0);
    List<CharSequence> languages = new ArrayList<CharSequence>(Arrays.asList(new CharSequence[] { "English", "Java" }));
    long startTimeNanos = System.nanoTime();
    for (int ii = 0; ii < count; ii++) {
      Person.newBuilder().setName("James Gosling").setYearOfBirth(1955).setCountry("US").setState("CA").setFriends(friends).
        setLanguages(languages).build();
    }
    long durationNanos = System.nanoTime() - startTimeNanos;
    double durationMillis = durationNanos / 1e6d;
    System.out.println("Built " + count + " records in " + durationMillis + "ms (" + 
        (count / (durationMillis / 1000d)) + " records/sec, " + (durationMillis / count) + 
        "ms/record");
  }
  
  @Ignore
  @Test
  public void testBuilderPerformanceWithDefaultValues() {
    int count = 1000000;
    long startTimeNanos = System.nanoTime();
    for (int ii = 0; ii < count; ii++) {
      Person.newBuilder().setName("James Gosling").setYearOfBirth(1955).setState("CA").build();
    }
    long durationNanos = System.nanoTime() - startTimeNanos;
    double durationMillis = durationNanos / 1e6d;
    System.out.println("Built " + count + " records in " + durationMillis + "ms (" + 
        (count / (durationMillis / 1000d)) + " records/sec, " + (durationMillis / count) + 
        "ms/record");
  }

  @Ignore
  @Test
  @SuppressWarnings("deprecation")
  public void testManualBuildPerformance() {
    int count = 1000000;
    List<Person> friends = new ArrayList<Person>(0);
    List<CharSequence> languages = new ArrayList<CharSequence>(Arrays.asList(new CharSequence[] { "English", "Java" }));
    long startTimeNanos = System.nanoTime();
    for (int ii = 0; ii < count; ii++) {
      Person person = new Person();
      person.name = "James Gosling";
      person.year_of_birth = 1955;
      person.state = "CA";
      person.country = "US";
      person.friends = friends;
      person.languages = languages;
    }
    long durationNanos = System.nanoTime() - startTimeNanos;
    double durationMillis = durationNanos / 1e6d;
    System.out.println("Built " + count + " records in " + durationMillis + "ms (" + 
        (count / (durationMillis / 1000d)) + " records/sec, " + (durationMillis / count) + 
        "ms/record");
  }
}
