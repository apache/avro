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
package org.apache.avro.reflect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * https://issues.apache.org/jira/browse/AVRO-1851
 */
public class TestReflectDatumWithAnonymousInstances {
  private static Pojo pojo;

  @BeforeClass
  public static void init() {
    // 1. Anonymous instance
    pojo = new Pojo() {
      {
        // 2. Anonymous instance
        Person person = new Person() {
          {
            setAddress("Address");
          }
        };
        setPerson(person);
        // 3. Anonymous instance
        setTestEnum(TestEnum.V);
      }
    };
  }

  // Properly serializes and deserializes a POJO with an enum instance
  // (TestEnum#V)
  @Test
  public void handleProperlyEnumInstances() throws IOException {
    byte[] output = serialize(pojo);
    Pojo deserializedPojo = deserialize(output);
    assertEquals(pojo, deserializedPojo);
    assertTrue(deserializedPojo.getTestEnum().is_V());
  }

  private Pojo deserialize(byte[] input) throws IOException {
    ByteArrayInputStream inputStream = new ByteArrayInputStream(input);
    Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
    ReflectData reflectData = ReflectData.AllowNull.get();
    ReflectDatumReader<Pojo> reflectDatumReader = new ReflectDatumReader<>(reflectData);
    Schema schema = reflectData.getSchema(Pojo.class);
    reflectDatumReader.setSchema(schema);
    return reflectDatumReader.read(null, decoder);
  }

  private byte[] serialize(Pojo input) throws IOException {
    // Reflect data that supports nulls
    ReflectData reflectData = ReflectData.AllowNull.get();
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    ReflectDatumWriter<Pojo> datumWriter = new ReflectDatumWriter<>(Pojo.class, reflectData);
    datumWriter.write(input, encoder);
    encoder.flush();
    return outputStream.toByteArray();
  }

  private static class Pojo {
    private TestEnum testEnum;
    private Person person;

    public TestEnum getTestEnum() {
      return testEnum;
    }

    public void setTestEnum(TestEnum testEnum) {
      this.testEnum = testEnum;
    }

    public Person getPerson() {
      return person;
    }

    public void setPerson(Person person) {
      this.person = person;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;

      if (o == null)
        return false;

      Class<?> thisClass = getClass();
      while (thisClass.isAnonymousClass()) {
        thisClass = thisClass.getSuperclass();
      }

      Class<?> oClass = o.getClass();
      while (oClass.isAnonymousClass()) {
        oClass = oClass.getSuperclass();
      }

      if (thisClass != oClass)
        return false;

      Pojo pojo = (Pojo) o;

      if (testEnum != pojo.testEnum)
        return false;
      return person != null ? person.equals(pojo.person) : pojo.person == null;
    }

    @Override
    public int hashCode() {
      int result = testEnum != null ? testEnum.hashCode() : 0;
      result = 31 * result + (person != null ? person.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return "Pojo{" + "testEnum=" + testEnum + ", person=" + person + '}';
    }
  }

  private static class Person {
    private String name;
    private String address;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getAddress() {
      return address;
    }

    public void setAddress(String address) {
      this.address = address;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;

      if (o == null)
        return false;

      Class<?> thisClass = getClass();
      while (thisClass.isAnonymousClass()) {
        thisClass = thisClass.getSuperclass();
      }

      Class<?> oClass = o.getClass();
      while (oClass.isAnonymousClass()) {
        oClass = oClass.getSuperclass();
      }

      if (thisClass != oClass)
        return false;

      Person person = (Person) o;

      if (name != null ? !name.equals(person.name) : person.name != null)
        return false;
      return address != null ? address.equals(person.address) : person.address == null;
    }

    @Override
    public int hashCode() {
      int result = name != null ? name.hashCode() : 0;
      result = 31 * result + (address != null ? address.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return "Person{" + "name='" + name + '\'' + ", address='" + address + '\'' + '}';
    }
  }

  enum TestEnum {
    V {
      @Override
      public boolean is_V() {
        return true;
      }
    };

    public boolean is_V() {
      return false;
    }
  }
}
