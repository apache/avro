package org.apache.avro.reflect;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that demonstrates the issue with anonymous enums with AVRO
 * serialization.
 */
public class TestReflectDatumWithAnonymousEnum {
  private static Pojo pojo;

  @BeforeClass
  public static void init() {
    pojo = new Pojo();
    Person person = new Person();
    person.setAddress("Address");
    pojo.setTestEnum(TestEnum.V);
    pojo.setPerson(person);
  }

  // Test fails with "empty name"
  @Test
  public void avroEnumWithNullTest() throws IOException {
    byte[] output = serialize(pojo);
    Pojo desePojo = deserialize(output);
    // TODO the deserialized pojo's address is set into its "name" field
    assertEquals(desePojo, pojo);
  }

  // Test fails when null value is encountered in Pojo for any field
  @Test
  public void avroEnumWithNotNullTest() throws IOException {
    byte[] output = serializeWithoutNulls(pojo);
    Pojo desePojo = deserialize(output);
  }

  private Pojo deserialize(byte[] input) throws IOException {
    ByteArrayInputStream inputStream = new ByteArrayInputStream(input);
    Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
    ReflectDatumReader<Pojo> reflectDatumReader = new ReflectDatumReader<>(Pojo.class);
    return reflectDatumReader.read(null, decoder);
  }

  private byte[] serializeWithoutNulls(Pojo input) throws IOException {
    // Reflect data that doesn't support nulls
    ReflectData reflectData = ReflectData.get();
    System.out.println("Schema: " + reflectData.getSchema(input.getClass()));
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    ReflectDatumWriter<Pojo> datumWriter = new ReflectDatumWriter<>(Pojo.class, reflectData);
    datumWriter.write(input, encoder);
    encoder.flush();
    return outputStream.toByteArray();
  }

  private byte[] serialize(Pojo input) throws IOException {
    // Reflect data that supports nulls
    ReflectData reflectData = ReflectData.AllowNull.get();
    System.out.println("Schema: " + reflectData.getSchema(input.getClass()));
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    ReflectDatumWriter<Pojo> datumWriter = new ReflectDatumWriter<>(Pojo.class, reflectData);
    datumWriter.write(input, encoder);
    encoder.flush();
    return outputStream.toByteArray();
  }

  public static class Pojo {
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
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Pojo pojo = (Pojo) o;

      if (testEnum != pojo.testEnum) return false;
      return person != null ? person.equals(pojo.person) : pojo.person == null;
    }

    @Override
    public int hashCode() {
      int result = testEnum != null ? testEnum.hashCode() : 0;
      result = 31 * result + (person != null ? person.hashCode() : 0);
      return result;
    }
  }

  public static class Person {
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
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Person person = (Person) o;

      if (name != null ? !name.equals(person.name) : person.name != null) return false;
      return address != null ? address.equals(person.address) : person.address == null;
    }

    @Override
    public int hashCode() {
      int result = name != null ? name.hashCode() : 0;
      result = 31 * result + (address != null ? address.hashCode() : 0);
      return result;
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
