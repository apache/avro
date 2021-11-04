package org.apache.avro.reflect;

import static org.junit.Assert.assertEquals;

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

  // Properly serializes and deserializes a POJO with an enum instance
  // (TestEnum#V)
  @Test
  public void handleProperlyEnumInstances() throws IOException {
    byte[] output = serialize(pojo);
    Pojo deserializedPojo = deserialize(output);
    assertEquals(pojo, deserializedPojo);
  }

  // The test fails because the Schema doesn't support null value for the Person's
  // name
  @Test(expected = NullPointerException.class)
  public void avroEnumWithNotNullTest() throws IOException {
    byte[] output = serializeWithoutNulls(pojo);
    deserialize(output);
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

  private byte[] serializeWithoutNulls(Pojo input) throws IOException {
    // Reflect data that doesn't support nulls
    ReflectData reflectData = ReflectData.get();
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
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
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
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
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
