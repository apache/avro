package org.apache.avro.reflect;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificData;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestReflectLogicalTypes {
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testReadUUID() throws IOException {
    Schema uuidSchema = SchemaBuilder.record(RecordWithUUID.class.getName())
        .fields().requiredString("uuid").endRecord();
    LogicalType.uuid().addToSchema(uuidSchema.getField("uuid").schema());

    GenericData data = new ReflectData();
    data.addLogicalTypeConversion(new Conversion.UUIDConversion());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    RecordWithStringUUID r1 = new RecordWithStringUUID();
    r1.uuid = u1.toString();
    RecordWithStringUUID r2 = new RecordWithStringUUID();
    r2.uuid = u2.toString();

    List<RecordWithUUID> expected = Arrays.asList(
        new RecordWithUUID(), new RecordWithUUID());
    expected.get(0).uuid = u1;
    expected.get(1).uuid = u2;

    File test = write(
        ReflectData.get().getSchema(RecordWithStringUUID.class), r1, r2);

    Assert.assertEquals("Should convert Strings to UUIDs",
        expected, read(data.createDatumReader(uuidSchema), test));

    // verify that the field's type overrides the logical type
    Schema uuidStringSchema = SchemaBuilder.record(RecordWithStringUUID.class.getName())
        .fields().requiredString("uuid").endRecord();
    LogicalType.uuid().addToSchema(uuidSchema.getField("uuid").schema());

    Assert.assertEquals("Should not convert to UUID if accessor is String",
        Arrays.asList(r1, r2), read(data.createDatumReader(uuidStringSchema), test));
  }

  @Test
  public void testReadUUIDArray() throws IOException {
    Schema uuidArraySchema = SchemaBuilder.record(RecordWithUUIDArray.class.getName())
        .fields()
        .name("uuids").type().array().items().stringType().noDefault()
        .endRecord();
    LogicalType.uuid().addToSchema(
        uuidArraySchema.getField("uuids").schema().getElementType());

    GenericData data = new ReflectData();
    data.addLogicalTypeConversion(new Conversion.UUIDConversion());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    GenericRecord r = new GenericData.Record(uuidArraySchema);
    r.put("uuids", Arrays.asList(u1.toString(), u2.toString()));

    RecordWithUUIDArray expected = new RecordWithUUIDArray();
    expected.uuids = new UUID[] {u1, u2};

    File test = write(uuidArraySchema, r);

    Assert.assertEquals("Should convert Strings to UUIDs",
        expected, read(data.createDatumReader(uuidArraySchema), test).get(0));
  }

  @Test
  public void testReadUUIDList() throws IOException {
    Schema uuidListSchema = SchemaBuilder.record(RecordWithUUIDList.class.getName())
        .fields()
        .name("uuids").type().array().items().stringType().noDefault()
        .endRecord();
    uuidListSchema.getField("uuids").schema().addProp(
        SpecificData.CLASS_PROP, ArrayList.class.getName());
    LogicalType.uuid().addToSchema(
        uuidListSchema.getField("uuids").schema().getElementType());

    GenericData data = new ReflectData();
    data.addLogicalTypeConversion(new Conversion.UUIDConversion());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();

    GenericRecord r = new GenericData.Record(uuidListSchema);
    r.put("uuids", Arrays.asList(u1.toString(), u2.toString()));

    RecordWithUUIDList expected = new RecordWithUUIDList();
    expected.uuids = Arrays.asList(u1, u2);

    File test = write(uuidListSchema, r);

    Assert.assertEquals("Should convert Strings to UUIDs",
        expected, read(data.createDatumReader(uuidListSchema), test).get(0));
  }

  private <D> List<D> read(DatumReader<D> reader, File file) throws IOException {
    List<D> data = new ArrayList<D>();
    FileReader<D> fileReader = null;

    try {
      fileReader = new DataFileReader<D>(file, reader);
      for (D datum : fileReader) {
        data.add(datum);
      }
    } finally {
      if (fileReader != null) {
        fileReader.close();
      }
    }

    return data;
  }

  @SuppressWarnings("unchecked")
  private <D> File write(Schema schema, D... data) throws IOException {
    File file = temp.newFile();
    DatumWriter<D> writer = ReflectData.get().createDatumWriter(schema);
    DataFileWriter<D> fileWriter = new DataFileWriter<D>(writer);

    try {
      fileWriter.create(schema, file);
      for (D datum : data) {
        fileWriter.append(datum);
      }
    } finally {
      fileWriter.close();
    }

    return file;
  }
}

class RecordWithUUID {
  UUID uuid;

  @Override
  public int hashCode() {
    return uuid.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof RecordWithUUID)) {
      return false;
    }
    RecordWithUUID that = (RecordWithUUID) obj;
    return this.uuid.equals(that.uuid);
  }
}

class RecordWithStringUUID {
  String uuid;

  @Override
  public int hashCode() {
    return uuid.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof RecordWithStringUUID)) {
      return false;
    }
    RecordWithStringUUID that = (RecordWithStringUUID) obj;
    return this.uuid.equals(that.uuid);
  }
}

class RecordWithUUIDArray {
  UUID[] uuids;

  @Override
  public int hashCode() {
    return Arrays.hashCode(uuids);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof RecordWithUUIDArray)) {
      return false;
    }
    RecordWithUUIDArray that = (RecordWithUUIDArray) obj;
    return Arrays.equals(this.uuids, that.uuids);
  }
}

class RecordWithUUIDList {
  List<UUID> uuids;

  @Override
  public int hashCode() {
    return uuids.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof RecordWithUUIDList)) {
      return false;
    }
    RecordWithUUIDList that = (RecordWithUUIDList) obj;
    return this.uuids.equals(that.uuids);
  }
}

