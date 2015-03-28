package org.apache.avro.generic;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestGenericLogicalTypes {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testReadUUID() throws IOException {
    Schema uuidSchema = Schema.create(Schema.Type.STRING);
    LogicalType.uuid().addToSchema(uuidSchema);

    GenericData data = new GenericData();
    data.addLogicalTypeConversion(new Conversion.UUIDConversion());

    UUID u1 = UUID.randomUUID();
    UUID u2 = UUID.randomUUID();
    List<UUID> expected = Arrays.asList(u1, u2);

    File test = write(Schema.create(Schema.Type.STRING),
        u1.toString(), u2.toString());
    Assert.assertEquals("Should convert Strings to UUIDs",
        expected, read(data.createDatumReader(uuidSchema), test));
  }

  @Test
  public void testReadDecimalFixed() throws IOException {
    LogicalType decimal = LogicalType.decimal(9, 2);
    Schema fixedSchema = Schema.createFixed("aFixed", null, null, 4);
    Schema decimalSchema = decimal.addToSchema(
        Schema.createFixed("aFixed", null, null, 4));

    BigDecimal d1 = new BigDecimal("-34.34");
    BigDecimal d2 = new BigDecimal("117230.00");
    List<BigDecimal> expected = Arrays.asList(d1, d2);

    GenericData data = new GenericData();
    Conversion<BigDecimal> conversion = new Conversion.DecimalConversion();
    data.addLogicalTypeConversion(conversion);

    // use the conversion directly instead of relying on the write side
    GenericFixed d1fixed = conversion.toFixed(d1, fixedSchema, decimal);
    GenericFixed d2fixed = conversion.toFixed(d2, fixedSchema, decimal);

    File test = write(fixedSchema, d1fixed, d2fixed);
    Assert.assertEquals("Should convert fixed to BigDecimals",
        expected, read(data.createDatumReader(decimalSchema), test));
  }

  @Test
  public void testReadDecimalBytes() throws IOException {
    LogicalType decimal = LogicalType.decimal(9, 2);
    Schema fixedSchema = Schema.create(Schema.Type.BYTES);
    Schema decimalSchema = decimal.addToSchema(Schema.create(Schema.Type.BYTES));

    BigDecimal d1 = new BigDecimal("-34.34");
    BigDecimal d2 = new BigDecimal("117230.00");
    List<BigDecimal> expected = Arrays.asList(d1, d2);

    GenericData data = new GenericData();
    Conversion<BigDecimal> conversion = new Conversion.DecimalConversion();
    data.addLogicalTypeConversion(conversion);

    // use the conversion directly instead of relying on the write side
    ByteBuffer d1fixed = conversion.toBytes(d1, fixedSchema, decimal);
    ByteBuffer d2fixed = conversion.toBytes(d2, fixedSchema, decimal);

    File test = write(fixedSchema, d1fixed, d2fixed);
    Assert.assertEquals("Should convert bytes to BigDecimals",
        expected, read(data.createDatumReader(decimalSchema), test));
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
    DatumWriter<D> writer = GenericData.get().createDatumWriter(schema);
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
