package org.apache.avro;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSchemaCommons {
  private static final Logger LOG = LoggerFactory.getLogger(TestSchemaCommons.class);

  @ParameterizedTest
  @MethodSource("sharedFolders")
  void runFolder(final File folder) throws IOException {
    final File schemaSource = new File(folder, "schema.json");
    final File data = new File(folder, "data.avro");

    if (!schemaSource.exists()) {
      LOG.warn("No 'schema.json' file on folder {}", folder.getPath());
      return;
    }
    final Schema schema = new Schema.Parser().parse(schemaSource);
    Assertions.assertNotNull(schema);

    if (!data.exists()) {
      LOG.warn("No 'data.avro' file on folder {}", folder.getPath());
      return;
    }

    // output file
    final String rootTest = Thread.currentThread().getContextClassLoader().getResource(".").getPath();
    final File copyData = new File(rootTest, "copy.avro");

    // Deserialize from disk
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
    try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(data, datumReader);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(schema, copyData);
      GenericRecord record = null;
      int counter = 0;
      while (dataFileReader.hasNext()) {
        record = dataFileReader.next();
        counter++;
        Assertions.assertNotNull(record);
        dataFileWriter.append(record);
      }
      Assertions.assertTrue(counter > 0, "no data in file");
    }
  }

  public static Stream<Arguments> sharedFolders() {
    File root = new File("../../../share/test/data/schemas");
    return Arrays.stream(root.listFiles(File::isDirectory)).map(Arguments::of);
  }

}
