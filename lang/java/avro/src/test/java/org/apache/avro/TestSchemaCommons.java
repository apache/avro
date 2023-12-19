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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    assertNotNull(schema);

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
        assertNotNull(record);
        dataFileWriter.append(record);
      }
      assertTrue(counter > 0, "no data in file");
    }

    // Cleanup
    assertTrue(copyData.delete());
  }

  public static Stream<Arguments> sharedFolders() {
    File root = new File("target/test-classes/share/test/data/schemas");
    return Arrays.stream(root.listFiles(File::isDirectory)).map(Arguments::of);
  }

}
