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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import com.sun.management.UnixOperatingSystemMXBean;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.junit.Test;

@SuppressWarnings("restriction")
public class TestDataFileReader {

  @Test
  // regression test for bug AVRO-2286
  public void testForLeakingFileDescriptors() throws IOException {
    Path emptyFile = Files.createTempFile("empty", ".avro");
    Files.deleteIfExists(emptyFile);
    Files.createFile(emptyFile);

    long openFilesBeforeOperation = getNumberOfOpenFileDescriptors();
    try (DataFileReader<Object> reader = new DataFileReader<>(emptyFile.toFile(), new GenericDatumReader<>())) {
      fail("Reading on empty file is supposed to fail.");
    } catch (IOException e) {
      // everything going as supposed to
    }
    Files.delete(emptyFile);

    long openFilesAfterOperation = getNumberOfOpenFileDescriptors();

    // Sometimes we have less open files because of a GC run during the test cycle.
    assertTrue("File descriptor leaked from new DataFileReader() (expected:" + openFilesBeforeOperation + " actual:"
        + openFilesAfterOperation + ")", openFilesBeforeOperation >= openFilesAfterOperation);
  }

  private long getNumberOfOpenFileDescriptors() {
    OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();
    if (osMxBean instanceof UnixOperatingSystemMXBean) {
      return ((UnixOperatingSystemMXBean) osMxBean).getOpenFileDescriptorCount();
    }
    return 0;
  }

  @Test
  // regression test for bug AVRO-2944
  public void testThrottledInputStream() throws IOException {
    // AVRO-2944 describes hanging/failure in reading Avro file with performing
    // magic header check. This happens with throttled input stream,
    // where we read into buffer less bytes than requested.

    Schema legacySchema = new Schema.Parser().setValidate(false).setValidateDefaults(false)
        .parse("{\"type\": \"record\", \"name\": \"TestSchema\", \"fields\": "
            + "[ {\"name\": \"id\", \"type\": [\"long\", \"null\"], \"default\": null}]}");
    File f = Files.createTempFile("testThrottledInputStream", ".avro").toFile();
    try (DataFileWriter<?> w = new DataFileWriter<>(new GenericDatumWriter<>())) {
      w.create(legacySchema, f);
      w.flush();
    }

    // Without checking for magic header, throttled input has no effect
    FileReader r = new DataFileReader(throttledInputStream(f), new GenericDatumReader<>());
    assertEquals("TestSchema", r.getSchema().getName());

    // With checking for magic header, throttled input should pass too.
    FileReader r2 = DataFileReader.openReader(throttledInputStream(f), new GenericDatumReader<>());
    assertEquals("TestSchema", r2.getSchema().getName());
  }

  private SeekableInput throttledInputStream(File f) throws IOException {
    SeekableFileInput input = new SeekableFileInput(f);
    return new SeekableInput() {
      @Override
      public void close() throws IOException {
        input.close();
      }

      @Override
      public void seek(long p) throws IOException {
        input.seek(p);
      }

      @Override
      public long tell() throws IOException {
        return input.tell();
      }

      @Override
      public long length() throws IOException {
        return input.length();
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        if (len == 1) {
          return input.read(b, off, len);
        } else {
          return input.read(b, off, len - 1);
        }
      }
    };
  }

  @Test
  public void testIgnoreSchemaValidationOnRead() throws IOException {
    // This schema has an accent in the name and the default for the field doesn't
    // match the first type in the union. A Java SDK in the past could create a file
    // containing this schema.
    Schema legacySchema = new Schema.Parser().setValidate(false).setValidateDefaults(false)
        .parse("{\"type\": \"record\", \"name\": \"InvalidAccëntWithInvalidNull\", \"fields\": "
            + "[ {\"name\": \"id\", \"type\": [\"long\", \"null\"], \"default\": null}]}");

    // Create a file with the legacy schema.
    File f = Files.createTempFile("testIgnoreSchemaValidationOnRead", ".avro").toFile();
    try (DataFileWriter<?> w = new DataFileWriter<>(new GenericDatumWriter<>())) {
      w.create(legacySchema, f);
      w.flush();
    }

    // This should not throw an exception.
    try (DataFileStream<Void> r = new DataFileStream<>(new FileInputStream(f), new GenericDatumReader<>())) {
      assertEquals(legacySchema, r.getSchema());
    }
  }

}
