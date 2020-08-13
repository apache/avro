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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.junit.Test;
import com.sun.management.UnixOperatingSystemMXBean;

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

}
