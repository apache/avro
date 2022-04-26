/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.avro.message;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.logging.Logger;

public class TestInteropMessageData {
  private static String inDir = System.getProperty("share.dir", "../../../share") + "/test/data/messageV1";
  private static File SCHEMA_FILE = new File(inDir + "/test_schema.json");
  private static File MESSAGE_FILE = new File(inDir + "/test_message.bin");
  private static final Schema SCHEMA;
  private static final GenericRecordBuilder BUILDER;

  static {
    try {
      SCHEMA = new Schema.Parser().parse(new FileInputStream(SCHEMA_FILE));
      BUILDER = new GenericRecordBuilder(SCHEMA);
    } catch (IOException e) {
      throw new RuntimeException("Interop Message Data Schema not found");
    }
  }

  @Test
  public void sanity_check() throws IOException {
    MessageEncoder<GenericData.Record> encoder = new BinaryMessageEncoder<>(GenericData.get(), SCHEMA);
    ByteBuffer buffer = encoder.encode(
        BUILDER.set("id", 42L).set("name", "Bill").set("tags", Arrays.asList("dog_lover", "cat_hater")).build());
    byte[] fileBuffer = Files.readAllBytes(MESSAGE_FILE.toPath());
    Assert.assertArrayEquals(fileBuffer, buffer.array());
  }
}
