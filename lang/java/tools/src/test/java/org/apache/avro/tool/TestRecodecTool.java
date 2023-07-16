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
package org.apache.avro.tool;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestRecodecTool {
  @TempDir
  public File DIR;

  @Test
  void recodec() throws Exception {
    String metaKey = "myMetaKey";
    String metaValue = "myMetaValue";

    File inputFile = new File(DIR, "input.avro");

    Schema schema = Schema.create(Type.STRING);
    DataFileWriter<String> writer = new DataFileWriter<>(new GenericDatumWriter<String>(schema));
    writer.setMeta(metaKey, metaValue).create(schema, inputFile);
    // We write some garbage which should be quite compressible by deflate,
    // but is complicated enough that deflate-9 will work better than deflate-1.
    // These values were plucked from thin air and worked on the first try, so
    // don't read too much into them.
    for (int i = 0; i < 100000; i++) {
      writer.append("" + i % 100);
    }
    writer.close();

    File defaultOutputFile = new File(DIR, "default-output.avro");
    File nullOutputFile = new File(DIR, "null-output.avro");
    File deflateDefaultOutputFile = new File(DIR, "deflate-default-output.avro");
    File deflate1OutputFile = new File(DIR, "deflate-1-output.avro");
    File deflate9OutputFile = new File(DIR, "deflate-9-output.avro");

    new RecodecTool().run(new FileInputStream(inputFile), new PrintStream(defaultOutputFile), null, new ArrayList<>());
    new RecodecTool().run(new FileInputStream(inputFile), new PrintStream(nullOutputFile), null,
        Collections.singletonList("--codec=null"));
    new RecodecTool().run(new FileInputStream(inputFile), new PrintStream(deflateDefaultOutputFile), null,
        Collections.singletonList("--codec=deflate"));
    new RecodecTool().run(new FileInputStream(inputFile), new PrintStream(deflate1OutputFile), null,
        asList("--codec=deflate", "--level=1"));
    new RecodecTool().run(new FileInputStream(inputFile), new PrintStream(deflate9OutputFile), null,
        asList("--codec=deflate", "--level=9"));

    // We assume that metadata copying is orthogonal to codec selection, and
    // so only test it for a single file.
    try (DataFileReader<Void> reader = new DataFileReader<Void>(defaultOutputFile, new GenericDatumReader<>())) {
      assertEquals(metaValue, reader.getMetaString(metaKey));
    }

    // The "default" codec should be the same as null.
    assertEquals(defaultOutputFile.length(), nullOutputFile.length());

    // All of the deflated files should be smaller than the null file.
    assertLessThan(deflateDefaultOutputFile.length(), nullOutputFile.length());
    assertLessThan(deflate1OutputFile.length(), nullOutputFile.length());
    assertLessThan(deflate9OutputFile.length(), nullOutputFile.length());

    // The "level 9" file should be smaller than the "level 1" file.
    assertLessThan(deflate9OutputFile.length(), deflate1OutputFile.length());
  }

  private static void assertLessThan(long less, long more) {
    if (less >= more) {
      fail("Expected " + less + " to be less than " + more);
    }
  }
}
