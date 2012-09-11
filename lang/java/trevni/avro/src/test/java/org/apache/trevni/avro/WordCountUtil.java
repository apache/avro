/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.trevni.avro;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapred.JobConf;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.mapred.Pair;

public class WordCountUtil {

  public static final File DIR = new File("target", "wc");
  public static final File LINES_FILE
    = new File(new File(DIR, "in"), "lines.avro");
  static final File COUNTS_FILE
    = new File(new File(DIR, "out"), "part-00000/part-0.trv");

  public static final String[] LINES = new String[] {
    "the quick brown fox jumps over the lazy dog",
    "the cow jumps over the moon",
    "the rain in spain falls mainly on the plains"
  };

  public static final Map<String,Long> COUNTS = new TreeMap<String,Long>();
  public static final long TOTAL;
  static {
    long total = 0;
    for (String line : LINES) {
      StringTokenizer tokens = new StringTokenizer(line);
      while (tokens.hasMoreTokens()) {
        String word = tokens.nextToken();
        long count = COUNTS.containsKey(word) ? COUNTS.get(word) : 0L;
        count++;
        total++;
        COUNTS.put(word, count);
      }
    }
    TOTAL = total;
  }

  public static void writeLinesFile() throws IOException {
    FileUtil.fullyDelete(DIR);
    DatumWriter<String> writer = new GenericDatumWriter<String>();
    DataFileWriter<String> out = new DataFileWriter<String>(writer);
    LINES_FILE.getParentFile().mkdirs();
    out.create(Schema.create(Schema.Type.STRING), LINES_FILE);
    for (String line : LINES)
      out.append(line);
    out.close();
  }

  public static void validateCountsFile() throws Exception {
    AvroColumnReader<Pair<String,Long>> reader =
      new AvroColumnReader<Pair<String,Long>>
      (new AvroColumnReader.Params(COUNTS_FILE).setModel(SpecificData.get()));
    int numWords = 0;
    for (Pair<String,Long> wc : reader) {
      assertEquals(wc.key(), COUNTS.get(wc.key()), wc.value());
      numWords++;
    }
    reader.close();
    assertEquals(COUNTS.size(), numWords);
  }

}
