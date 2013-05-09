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
package org.apache.avro.tool;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.trevni.avro.RandomData;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestCreateRandomFileTool {
  private static final String COUNT = System.getProperty("test.count", "200");
  private static final File DIR
    = new File(System.getProperty("test.dir", "/tmp"));
  private static final File OUT_FILE = new File(DIR, "random.avro");
  private static final File SCHEMA_FILE =
    new File("../../../share/test/schemas/weather.avsc");

  private String run(List<String> args) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream p = new PrintStream(baos);
    new CreateRandomFileTool().run(null, p, null, args);
    return baos.toString("UTF-8").replace("\r", "");
  }
  
  public void check(String... extraArgs) throws Exception {
    ArrayList<String> args = new ArrayList<String>();
    args.addAll(Arrays.asList(new String[] {
        OUT_FILE.toString(),
        "--count", COUNT,
        "--schema-file", SCHEMA_FILE.toString()
        }));
    args.addAll(Arrays.asList(extraArgs));
    run(args);

    DataFileReader<Object> reader =
      new DataFileReader(OUT_FILE, new GenericDatumReader<Object>());
    
    Iterator<Object> found = reader.iterator();
    for (Object expected :
           new RandomData(Schema.parse(SCHEMA_FILE), Integer.parseInt(COUNT)))
      assertEquals(expected, found.next());

    reader.close();
  }

  @Test
  public void testSimple() throws Exception {
    check();
  }

  @Test
  public void testCodec() throws Exception {
    check("--codec", "snappy");
  }

}
