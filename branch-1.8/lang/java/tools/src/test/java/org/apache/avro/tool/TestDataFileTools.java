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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.StringBufferInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.AvroTestUtil;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class TestDataFileTools {
  static final int COUNT = 10;
  static File sampleFile;
  static String jsonData;
  static Schema schema;
  static File schemaFile;
  
  private static final String KEY_NEEDING_ESCAPES = "trn\\\r\t\n";
  private static final String ESCAPED_KEY = "trn\\\\\\r\\t\\n";

  @BeforeClass
  public static void writeSampleFile() throws IOException {
    sampleFile = AvroTestUtil.tempFile(TestDataFileTools.class,
      TestDataFileTools.class.getName() + ".avro");
    schema = Schema.create(Type.INT);
    schemaFile = AvroTestUtil.tempFile(TestDataFileTools.class, "schema-temp.schema");
    FileWriter fw = new FileWriter(schemaFile);
    fw.append(schema.toString());
    fw.close();
    
    DataFileWriter<Object> writer
      = new DataFileWriter<Object>(new GenericDatumWriter<Object>(schema))
      .setMeta(KEY_NEEDING_ESCAPES, "")
      .create(schema, sampleFile);
    StringBuilder builder = new StringBuilder();

    for (int i = 0; i < COUNT; ++i) {
      builder.append(Integer.toString(i));
      builder.append("\n");
      writer.append(i);
    }

    writer.flush();
    writer.close();
    
    jsonData = builder.toString();
  }
  
  private String run(Tool tool, String... args) throws Exception {
    return run(tool, null, args);
  }

  private String run(Tool tool, InputStream stdin, String... args) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream p = new PrintStream(baos);
    tool.run(
        stdin,
        p, // stdout
        null, // stderr
        Arrays.asList(args));
    return baos.toString("UTF-8").replace("\r", "");
  }

  @Test
  public void testRead() throws Exception {
    assertEquals(jsonData,
        run(new DataFileReadTool(), sampleFile.getPath()));
  }

  @Test
  public void testReadStdin() throws Exception {
    FileInputStream stdin = new FileInputStream(sampleFile);
    assertEquals(jsonData, run(new DataFileReadTool(), stdin, "-"));
  }
  
  @Test
  public void testReadToJsonPretty() throws Exception {
    assertEquals(jsonData,
        run(new DataFileReadTool(), "--pretty", sampleFile.getPath()));
  }
  
  @Test
  public void testGetMeta() throws Exception {
    String output = run(new DataFileGetMetaTool(), sampleFile.getPath());
    assertTrue(output, output.contains("avro.schema\t"+schema.toString()+"\n"));
    assertTrue(output, output.contains(ESCAPED_KEY+"\t\n"));
  }
  
  @Test
  public void testGetMetaForSingleKey() throws Exception {
    assertEquals(schema.toString() + "\n",
        run(new DataFileGetMetaTool(), sampleFile.getPath(), "--key",
            "avro.schema"));
  }
  
  @Test
  public void testGetSchema() throws Exception {
    assertEquals(schema.toString() + "\n",
        run(new DataFileGetSchemaTool(), sampleFile.getPath()));
  }
  
  @Test
  public void testWriteWithDeflate() throws Exception {
    testWrite("deflate", Arrays.asList("--codec", "deflate"), "deflate");
  }
  
  @Test
  public void testWrite() throws Exception {
    testWrite("plain", Collections.<String>emptyList(), "null");
  }
  
  public void testWrite(String name, List<String> extra, String expectedCodec) 
      throws Exception {
      testWrite(name, extra, expectedCodec, "-schema", schema.toString());
      testWrite(name, extra, expectedCodec, "-schema-file", schemaFile.toString());
  }
  public void testWrite(String name, List<String> extra, String expectedCodec, String... extraArgs) 
  throws Exception {
    File outFile = AvroTestUtil.tempFile(getClass(),
        TestDataFileTools.class + ".testWrite." + name + ".avro");
    FileOutputStream fout = new FileOutputStream(outFile);
    PrintStream out = new PrintStream(fout);
    List<String> args = new ArrayList<String>();
    for (String arg : extraArgs) {
        args.add(arg);
    }
    args.add("-");
    args.addAll(extra);
    new DataFileWriteTool().run(
        new StringBufferInputStream(jsonData),
        new PrintStream(out), // stdout
        null, // stderr
        args);
    out.close();
    fout.close();
    
    // Read it back, and make sure it's valid.
    GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
    DataFileReader<Object> fileReader = new DataFileReader<Object>(outFile,reader);
    int i = 0;
    for (Object datum : fileReader) {
      assertEquals(i, datum);
      i++;
    }
    assertEquals(COUNT, i);
    assertEquals(schema, fileReader.getSchema());
    String codecStr = fileReader.getMetaString("avro.codec");
    if (null == codecStr) {
      codecStr = "null";
    }
    assertEquals(expectedCodec, codecStr);
  }
  
  @Test
  public void testFailureOnWritingPartialJSONValues() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baos);
    try {
      new DataFileWriteTool().run(
          new StringBufferInputStream("{"),
          new PrintStream(out), // stdout
          null, // stderr          
          Arrays.asList("-schema", "{ \"type\":\"record\", \"fields\":" +
                        "[{\"name\":\"foo\", \"type\":\"string\"}], " +
                        "\"name\":\"boring\" }", "-"));
      fail("Expected exception.");
    } catch (IOException expected) {
      // expected
    }
  }
  
  @Test
  public void testWritingZeroJsonValues() throws Exception {
    File outFile = writeToAvroFile("zerojsonvalues",
        schema.toString(),
        "");
    assertEquals(0, countRecords(outFile));
  }
  
  private int countRecords(File outFile) throws IOException {
    GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
    DataFileReader<Object> fileReader = 
      new DataFileReader<Object>(outFile,reader);
    int i = 0;
    for (@SuppressWarnings("unused") Object datum : fileReader) {
      i++;
    }
    return i;
  }

  @Test
  public void testDifferentSeparatorsBetweenJsonRecords() throws Exception {
    File outFile = writeToAvroFile(
        "seperators", 
        "{ \"type\":\"array\", \"items\":\"int\" }", 
        "[]    [] []\n[][3]     ");
    assertEquals(5, countRecords(outFile));
  }
  
  public File writeToAvroFile(String testName, String schema, String json) throws Exception {
    File outFile = AvroTestUtil.tempFile(getClass(),
        TestDataFileTools.class + "." + testName + ".avro");
    FileOutputStream fout = new FileOutputStream(outFile);
    PrintStream out = new PrintStream(fout);
    new DataFileWriteTool().run(
        new StringBufferInputStream(json),
        new PrintStream(out), // stdout
        null, // stderr
        Arrays.asList("-schema", schema, "-"));
    out.close();
    fout.close();
    return outFile;
  }
  
}
