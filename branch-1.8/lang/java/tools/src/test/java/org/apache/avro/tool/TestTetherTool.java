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

import static java.util.Arrays.asList;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.FileWriter;


import org.apache.avro.AvroTestUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapred.WordCountUtil;
import org.apache.avro.mapred.tether.TetherJob;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

public class TestTetherTool {

  /**
   * Test that the tether tool works with the mapreduce example
   *
   * TODO: How can we ensure that when we run, the WordCountTether example has
   * been properly compiled?
   */
  @Test
  public void test() throws Exception {

    // Create the schema files.
    Schema outscheme = new Pair<Utf8,Long>(new Utf8(""), 0L).getSchema();

    // we need to write the schemas to a file
    File midscfile = AvroTestUtil.tempFile(getClass(), "midschema.avpr");

    FileWriter hf = null;
    try {
      hf =new FileWriter(midscfile);
      hf.write(outscheme.toString());
    }
    finally {
      if (hf != null) {
        hf.close();
      }
    }

    // Get the classpath to use as an argument.
    String cp = System.getProperty("java.class.path");

    JobConf job = new JobConf();
    String dir = System.getProperty("test.dir", ".") + "/mapred";
    Path outputPath = new Path(dir + "/out");

    outputPath.getFileSystem(job).delete(outputPath);

    // create the input file
    WordCountUtil.writeLinesFile();

    // Executable is java? Argument will be WordCountTask.java - Is the classpath
    // set appropriately automatically?
    java.net.URI exec = new java.net.URI("java");
    //input path
    String in = dir + "/in";

    // create a string of the arguments
    String execargs = "-classpath " + System.getProperty("java.class.path");
    execargs += " org.apache.avro.mapred.tether.WordCountTask";

    // Create a list of the arguments to pass to the tull run method
    java.util.List<String> runargs = new java.util.ArrayList<String> ();


    runargs.addAll(java.util.Arrays.asList("--program", "java"));
    runargs.addAll(asList("--exec_args", '"'+execargs+'"'));
    runargs.addAll(asList("--exec_cached", "false"));
    runargs.addAll(asList("--in", in));
    runargs.addAll(asList("--out", outputPath.toString()));
    runargs.addAll(asList("--outschema", midscfile.toString()));

    TetherTool tool = new TetherTool();

    tool.run(null, null, System.err, runargs);

    // TODO:: We should probably do some validation
    // validate the output
    DatumReader<Pair<Utf8,Long>> reader = new SpecificDatumReader<Pair<Utf8,Long>>();
    InputStream cin = new BufferedInputStream(new FileInputStream(WordCountUtil.COUNTS_FILE));
    DataFileStream<Pair<Utf8,Long>> counts = new DataFileStream<Pair<Utf8,Long>>(cin,reader);
    int numWords = 0;
    for (Pair<Utf8,Long> wc : counts) {
      assertEquals(wc.key().toString(),
      WordCountUtil.COUNTS.get(wc.key().toString()), wc.value());
      numWords++;
    }
    cin.close();
    assertEquals(WordCountUtil.COUNTS.size(), numWords);
  }
}
