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

package org.apache.avro.mapred.tether;

import static org.junit.Assert.assertEquals;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

import org.junit.Test;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.WordCountUtil;
import org.apache.avro.mapred.Pair;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.avro.specific.SpecificDatumReader;

public class TestWordCountTether {


    @Test
    @SuppressWarnings("deprecation")
    public void testJob() throws Exception {

    System.out.println(System.getProperty("java.class.path"));
    JobConf job = new JobConf();
    String dir = System.getProperty("test.dir", ".") + "/mapred";
    Path outputPath = new Path(dir + "/out");

    outputPath.getFileSystem(job).delete(outputPath);

    // create the input file
    WordCountUtil.writeLinesFile();

    File exec =
      new File(System.getProperty("java.home")+"/bin/java");

    //input path
    String in=dir+"/in";

    //create a string of the arguments
    List<String> execargs = new ArrayList<String>();
    execargs.add("-classpath");
    execargs.add(System.getProperty("java.class.path"));
    execargs.add("org.apache.avro.mapred.tether.WordCountTask");

    FileInputFormat.addInputPaths(job, in);
    FileOutputFormat.setOutputPath(job, outputPath);
    TetherJob.setExecutable(job, exec, execargs, false);

    Schema outscheme= new Pair<Utf8,Long>(new Utf8(""), 0L).getSchema();
    AvroJob.setInputSchema(job, Schema.create(Schema.Type.STRING));
    job.set(AvroJob.OUTPUT_SCHEMA, outscheme.toString());

    TetherJob.runJob(job);

    // validate the output
    DatumReader<Pair<Utf8,Long>> reader
      = new SpecificDatumReader<Pair<Utf8,Long>>();
    InputStream cin = new BufferedInputStream(new FileInputStream(WordCountUtil.COUNTS_FILE));
    DataFileStream<Pair<Utf8,Long>> counts
      = new DataFileStream<Pair<Utf8,Long>>(cin,reader);
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
