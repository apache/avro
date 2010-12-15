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
package org.apache.avro.mapred;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.File;
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import org.apache.avro.Schema;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;

import org.junit.BeforeClass;
import org.junit.Test;

public class TestSequenceFileReader {
  private static final int COUNT =
    Integer.parseInt(System.getProperty("test.count", "10"));
  private static final File DIR
    = new File(System.getProperty("test.dir", "/tmp"));
  private static final File FILE = new File(DIR, "test.seq");

  private static final Schema SCHEMA
    = Pair.getPairSchema(Schema.create(Schema.Type.LONG),
                         Schema.create(Schema.Type.STRING));

  @BeforeClass
  public static void testWriteSequenceFile() throws IOException {
    FILE.delete();
    Configuration c = new Configuration();
    URI uri = FILE.toURI();
    SequenceFile.Writer writer
      = new SequenceFile.Writer(FileSystem.get(uri, c), c,
                                new Path(uri.toString()),
                                LongWritable.class, Text.class);
    final LongWritable key = new LongWritable();
    final Text val = new Text();
    for (int i = 0; i < COUNT; ++i) {
      key.set(i);
      val.set(Integer.toString(i));
      writer.append(key, val);
    }
    writer.close();
  }

  @Test
  public void testReadSequenceFile() throws Exception {
    checkFile(new SequenceFileReader<Long,CharSequence>(FILE));
  }

  public void checkFile(FileReader<Pair<Long,CharSequence>> reader) throws Exception {
    long i = 0;
    for (Pair<Long,CharSequence> p : reader) {
      assertEquals((Long)i, p.key());
      assertEquals(Long.toString(i), p.value().toString());
      i++;
    }
    assertEquals(COUNT, i);
    reader.close();
  }

  @Test
  public void testSequenceFileInputFormat() throws Exception {
    JobConf job = new JobConf();
    Path output = new Path(System.getProperty("test.dir",".")+"/seq-out");

    output.getFileSystem(job).delete(output);
    
    // configure input for Avro from sequence file
    AvroJob.setInputSequenceFile(job);
    FileInputFormat.setInputPaths(job, FILE.toURI().toString());
    AvroJob.setInputSchema(job, SCHEMA);

    // mapper is default, identity
    // reducer is default, identity

    // configure output for avro
    AvroJob.setOutputSchema(job, SCHEMA);
    FileOutputFormat.setOutputPath(job, output);
    
    JobClient.runJob(job);

    checkFile(new DataFileReader<Pair<Long,CharSequence>>
              (new File(output.toString()+"/part-00000.avro"),
               new SpecificDatumReader<Pair<Long,CharSequence>>()));
  }

  private static class NonAvroMapper
    extends MapReduceBase
    implements Mapper<LongWritable,Text,AvroKey<Long>,AvroValue<Utf8>> {
    
    public void map(LongWritable key, Text value, 
                  OutputCollector<AvroKey<Long>,AvroValue<Utf8>> out, 
                  Reporter reporter) throws IOException {
      out.collect(new AvroKey<Long>(key.get()),
                  new AvroValue<Utf8>(new Utf8(value.toString())));
    }
  }

  @Test
  public void testNonAvroMapper() throws Exception {
    JobConf job = new JobConf();
    Path output = new Path(System.getProperty("test.dir",".")+"/seq-out");

    output.getFileSystem(job).delete(output);
    
    // configure input for non-Avro sequence file
    job.setInputFormat(SequenceFileInputFormat.class);
    FileInputFormat.setInputPaths(job, FILE.toURI().toString());

    // use a hadoop mapper that emits Avro output
    job.setMapperClass(NonAvroMapper.class);

    // reducer is default, identity

    // configure output for avro
    FileOutputFormat.setOutputPath(job, output);
    AvroJob.setOutputSchema(job, SCHEMA);

    JobClient.runJob(job);

    checkFile(new DataFileReader<Pair<Long,CharSequence>>
              (new File(output.toString()+"/part-00000.avro"),
               new SpecificDatumReader<Pair<Long,CharSequence>>()));
  }

  private static class NonAvroReducer
    extends MapReduceBase
    implements Reducer<AvroKey<Long>,AvroValue<Utf8>,LongWritable,Text> {
    
    public void reduce(AvroKey<Long> key, Iterator<AvroValue<Utf8>> values,
                       OutputCollector<LongWritable, Text> out, 
                       Reporter reporter) throws IOException {
      while (values.hasNext()) {
        AvroValue<Utf8> value = values.next();
        out.collect(new LongWritable(key.datum()),
                    new Text(value.datum().toString()));
      }
    }
  }

  @Test
  public void testNonAvroReducer() throws Exception {
    JobConf job = new JobConf();
    Path output = new Path(System.getProperty("test.dir",".")+"/seq-out");

    output.getFileSystem(job).delete(output);
    
    // configure input for Avro from sequence file
    AvroJob.setInputSequenceFile(job);
    AvroJob.setInputSchema(job, SCHEMA);
    FileInputFormat.setInputPaths(job, FILE.toURI().toString());

    // mapper is default, identity

    // use a hadoop reducer that consumes Avro input
    AvroJob.setMapOutputSchema(job, SCHEMA);
    job.setReducerClass(NonAvroReducer.class);

    // configure output for non-Avro SequenceFile
    job.setOutputFormat(SequenceFileOutputFormat.class);
    FileOutputFormat.setOutputPath(job, output);

    // output key/value classes are default, LongWritable/Text

    JobClient.runJob(job);

    checkFile(new SequenceFileReader<Long,CharSequence>
              (new File(output.toString()+"/part-00000")));
  }

}
