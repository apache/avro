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

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.junit.Test;

public class TestWordCountGeneric {
  
  private static GenericRecord newWordCount(String word, int count) {
    GenericRecord value = new GenericData.Record(WordCount.SCHEMA$);
    value.put("word", new Utf8(word));
    value.put("count", count);
    return value;
  }

  public static class MapImpl extends AvroMapper<Utf8, GenericRecord> {
    public void map(Utf8 text) throws IOException {
      StringTokenizer tokens = new StringTokenizer(text.toString());
      while (tokens.hasMoreTokens())
        collect(newWordCount(tokens.nextToken(), 1));
    }
  }
  
  public static class ReduceImpl
    extends AvroReducer<GenericRecord, GenericRecord> {

    private GenericRecord previous;

    public void reduce(GenericRecord current) throws IOException {
      if (current.equals(previous)) {
        previous.put("count", ((Integer)previous.get("count"))
                     + (Integer)current.get("count"));
      } else {
        if (previous != null)
          collect(previous);
        previous = newWordCount(current.get("word").toString(),
                                (Integer)current.get("count"));
      }
    }
    
    public void close() throws IOException {
      if (previous != null)
        collect(previous);
    }

  }

  @Test
  @SuppressWarnings("deprecation")
  public void testJob() throws Exception {
    String dir = System.getProperty("test.dir", ".") + "/mapred";
    Path outputPath = new Path(dir + "/out");
    JobConf job = new JobConf();
    try {
      WordCountUtil.writeLinesFile();
  
      job.setJobName("wordcount");
   
      AvroJob.setInputGeneric(job, Schema.create(Schema.Type.STRING));
      AvroJob.setOutputGeneric(job, WordCount.SCHEMA$);
  
      job.setMapperClass(MapImpl.class);        
      job.setCombinerClass(ReduceImpl.class);
      job.setReducerClass(ReduceImpl.class);
  
      FileInputFormat.setInputPaths(job, new Path(dir + "/in"));
      FileOutputFormat.setOutputPath(job, outputPath);
      FileOutputFormat.setCompressOutput(job, true);
  
      JobClient.runJob(job);
  
      WordCountUtil.validateCountsFile();
    } finally {
      outputPath.getFileSystem(job).delete(outputPath);
    }
  }
  
  @Test
  @SuppressWarnings("deprecation")
  public void testAvroUtf8InputFormat() throws Exception {
    String dir = System.getProperty("test.dir", ".") + "/mapred";
    Path outputPath = new Path(dir + "/out");
    JobConf job = new JobConf();
    try {
      WordCountUtil.writeLinesTextFile();
  
      job.setJobName("wordcount");
   
      job.setInputFormat(AvroUtf8InputFormat.class);
      AvroJob.setOutputGeneric(job, WordCount.SCHEMA$);
  
      job.setMapperClass(MapImpl.class);        
      job.setCombinerClass(ReduceImpl.class);
      job.setReducerClass(ReduceImpl.class);
  
      FileInputFormat.setInputPaths(job, new Path(dir + "/in"));
      FileOutputFormat.setOutputPath(job, outputPath);
      FileOutputFormat.setCompressOutput(job, true);
  
      JobClient.runJob(job);
  
      WordCountUtil.validateCountsFile();
    } finally {
      outputPath.getFileSystem(job).delete(outputPath);
    }
  }

}
