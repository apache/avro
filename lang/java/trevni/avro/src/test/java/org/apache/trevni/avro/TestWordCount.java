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

import java.io.IOException;
import java.util.StringTokenizer;
import java.io.File;

import junit.framework.Assert;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapred.Reporter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.AvroCollector;

import org.apache.avro.Schema;

import org.junit.Test;
import static org.junit.Assert.*;

import static org.apache.trevni.avro.WordCountUtil.DIR;

public class TestWordCount {

  public static class MapImpl extends AvroMapper<String, Pair<String, Long> > {
    @Override
      public void map(String text, AvroCollector<Pair<String,Long>> collector,
                      Reporter reporter) throws IOException {
      StringTokenizer tokens = new StringTokenizer(text.toString());
      while (tokens.hasMoreTokens())
        collector.collect(new Pair<String,Long>(tokens.nextToken(),1L));
    }
  }
  
  public static class ReduceImpl
    extends AvroReducer<String, Long, Pair<String, Long> > {
    @Override
    public void reduce(String word, Iterable<Long> counts,
                       AvroCollector<Pair<String,Long>> collector,
                       Reporter reporter) throws IOException {
      long sum = 0;
      for (long count : counts)
        sum += count;
      collector.collect(new Pair<String,Long>(word, sum));
    }
  }    

  @Test public void runTestsInOrder() throws Exception {
    testOutputFormat();
    testInputFormat();
  }

  private static final Schema STRING = Schema.create(Schema.Type.STRING);
  static { GenericData.setStringType(STRING, GenericData.StringType.String); }
  private static final Schema LONG = Schema.create(Schema.Type.LONG);

  public void testOutputFormat() throws Exception {
    JobConf job = new JobConf();
    
    WordCountUtil.writeLinesFile();
    
    AvroJob.setInputSchema(job, STRING);
    AvroJob.setOutputSchema(job, Pair.getPairSchema(STRING,LONG));
    
    AvroJob.setMapperClass(job, MapImpl.class);        
    AvroJob.setCombinerClass(job, ReduceImpl.class);
    AvroJob.setReducerClass(job, ReduceImpl.class);
    
    FileInputFormat.setInputPaths(job, new Path(DIR + "/in"));
    FileOutputFormat.setOutputPath(job, new Path(DIR + "/out"));
    FileOutputFormat.setCompressOutput(job, true);
    
    job.setOutputFormat(AvroTrevniOutputFormat.class);

    JobClient.runJob(job);
    
    WordCountUtil.validateCountsFile();
  }

  private static long total;

  public static class Counter extends AvroMapper<GenericRecord,Void> {
    @Override public void map(GenericRecord r, AvroCollector<Void> collector,
                              Reporter reporter) throws IOException {
      total += (Long)r.get("value");
    }
  }
  
  public void testInputFormat() throws Exception {
    JobConf job = new JobConf();

    Schema subSchema = Schema.parse("{\"type\":\"record\"," +
                                    "\"name\":\"PairValue\","+
                                    "\"fields\": [ " + 
                                    "{\"name\":\"value\", \"type\":\"long\"}" + 
                                    "]}");
    AvroJob.setInputSchema(job, subSchema);
    AvroJob.setMapperClass(job, Counter.class);        
    FileInputFormat.setInputPaths(job, new Path(DIR + "/out/*"));
    job.setInputFormat(AvroTrevniInputFormat.class);

    job.setNumReduceTasks(0);                     // map-only
    job.setOutputFormat(NullOutputFormat.class);  // ignore output

    total = 0;
    JobClient.runJob(job);
    assertEquals(WordCountUtil.TOTAL, total);
  }


}
