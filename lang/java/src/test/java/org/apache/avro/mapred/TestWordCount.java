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
import org.apache.hadoop.mapred.Reporter;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.junit.Test;

public class TestWordCount {

  public static class MapImpl extends AvroMapper<Utf8, Pair<Utf8, Long> > {
    @Override
      public void map(Utf8 text, AvroCollector<Pair<Utf8,Long>> collector,
                      Reporter reporter) throws IOException {
      StringTokenizer tokens = new StringTokenizer(text.toString());
      while (tokens.hasMoreTokens())
        collector.collect(new Pair<Utf8,Long>(new Utf8(tokens.nextToken()),1L));
    }
  }
  
  public static class ReduceImpl
    extends AvroReducer<Utf8, Long, Pair<Utf8, Long> > {
    @Override
    public void reduce(Utf8 word, Iterable<Long> counts,
                       AvroCollector<Pair<Utf8,Long>> collector,
                       Reporter reporter) throws IOException {
      long sum = 0;
      for (long count : counts)
        sum += count;
      collector.collect(new Pair<Utf8,Long>(word, sum));
    }
  }    

  @Test
  @SuppressWarnings("deprecation")
  public void testJob() throws Exception {
    JobConf job = new JobConf();
    String dir = System.getProperty("test.dir", ".") + "/mapred";
    Path outputPath = new Path(dir + "/out");
    
    outputPath.getFileSystem(job).delete(outputPath);
    WordCountUtil.writeLinesFile();
    
    job.setJobName("wordcount");
    
    AvroJob.setInputSchema(job, Schema.create(Schema.Type.STRING));
    AvroJob.setOutputSchema(job,
                            new Pair<Utf8,Long>(new Utf8(""), 0L).getSchema());
    
    AvroJob.setMapperClass(job, MapImpl.class);        
    AvroJob.setCombinerClass(job, ReduceImpl.class);
    AvroJob.setReducerClass(job, ReduceImpl.class);
    
    FileInputFormat.setInputPaths(job, new Path(dir + "/in"));
    FileOutputFormat.setOutputPath(job, outputPath);
    FileOutputFormat.setCompressOutput(job, true);
    
    WordCountUtil.setMeta(job);

    JobClient.runJob(job);
    
    WordCountUtil.validateCountsFile();
  }

}
