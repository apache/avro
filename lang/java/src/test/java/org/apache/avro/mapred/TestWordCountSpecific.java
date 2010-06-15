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
import org.junit.Test;

public class TestWordCountSpecific {
  
  private static WordCount newWordCount(String word, int count) {
    WordCount value = new WordCount();
    value.word = new Utf8(word);
    value.count = count;
    return value;
  }

  public static class MapImpl extends AvroMapper<Utf8, WordCount> {
    @Override
    public void map(Utf8 text) throws IOException {
      StringTokenizer tokens = new StringTokenizer(text.toString());
      while (tokens.hasMoreTokens())
        collect(newWordCount(tokens.nextToken(), 1));
    }
  }
  
  public static class ReduceImpl extends AvroReducer<WordCount, WordCount> {

    private WordCount previous;

    @Override
    public void reduce(WordCount current) throws IOException {
      if (current.equals(previous)) {
        previous.count++;
      } else {
        if (previous != null)
          collect(previous);
        previous = newWordCount(current.word.toString(), current.count);
      }
    }
    
    @Override
    public void close() throws IOException {
      if (previous != null)
        collect(previous);
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
    
    AvroJob.setInputSpecific(job, Schema.create(Schema.Type.STRING));
    AvroJob.setOutputSpecific(job, WordCount.SCHEMA$);
    
    job.setMapperClass(MapImpl.class);        
    job.setCombinerClass(ReduceImpl.class);
    job.setReducerClass(ReduceImpl.class);
    
    FileInputFormat.setInputPaths(job, new Path(dir + "/in"));
    FileOutputFormat.setOutputPath(job, outputPath);
    FileOutputFormat.setCompressOutput(job, true);
    
    JobClient.runJob(job);
    
    WordCountUtil.validateCountsFile();
  }

}
