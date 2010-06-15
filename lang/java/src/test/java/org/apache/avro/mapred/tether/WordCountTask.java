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

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.avro.util.Utf8;
import org.apache.avro.mapred.WordCount;

/** Example Java tethered mapreduce executable.  Implements map and reduce
 * functions for word count. */
public class WordCountTask extends TetherTask<Utf8,WordCount,WordCount> {
  
  @Override public void map(Utf8 text, Collector<WordCount> collector)
    throws IOException {
    StringTokenizer tokens = new StringTokenizer(text.toString());
    while (tokens.hasMoreTokens()) {
      WordCount wc = new WordCount();
      wc.word = new Utf8(tokens.nextToken());
      wc.count = 1;
      collector.collect(wc);
    }
  }
  
  private int sum;

  @Override public void reduce(WordCount wc, Collector<WordCount> c) {
    sum += wc.count;
  }
    
  @Override public void reduceFlush(WordCount wc, Collector<WordCount> c)
    throws IOException {
    wc.count = sum;
    c.collect(wc);
    sum = 0;
  }

  public static void main(String... args) throws Exception {
    new TetherTaskRunner(new WordCountTask()).join();
  }

}
