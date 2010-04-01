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
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Reducer;

/** A {@link Reducer} for Avro data.
 *
 * <p>Applications should subclass this class and pass their subclass to {@link
 * org.apache.hadoop.mapred.JobConf#setReducerClass(Class)} and perhaps {@link
 * org.apache.hadoop.mapred.JobConf#setCombinerClass(Class)} Subclasses must
 * override {@link #reduce} and may call {@link #collect} to generate output.
 *
 * <p>Note that reducers here are not passed an iterator of all matching
 * values.  Rather, the reducer is called with every value.  If values are to
 * be combined then the reducer must maintain state accordingly.  The final
 * value may be flushed by overriding {@link #close} to call {@link #collect}.
 */
public abstract class AvroReducer<IN,OUT> extends MapReduceBase
  implements Reducer<AvroWrapper<IN>, NullWritable,
                     AvroWrapper<OUT>, NullWritable> {
    
  private OutputCollector<AvroWrapper<OUT>, NullWritable> out;
  private Reporter reporter;

  private final AvroWrapper<OUT> outputWrapper = new AvroWrapper<OUT>(null);

  public void reduce(AvroWrapper<IN> wrapper, Iterator<NullWritable> ignore,
                     OutputCollector<AvroWrapper<OUT>,NullWritable> output, 
                     Reporter reporter) throws IOException {
    if (this.out == null) {
      this.out = output;
      this.reporter = reporter;
    }
    reduce(wrapper.datum());
  }
    
  /** Return the {@link Reporter} to permit status updates. */
  public Reporter getReporter() { return reporter; }

  /** Called with each reduce input datum for this partition, in order. */
  public abstract void reduce(IN datum) throws IOException;

  /** Call with each final output datum. */
  public void collect(OUT datum) throws IOException {
    outputWrapper.datum(datum);
    out.collect(outputWrapper, NullWritable.get());
  }
}
