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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Mapper;

/** A {@link Mapper} for Avro data.
 *
 * <p>Applications should subclass this class and pass their subclass to {@link
 * org.apache.hadoop.mapred.JobConf#setMapperClass(Class)}.  Subclasses must
 * override {@link #map} and may call {@link #collect} to generate output.
 */
public abstract class AvroMapper<IN,OUT> extends MapReduceBase
  implements Mapper<AvroWrapper<IN>, NullWritable,
                    AvroWrapper<OUT>, NullWritable> {
    
  private OutputCollector<AvroWrapper<OUT>, NullWritable> out;
  private Reporter reporter;

  public void map(AvroWrapper<IN> wrapper, NullWritable value, 
                  OutputCollector<AvroWrapper<OUT>, NullWritable> output, 
                  Reporter reporter) throws IOException {
    if (this.out == null) {
      this.out = output;
      this.reporter = reporter;
    }
    map(wrapper.datum());
  }

  /** Return the {@link Reporter} to permit status updates. */
  public Reporter getReporter() { return reporter; }

  /** Called with each map input datum. */
  public abstract void map(IN datum) throws IOException;

  private final AvroWrapper<OUT> outputWrapper = new AvroWrapper<OUT>(null);

  /** Call with each map output datum. */
  public void collect(OUT datum) throws IOException {
    outputWrapper.datum(datum);
    out.collect(outputWrapper, NullWritable.get());
  }

}
