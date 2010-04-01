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

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryData;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;

/** The {@link RawComparator} used by jobs configured with {@link AvroJob}. */
public class AvroKeyComparator<T>
  extends Configured implements RawComparator<AvroWrapper<T>> {

  private Schema schema;
  private GenericData model;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      schema = Schema.parse(conf.get(AvroJob.MAP_OUTPUT_SCHEMA,
                                     conf.get(AvroJob.OUTPUT_SCHEMA)));
      String api = getConf().get(AvroJob.MAP_OUTPUT_API,
                                 getConf().get(AvroJob.OUTPUT_API));
      model = AvroJob.API_SPECIFIC.equals(api)
        ? SpecificData.get()
        : GenericData.get();
    }
  }

  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    int diff = BinaryData.compare(b1, s1, b2, s2, schema);
    return diff == 0 ? -1 : diff;
  }

  public int compare(AvroWrapper<T> x, AvroWrapper<T> y) {
    int diff = model.compare(x.datum(), y.datum(), schema);
    return diff == 0 ? -1 : diff;
  }

}
