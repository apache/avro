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

import java.util.Collection;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.avro.Schema;

/** Setters to configure jobs for Avro data. */
public class AvroJob {
  private AvroJob() {}                            // no public ctor

  static final String API_GENERIC = "generic";
  static final String API_SPECIFIC = "specific";

  static final String INPUT_API = "avro.input.api";
  static final String OUTPUT_API = "avro.output.api";
  static final String MAP_OUTPUT_API = "avro.map.output.api";

  /** The configuration key for a job's input schema. */
  public static final String INPUT_SCHEMA = "avro.input.schema";
  /** The configuration key for a job's intermediate schema. */
  public static final String MAP_OUTPUT_SCHEMA = "avro.map.output.schema";
  /** The configuration key for a job's output schema. */
  public static final String OUTPUT_SCHEMA = "avro.output.schema";

  /** Configure a job's map input to use Avro's generic API. */
  public static void setInputGeneric(JobConf job, Schema s) {
    job.set(INPUT_API, API_GENERIC);
    configureAvroInput(job, s);
  }

  /** Configure a job's map input to use Avro's specific API. */
  public static void setInputSpecific(JobConf job, Schema s) {
    job.set(INPUT_API, API_SPECIFIC);
    configureAvroInput(job, s);
  }

  private static void configureAvroInput(JobConf job, Schema s) {
    job.set(INPUT_SCHEMA, s.toString());
    job.setInputFormat(AvroInputFormat.class);
  }

  /** Configure a job's map output key schema using Avro's generic API. */
  public static void setMapOutputGeneric(JobConf job, Schema s) {
    job.set(MAP_OUTPUT_API, API_GENERIC);
    setMapOutputSchema(job, s);
    configureAvroOutput(job);
  }

  /** Configure a job's map output key schema using Avro's specific API. */
  public static void setMapOutputSpecific(JobConf job, Schema s) {
    job.set(MAP_OUTPUT_API, API_SPECIFIC);
    setMapOutputSchema(job, s);
    configureAvroOutput(job);
  }

  /** Configure a job's output key schema using Avro's generic API. */
  public static void setOutputGeneric(JobConf job, Schema s) {
    job.set(OUTPUT_API, API_GENERIC);
    setOutputSchema(job, s);
    configureAvroOutput(job);
  }

  /** Configure a job's output key schema using Avro's specific API. */
  public static void setOutputSpecific(JobConf job, Schema s) {
    job.set(OUTPUT_API, API_SPECIFIC);
    setOutputSchema(job, s);
    configureAvroOutput(job);
  }

  /** Set a job's map output key schema. */
  public static void setMapOutputSchema(JobConf job, Schema s) {
    job.set(MAP_OUTPUT_SCHEMA, s.toString());
  }

  /** Return a job's map output key schema. */
  public static Schema getMapOutputSchema(Configuration job) {
    return Schema.parse(job.get(AvroJob.MAP_OUTPUT_SCHEMA,
                                job.get(AvroJob.OUTPUT_SCHEMA)));
  }

  /** Set a job's output key schema. */
  public static void setOutputSchema(JobConf job, Schema s) {
    job.set(OUTPUT_SCHEMA, s.toString());
  }

  /** Return a job's output key schema. */
  public static Schema getOutputSchema(Configuration job) {
    return Schema.parse(job.get(AvroJob.OUTPUT_SCHEMA));
  }

  private static void configureAvroOutput(JobConf job) {
    job.setOutputKeyClass(AvroWrapper.class);
    job.setOutputKeyComparatorClass(AvroKeyComparator.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputFormat(AvroOutputFormat.class);

    // add AvroKeySerialization to io.serializations
    Collection<String> serializations =
      job.getStringCollection("io.serializations");
    if (!serializations.contains(AvroKeySerialization.class.getName())) {
      serializations.add(AvroKeySerialization.class.getName());
      job.setStrings("io.serializations",
                     serializations.toArray(new String[0]));
    }
  }

}
