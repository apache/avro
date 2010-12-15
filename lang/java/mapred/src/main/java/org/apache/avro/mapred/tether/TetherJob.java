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
import java.io.InputStream;
import java.io.PrintStream;
import java.io.File;
import java.util.List;
import java.util.Collection;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.avro.Schema;
import org.apache.avro.tool.Tool;
import org.apache.avro.mapred.AvroJob;

/** Constructs and submits tether jobs. This may either be used as a
 * commandline-based or API-based method to launch tether jobs. */
public class TetherJob extends Configured implements Tool {

  /** Get the URI of the application's executable. */
  public static URI getExecutable(JobConf job) {
    try {
      return new URI(job.get("avro.tether.executable"));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
  
  /** Set the URI for the application's executable. Normally this in HDFS. */
  public static void setExecutable(JobConf job, URI executable) {
    job.set("avro.tether.executable", executable.toString());
  }

  /** Submit a job to the map/reduce cluster. All of the necessary
   * modifications to the job to run under tether are made to the
   * configuration.
   */
  public static RunningJob runJob(JobConf job) throws IOException {
    setupTetherJob(job);
    return JobClient.runJob(job);
  }

  /** Submit a job to the Map-Reduce framework. */
  public static RunningJob submitJob(JobConf conf) throws IOException {
    setupTetherJob(conf);
    return new JobClient(conf).submitJob(conf);
  }
  
  private static void setupTetherJob(JobConf job) throws IOException {
    job.setMapRunnerClass(TetherMapRunner.class);
    job.setPartitionerClass(TetherPartitioner.class);
    job.setReducerClass(TetherReducer.class);

    job.setInputFormat(TetherInputFormat.class);
    job.setOutputFormat(TetherOutputFormat.class);

    job.setOutputKeyClass(TetherData.class);
    job.setOutputKeyComparatorClass(TetherKeyComparator.class);
    job.setMapOutputValueClass(NullWritable.class);

    // add TetherKeySerialization to io.serializations
    Collection<String> serializations =
      job.getStringCollection("io.serializations");
    if (!serializations.contains(TetherKeySerialization.class.getName())) {
      serializations.add(TetherKeySerialization.class.getName());
      job.setStrings("io.serializations",
                     serializations.toArray(new String[0]));
    }
    
    DistributedCache.addCacheFile(getExecutable(job), job);
  }

  // Tool methods

  @Override
  public String getName() { return "tether"; }

  @Override
  public String getShortDescription() {return "Run a tethered mapreduce job.";}

  @Override
  public int run(InputStream ins, PrintStream outs, PrintStream err,
                 List<String> args) throws Exception {

    OptionParser p = new OptionParser();
    OptionSpec<URI> exec =
      p.accepts("program", "executable program, usually in HDFS")
      .withRequiredArg().ofType(URI.class);
    OptionSpec<String> in = p.accepts("in", "comma-separated input paths")
      .withRequiredArg().ofType(String.class);
    OptionSpec<Path> out = p.accepts("out", "output directory")
      .withRequiredArg().ofType(Path.class);
    OptionSpec<File> outSchema = p.accepts("outschema", "output schema file")
      .withRequiredArg().ofType(File.class);
    OptionSpec<File> mapOutSchema =
      p.accepts("outschemamap", "map output schema file, if different")
      .withOptionalArg().ofType(File.class);
    OptionSpec<Integer> reduces = p.accepts("reduces", "number of reduces")
      .withOptionalArg().ofType(Integer.class);

    JobConf job = new JobConf();
      
    try {
      OptionSet opts = p.parse(args.toArray(new String[0]));
      FileInputFormat.addInputPaths(job, in.value(opts));
      FileOutputFormat.setOutputPath(job, out.value(opts));
      TetherJob.setExecutable(job, exec.value(opts));
      job.set(AvroJob.OUTPUT_SCHEMA,
              Schema.parse(outSchema.value(opts)).toString());
      if (opts.hasArgument(mapOutSchema))
        job.set(AvroJob.MAP_OUTPUT_SCHEMA,
                Schema.parse(mapOutSchema.value(opts)).toString());
      if (opts.hasArgument(reduces))
        job.setNumReduceTasks(reduces.value(opts));
    } catch (Exception e) {
      p.printHelpOn(err);
      return -1;
    }

    runJob(job);
    return 0;
  }

}
