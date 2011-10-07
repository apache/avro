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
package org.apache.avro.tool;

import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.tether.TetherJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;

@SuppressWarnings("deprecation")
public class TetherTool implements Tool {
  public TetherJob job;

  @Override
  public String getName() {
    return "tether";
  }

  @Override
  public String getShortDescription() {
    return "Run a tethered mapreduce job.";
  }

  @Override
  public int run(InputStream ins, PrintStream outs, PrintStream err,
      List<String> args) throws Exception {

    OptionParser p = new OptionParser();
    OptionSpec<File> exec = p
        .accepts("program", "executable program, usually in HDFS")
        .withRequiredArg().ofType(File.class);
    OptionSpec<String> in = p.accepts("in", "comma-separated input paths")
        .withRequiredArg().ofType(String.class);
    OptionSpec<Path> out = p.accepts("out", "output directory")
        .withRequiredArg().ofType(Path.class);
    OptionSpec<File> outSchema = p.accepts("outschema", "output schema file")
        .withRequiredArg().ofType(File.class);
    OptionSpec<File> mapOutSchema = p
        .accepts("outschemamap", "map output schema file, if different")
        .withOptionalArg().ofType(File.class);
    OptionSpec<Integer> reduces = p.accepts("reduces", "number of reduces")
        .withOptionalArg().ofType(Integer.class);

    JobConf job = new JobConf();

    try {
      OptionSet opts = p.parse(args.toArray(new String[0]));
      FileInputFormat.addInputPaths(job, in.value(opts));
      FileOutputFormat.setOutputPath(job, out.value(opts));
      TetherJob.setExecutable(job, exec.value(opts));
      job.set(AvroJob.OUTPUT_SCHEMA, Schema.parse(outSchema.value(opts))
          .toString());
      if (opts.hasArgument(mapOutSchema))
        job.set(AvroJob.MAP_OUTPUT_SCHEMA,
            Schema.parse(mapOutSchema.value(opts)).toString());
      if (opts.hasArgument(reduces))
        job.setNumReduceTasks(reduces.value(opts));
    } catch (Exception e) {
      p.printHelpOn(err);
      return -1;
    }

    TetherJob.runJob(job);
    return 0;
  }

}
