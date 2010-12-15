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
import java.io.File;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;

import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.SocketTransceiver;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.SocketServer;
import org.apache.avro.specific.SpecificRequestor;
import org.apache.avro.specific.SpecificResponder;

class TetheredProcess  {

  private JobConf job;

  TetherOutputService outputService;
  Server outputServer;
  Process subprocess;
  Transceiver clientTransceiver;
  InputProtocol inputClient;

  public TetheredProcess(JobConf job,
                          OutputCollector<TetherData, NullWritable> collector,
                          Reporter reporter) throws Exception {
    try {
      // start server
      this.outputService = new TetherOutputService(collector, reporter);
      this.outputServer = new SocketServer
        (new SpecificResponder(OutputProtocol.class, outputService),
         new InetSocketAddress(0));
      outputServer.start();
      
      // start sub-process, connecting back to server
      this.subprocess = startSubprocess(job);
      
      // open client, connecting to sub-process
      this.clientTransceiver =
        new SocketTransceiver(new InetSocketAddress(outputService.inputPort()));
      this.inputClient =
        SpecificRequestor.getClient(InputProtocol.class, clientTransceiver);


    } catch (Exception t) {
      close();
      throw t;
    }
  }

  public void close() {
    if (clientTransceiver != null)
      try {
        clientTransceiver.close();
      } catch (IOException e) {}                  // ignore
    if (subprocess != null)
      subprocess.destroy();
    if (outputServer != null)
      outputServer.close();
  }

  private Process startSubprocess(JobConf job)
    throws IOException, InterruptedException {
    // get the executable command
    List<String> command = new ArrayList<String>();
    Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
    if (localFiles == null) {                     // until MAPREDUCE-476
      URI[] files = DistributedCache.getCacheFiles(job);
      localFiles = new Path[] { new Path(files[0].toString()) };
    }
    String executable = localFiles[0].toString();
    FileUtil.chmod(executable, "a+x");
    command.add(executable);

    if (System.getProperty("hadoop.log.dir") == null
        && System.getenv("HADOOP_LOG_DIR") != null)
      System.setProperty("hadoop.log.dir", System.getenv("HADOOP_LOG_DIR"));

    // wrap the command in a stdout/stderr capture
    TaskAttemptID taskid = TaskAttemptID.forName(job.get("mapred.task.id"));
    File stdout = TaskLog.getTaskLogFile(taskid, TaskLog.LogName.STDOUT);
    File stderr = TaskLog.getTaskLogFile(taskid, TaskLog.LogName.STDERR);
    long logLength = TaskLog.getTaskLogLength(job);
    command = TaskLog.captureOutAndError(command, stdout, stderr, logLength);
    stdout.getParentFile().mkdirs();
    stderr.getParentFile().mkdirs();

    // add output server's port to env
    Map<String, String> env = new HashMap<String,String>();
    env.put("AVRO_TETHER_OUTPUT_PORT",
            Integer.toString(outputServer.getPort()));

    // start child process
    ProcessBuilder builder = new ProcessBuilder(command);
    builder.environment().putAll(env);
    return builder.start();
  }
  
}
