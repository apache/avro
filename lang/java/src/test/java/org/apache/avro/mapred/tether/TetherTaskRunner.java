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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.util.Utf8;
import org.apache.avro.ipc.SocketServer;
import org.apache.avro.specific.SpecificResponder;

/** Java implementation of a tether executable.  Useless except for testing,
 * since it's already possible to write Java MapReduce programs without
 * tethering.  Also serves as an example of how a framework may be
 * implemented. */
public class TetherTaskRunner implements InputProtocol {
  static final Logger LOG = LoggerFactory.getLogger(TetherTaskRunner.class);

  private SocketServer inputServer;
  private TetherTask task;

  public TetherTaskRunner(TetherTask task) throws IOException {
    this.task = task;

    // start input server
    this.inputServer = new SocketServer
      (new SpecificResponder(InputProtocol.class, this),
       new InetSocketAddress(0));
    inputServer.start();

    // open output to parent
    task.open(inputServer.getPort());
  }

  @Override public void configure(TaskType taskType,
                                  Utf8 inSchema,
                                  Utf8 outSchema) {
    LOG.info("got configure");
    task.configure(taskType, inSchema, outSchema);
  }

  @Override public synchronized void input(ByteBuffer data, long count) {
    task.input(data, count);
  }

  @Override public void partitions(int partitions) {
    task.partitions(partitions);
  }

  @Override public void abort() {
    LOG.info("got abort");
    close();
  }

  @Override public synchronized void complete() {
    LOG.info("got input complete");
    task.complete();
    close();
  }

  /** Wait for task to complete. */
  public void join() throws InterruptedException {
    inputServer.join();
  }

  private void close() {
    task.close();
    if (inputServer != null)
      inputServer.close();
  }
}
