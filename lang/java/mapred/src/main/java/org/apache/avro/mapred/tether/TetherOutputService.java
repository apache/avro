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

import java.nio.ByteBuffer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

class TetherOutputService implements OutputProtocol {
  private Reporter reporter;
  private OutputCollector<TetherData, NullWritable> collector;
  private int inputPort;
  private boolean complete;
  private String error;

  public TetherOutputService(OutputCollector<TetherData,NullWritable> collector,
                             Reporter reporter) {
    this.reporter = reporter;
    this.collector = collector;
  }

  public synchronized void configure(int inputPort) {
    TetherMapRunner.LOG.info("got input port from child");
    this.inputPort = inputPort;
    notify();
  }

  public synchronized int inputPort() throws InterruptedException {
    while (inputPort == 0) {
      TetherMapRunner.LOG.info("waiting for input port from child");
      wait();
    }
    return inputPort;
  }

  public void output(ByteBuffer datum) {
    try {
      collector.collect(new TetherData(datum), NullWritable.get());
    } catch (Throwable e) {
      TetherMapRunner.LOG.warn("Error: "+e, e);
      synchronized (this) {
        error = e.toString();
      }
    }
  }

  public void outputPartitioned(int partition, ByteBuffer datum) {
    TetherPartitioner.setNextPartition(partition);
    output(datum);
  }

  public void status(String message) { reporter.setStatus(message.toString());  }


  public void count(String group, String name, long amount) {
    reporter.getCounter(group.toString(), name.toString()).increment(amount);
  }

  public synchronized void fail(String message) {
    TetherMapRunner.LOG.warn("Failing: "+message);
    error = message.toString();
    notify();
  }

  public synchronized void complete() {
    TetherMapRunner.LOG.info("got task complete");
    complete = true;
    notify();
  }

  public synchronized boolean isFinished() {
    return complete || (error != null);
  }

  public String error() { return error; }

  public synchronized boolean waitForFinish() throws InterruptedException {
    while (!isFinished())
      wait();
    return error != null;
  }

}
