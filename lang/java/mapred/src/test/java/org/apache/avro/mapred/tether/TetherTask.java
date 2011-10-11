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
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.Schema;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.SaslSocketTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

/** Base class for Java tether mapreduce programs.  Useless except for testing,
 * since it's already possible to write Java MapReduce programs without
 * tethering.  Also serves as an example of how a framework may be
 * implemented. */
public abstract class TetherTask<IN,MID,OUT> {
  static final Logger LOG = LoggerFactory.getLogger(TetherTask.class);

  private Transceiver clientTransceiver;
  private OutputProtocol outputClient;

  private TaskType taskType;
  private int partitions;

  private DecoderFactory decoderFactory = DecoderFactory.get();
  private BinaryDecoder decoder;

  private SpecificDatumReader<IN> inReader;
  private SpecificDatumReader<MID> midReader;
  private IN inRecord;
  private MID midRecord;
  private MID midRecordSpare;
  private Collector<MID> midCollector;
  private Collector<OUT> outCollector;

  private static class Buffer extends ByteArrayOutputStream {
    public ByteBuffer data() {
      return ByteBuffer.wrap(buf, 0, count);
    }
  }

  /** Collector for map and reduce output values. */
  public class Collector<T> {
    private SpecificDatumWriter<T> writer;
    private Buffer buffer = new Buffer();
    private BinaryEncoder encoder = new EncoderFactory()
        .configureBlockSize(512).binaryEncoder(buffer, null);
    
    private Collector(Schema schema) {
      this.writer = new SpecificDatumWriter<T>(schema);
    }

    /** Collect a map or reduce output value. */
    public void collect(T record) throws IOException {
      buffer.reset();
      writer.write(record, encoder);
      encoder.flush();
      outputClient.output(buffer.data());
    }
    
    /** Collect a pre-partitioned map output value. */
    public void collect(T record, int partition) throws IOException {
      buffer.reset();
      writer.write(record, encoder);
      encoder.flush();
      outputClient.outputPartitioned(partition, buffer.data());
    }
  }

  void open(int inputPort) throws IOException {
    // open output client, connecting to parent
    String clientPortString = System.getenv("AVRO_TETHER_OUTPUT_PORT");
    if (clientPortString == null)
      throw new RuntimeException("AVRO_TETHER_OUTPUT_PORT env var is null");
    int clientPort = Integer.parseInt(clientPortString);
    this.clientTransceiver =
      new SaslSocketTransceiver(new InetSocketAddress(clientPort));
    this.outputClient = SpecificRequestor.getClient(OutputProtocol.class, clientTransceiver);

    // send inputPort to parent
    outputClient.configure(inputPort);
  }

  void configure(TaskType taskType, CharSequence inSchemaText, CharSequence outSchemaText) {
    this.taskType = taskType;
    try {
      Schema inSchema = Schema.parse(inSchemaText.toString());
      Schema outSchema = Schema.parse(outSchemaText.toString());
      switch (taskType) {
      case MAP:
        this.inReader = new SpecificDatumReader<IN>(inSchema);
        this.midCollector = new Collector<MID>(outSchema);
        break;
      case REDUCE:
        this.midReader = new SpecificDatumReader<MID>(inSchema);
        this.outCollector = new Collector<OUT>(outSchema);
        break;
      }
    } catch (Throwable e) {
      fail(e.toString());
    }
  }

  void partitions(int partitions) { this.partitions = partitions; }

  /** Return the number of map output partitions of this job. */
  public int partitions() { return partitions; }

  void input(ByteBuffer data, long count) {
    try {
      decoder = decoderFactory.binaryDecoder(data.array(), decoder);
      for (long i = 0; i < count; i++) {
        switch (taskType) {
        case MAP:
          inRecord = inReader.read(inRecord, decoder);
          map(inRecord, midCollector);
          break;
        case REDUCE:
          MID prev = midRecord;
          midRecord = midReader.read(midRecordSpare, decoder);
          if (prev != null && !midRecord.equals(prev))
            reduceFlush(prev, outCollector);
          reduce(midRecord, outCollector);
          midRecordSpare = prev;
          break;
        }
      }
    } catch (Throwable e) {
      LOG.warn("failing: "+e, e);
      fail(e.toString());
    }
  }

  void complete() {
    if (taskType == TaskType.REDUCE && midRecord != null)
      try {
        reduceFlush(midRecord, outCollector);
      } catch (Throwable e) {
        LOG.warn("failing: "+e, e);
        fail(e.toString());
      }
    outputClient.complete();
  }

  /** Called with input values to generate intermediate values. */
  public abstract void map(IN record, Collector<MID> collector)
    throws IOException;
  /** Called with sorted intermediate values. */
  public abstract void reduce(MID record, Collector<OUT> collector)
    throws IOException;
  /** Called with the last intermediate value in each equivalence run. */
  public abstract void reduceFlush(MID record, Collector<OUT> collector)
    throws IOException;

  /** Call to update task status. */
  public void status(String message) {
    outputClient.status(message);
  }

  /** Call to increment a counter. */
  public void count(String group, String name, long amount) {
    outputClient.count(group, name, amount);
  }

  /** Call to fail the task. */
  public void fail(String message) {
    outputClient.fail(message);
    close();
  }

  void close() {
    if (clientTransceiver != null)
      try {
        clientTransceiver.close();
      } catch (IOException e) {}                  // ignore
  }

}
