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
package org.apache.avro.ipc.stats;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.avro.Protocol.Message;
import org.apache.avro.ipc.RPCContext;
import org.apache.avro.ipc.RPCPlugin;
import org.apache.avro.ipc.stats.Histogram.Segmenter;
import org.apache.avro.ipc.stats.Stopwatch.Ticks;

/**
 * Collects count and latency statistics about RPC calls.  Keeps
 * data for every method.
 *
 * This uses milliseconds as the standard unit of measure
 * throughout the class, stored in floats.
 */
public class StatsPlugin extends RPCPlugin {
  /** Static declaration of histogram buckets. */
  static final Segmenter<String, Float> DEFAULT_SEGMENTER =
    new Histogram.TreeMapSegmenter<Float>(new TreeSet<Float>(Arrays.asList(
            0f,
           25f,
           50f,
           75f,
          100f,
          200f,
          300f,
          500f,
          750f,
         1000f, // 1 second
         2000f,
         5000f,
        10000f,
        60000f, // 1 minute
       600000f)));

  /** Per-method histograms.
   * Must be accessed while holding a lock on methodTimings. */
  Map<Message, FloatHistogram<?>> methodTimings =
    new HashMap<Message, FloatHistogram<?>>();

  /** RPCs in flight. */
  ConcurrentMap<RPCContext, Stopwatch> activeRpcs =
    new ConcurrentHashMap<RPCContext, Stopwatch>();
  private Ticks ticks;

  private Segmenter<?, Float> segmenter;

  /** Construct a plugin with custom Ticks and Segmenter implementations. */
  StatsPlugin(Ticks ticks, Segmenter<?, Float> segmenter) {
    this.segmenter = segmenter;
    this.ticks = ticks;
  }

  /** Construct a plugin with default (system) ticks, and default
   * histogram segmentation. */
  public StatsPlugin() {
    this(Stopwatch.SYSTEM_TICKS, DEFAULT_SEGMENTER);
  }

  @Override
  public void serverReceiveRequest(RPCContext context) {
    Stopwatch t = new Stopwatch(ticks);
    t.start();
    this.activeRpcs.put(context, t);
  }

  @Override
  public void serverSendResponse(RPCContext context) {
    Stopwatch t = this.activeRpcs.remove(context);
    t.stop();
    publish(context, t);
  }

  /** Adds timing to the histograms. */
  private void publish(RPCContext context, Stopwatch t) {
    Message message = context.getMessage();
    if (message == null) throw new IllegalArgumentException();
    synchronized(methodTimings) {
      FloatHistogram<?> h = methodTimings.get(context.getMessage());
      if (h == null) {
        h = createNewHistogram();
        methodTimings.put(context.getMessage(), h);
      }
      h.add(nanosToMillis(t.elapsedNanos()));
    }
  }

  @SuppressWarnings("unchecked")
  private FloatHistogram<?> createNewHistogram() {
    return new FloatHistogram(segmenter);
  }

  /** Converts nanoseconds to milliseconds. */
  static float nanosToMillis(long elapsedNanos) {
    return elapsedNanos / 1000000.0f;
  }
}
