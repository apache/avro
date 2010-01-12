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

import java.io.IOException;
import java.io.Writer;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.avro.Protocol.Message;
import org.apache.avro.ipc.RPCContext;

/**
 * Exposes information provided by a StatsPlugin as
 * a web page.
 *
 * This class follows the same synchronization conventions
 * as StatsPlugin, to avoid requiring StatsPlugin to serve
 * a copy of the data.
 */
public class StatsServlet extends HttpServlet {
  private final StatsPlugin statsPlugin;

  public StatsServlet(StatsPlugin statsPlugin) {
    this.statsPlugin = statsPlugin;
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    resp.setContentType("text/html");
    writeStats(resp.getWriter());
  }

  void writeStats(Writer w) throws IOException {
    w.append("<html><head><title>Avro RPC Stats</title></head>");
    w.append("<body><h1>Avro RPC Stats</h1>");

    w.append("<h2>Active RPCs</h2>");
    w.append("<ol>");
    for (Entry<RPCContext, Stopwatch> rpc : this.statsPlugin.activeRpcs.entrySet()) {
      writeActiveRpc(w, rpc.getKey(), rpc.getValue());
    }
    w.append("</ol>");

    w.append("<h2>Per-method Timing</h2>");
    synchronized(this.statsPlugin.methodTimings) {
      for (Entry<Message, FloatHistogram<?>> e :
        this.statsPlugin.methodTimings.entrySet()) {
        writeMethod(w, e.getKey(), e.getValue());
      }
    }
    w.append("</body></html>");
  }

  private void writeActiveRpc(Writer w, RPCContext rpc, Stopwatch stopwatch) throws IOException {
    w.append("<li>").append(rpc.getMessage().getName()).append(": ");
    w.append(formatMillis(StatsPlugin.nanosToMillis(stopwatch.elapsedNanos())));
    w.append("</li>");
  }

  private void writeMethod(Writer w, Message message, FloatHistogram<?> hist) throws IOException {
    w.append("<h3>").append(message.getName()).append("</h3>");
    w.append("<p>Number of calls: ");
    w.append(Integer.toString(hist.getCount()));
    w.append("</p><p>Average Duration: ");
    w.append(formatMillis(hist.getMean()));
    w.append("</p>");
    w.append("</p><p>Std Dev: ");
    w.append(formatMillis(hist.getUnbiasedStdDev()));
    w.append("</p>");

    w.append("<dl>");

    for (Histogram.Entry<?> e : hist.entries()) {
      w.append("<dt>");
      w.append(e.bucket.toString());
      w.append("</dt>");
      w.append("<dd>").append(Integer.toString(e.count)).append("</dd>");
      w.append("</dt>");
    }
    w.append("</dl>");
  }

  private CharSequence formatMillis(float millis) {
    return String.format("%.0fms", millis);
  }
}
