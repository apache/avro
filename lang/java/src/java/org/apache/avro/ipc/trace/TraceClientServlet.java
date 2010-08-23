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
package org.apache.avro.ipc.trace;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.trace.TraceCollection.TraceNodeStats;
import org.apache.avro.specific.SpecificRequestor;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;

public class TraceClientServlet extends HttpServlet {
  public static Template getTemplate(VelocityEngine ve, String path) 
    throws IOException {
    try {
      return ve.getTemplate(path);
    } catch (ResourceNotFoundException e) {
      throw new IOException();
    } catch (ParseErrorException e) {
      throw new IOException();
    } catch (Exception e) {
      throw new IOException();
    }
  }
  
  private VelocityEngine velocityEngine;
  private List<Span> activeSpans;
  private HashMap<Integer, TraceCollection> activeCollections;
  private String lastInput = "";
  
  public TraceClientServlet() {
    this.velocityEngine = new VelocityEngine();
    this.activeCollections = new HashMap<Integer, TraceCollection>();
    this.activeSpans = new ArrayList<Span>();
    
    // These two properties tell Velocity to use its own classpath-based loader
    velocityEngine.addProperty("resource.loader", "class");
    velocityEngine.addProperty("class.resource.loader.class",
        "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
  }
  
  public void doPost(HttpServletRequest request, 
    HttpServletResponse response)
    throws ServletException, IOException {
    response.setContentType("text/html");
    VelocityContext context = new VelocityContext();
    
    PrintWriter out = response.getWriter();
    String servers = request.getParameter("servers");

    if(servers != null){
      String splitToken = System.getProperty("line.separator");
      lastInput = servers;
      if (splitToken == null) {
        splitToken = "\n";
      }
      String[] parts = servers.split(splitToken);
      List<URL> urls = new LinkedList<URL>();
      for (String p : parts) {
        String[] portHost = p.split(":");
        if (portHost.length != 2) { continue; }
        try {
          URL url = new URL("http://" + p);
          urls.add(url);
        } catch (MalformedURLException e) {
          continue;
        }
      }
      List<Span> spans = collectAllSpans(urls);
      List<Span> merged = SpanAggregator.getFullSpans(spans).completeSpans;
      this.activeSpans.addAll(merged);
      List<Trace> traces = SpanAggregator.getTraces(merged).traces;
      List<TraceCollection> collections = 
        SpanAggregator.getTraceCollections(traces);
      for (TraceCollection col: collections) {
        this.activeCollections.put(col.getExecutionPathHash(), col);
      }
      response.sendRedirect("/overview/");
    } else {
       out.println("No text entered.");
    }
  }
  
  protected List<Span> collectAllSpans(List<URL> hosts) {
    List<Span> out = new ArrayList<Span>();
    for (URL url: hosts) {
      HttpTransceiver trans = new HttpTransceiver(url);
      try {
        AvroTrace client = (AvroTrace) 
          SpecificRequestor.getClient(AvroTrace.class, trans);
        for (Span s: client.getAllSpans()) {
          out.add(s);
        }
      }
      catch (IOException e) {
        continue;
      }
    }
    return out;
  }
  
  protected List<Span> collectRangedSpans(List<URL> hosts, long start, long end) {
    List<Span> out = new ArrayList<Span>();
    for (URL url: hosts) {
      HttpTransceiver trans = new HttpTransceiver(url);
      try {
        AvroTrace client = (AvroTrace) 
          SpecificRequestor.getClient(AvroTrace.class, trans);
        for (Span s: client.getSpansInRange(start, end)) {
          out.add(s);
        }
      }
      catch (IOException e) {
        continue;
      }
    }
    return out;
  }
  
  /**
   * We support the following URL patterns
   * 
   *   /overview/                          Show all execution patterns
   *   /collection/[p_id]                  Show details for pattern with [p_id]
   *   /collection/[p_id]/[n_id]/          Show trace node with [n_id] 
   */
  public void doGet(HttpServletRequest request, 
      HttpServletResponse response) throws IOException {
    response.setContentType("text/html");
    PrintWriter out = response.getWriter();
    URL url = new URL(request.getRequestURL().toString());
    String path = url.getPath();
    String[] pathParts = path.split("/");
    
    if (pathParts.length == 0 || pathParts.length == 1) {
      loadSpans(out);
      return;
    }
    
    if (pathParts[1].equals("overview")) {
      overview(out);
      return; 
    }
    
    if (pathParts[1].equals("collection")) {
      if (pathParts.length == 3) {
        int patternID = Integer.parseInt(pathParts[2]);
        collection(out, patternID);
        return;
      } 
      else if (pathParts.length == 4) {
        int patternID = Integer.parseInt(pathParts[2]);
        int nodeID = Integer.parseInt(pathParts[3]);
        collectionNode(out, patternID, nodeID);
        return;
      }
      else {
        response.sendRedirect("/");
        return;
      }
    }
    
    // Default
    response.sendRedirect("/");
    return;
  }
  
  // VIEW FUNCTIONS
  
  /**
   * Display an overview of patterns detected from a group of spans.
   */
  private void overview(PrintWriter out) throws IOException {
    VelocityContext context = new VelocityContext();
    context.put("collections", this.activeCollections);
    context.put("spans", this.activeSpans);
    Template t = getTemplate(velocityEngine, 
        "org/apache/avro/ipc/trace/templates/overview.vm");
    t.merge(context, out);
  }
  
  /**
   * Display summary statistics for a { @link TraceCollection } of individual
   * Traces.
   */
  private void collection(PrintWriter out, int patternID) throws IOException {
    VelocityContext context = new VelocityContext();
    TraceCollection col = this.activeCollections.get(patternID);
    context.put("collection", col);
    Template t = getTemplate(velocityEngine, 
      "org/apache/avro/ipc/trace/templates/collection.vm");
    t.merge(context, out);
  }
  
  /**
   * Display in-depth statistics for an individual node of a 
   * { @link TraceCollection }. 
   */
  private void collectionNode(PrintWriter out, int patternID, int nodeID)
      throws IOException {
    VelocityContext context = new VelocityContext();
    TraceCollection col = this.activeCollections.get(patternID);
    TraceNodeStats node = col.getNodeWithID(nodeID);
    context.put("collection", col);
    context.put("node", node);
    Template t = getTemplate(velocityEngine, 
      "org/apache/avro/ipc/trace/templates/node.vm");
    t.merge(context, out);
  }
  
  /**
   * Display a simple UI for loading span data from remote machines.
   */
  private void loadSpans(PrintWriter out) throws IOException {
    VelocityContext context = new VelocityContext();
    context.put("last_input", this.lastInput);
    context.put("default_port", TracePluginConfiguration.DEFAULT_PORT);
    Template t = getTemplate(velocityEngine, 
        "org/apache/avro/ipc/trace/templates/traceinput.vm");
    t.merge(context, out); 
  }
}
