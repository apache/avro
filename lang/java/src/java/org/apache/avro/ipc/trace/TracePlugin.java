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
import java.net.BindException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.RPCContext;
import org.apache.avro.ipc.RPCPlugin;
import org.apache.avro.specific.SpecificResponder;
import org.apache.avro.util.Utf8;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A tracing plugin for Avro.
 * 
 * This plugin traces RPC call timing and follows nested trees of RPC
 * calls. To use, instantiate and add to an existing Requestor or
 * Responder. If you have a Responder that itself acts as an RPC client (i.e.
 * it contains a Requestor) be sure to pass the same instance of the 
 * plugin to both components so that propagation occurs correctly.
 * 
 * Currently, propagation only works if each requester is performing
 * serial RPC calls. That is, we cannot support the case in which the
 * Requestor's request does not occur in the same thread as the Responder's
 * response. 
 */
public class TracePlugin extends RPCPlugin {
  final private static Random RANDOM = new Random();
  private static final Logger LOG = LoggerFactory.getLogger(TracePlugin.class);
  public static enum StorageType { MEMORY, DISK };
  
  /*
   * This plugin uses three key/value meta-data pairs. The value type for all
   * of these pairs is fixed(8) and they are assumed to be encoded as 8-byte
   * ID's. The presence of a TRACE_ID_KEY and a SPAN_ID_KEY in a message
   * signals that tracing is in progress. The optional PARENT_SPAN_ID_KEY
   * signals that this message has a parent node in the RPC call tree. 
   */
  private static final Utf8 TRACE_ID_KEY = new Utf8("traceID");
  private static final Utf8 SPAN_ID_KEY = new Utf8("spanID");
  private static final Utf8 PARENT_SPAN_ID_KEY = new Utf8("parentSpanID");
  

  class TraceResponder implements AvroTrace {
    private SpanStorage spanStorage;
    
    public TraceResponder(SpanStorage spanStorage) {
      this.spanStorage = spanStorage;
    }

    @Override
    public GenericArray<Span> getAllSpans() throws AvroRemoteException {
      List<Span> spans = this.spanStorage.getAllSpans();
      GenericData.Array<Span> out;
      out = new GenericData.Array<Span>(spans.size(), 
        Schema.createArray(Span.SCHEMA$));
      for (Span s: spans) {
        out.add(s);
      }
      return out;
    }

    @Override
    public GenericArray<Span> getSpansInRange(long start, long end)
        throws AvroRemoteException {
      List<Span> spans = this.spanStorage.getSpansInRange(start, end);
      GenericData.Array<Span> out;
      out = new GenericData.Array<Span>(spans.size(), 
        Schema.createArray(Span.SCHEMA$));
      for (Span s: spans) {
        out.add(s);
      }
      return out;
    }
  }
  
  private double traceProb; // Probability of starting tracing
  private int port;         // Port to serve tracing data
  private int clientPort;   // Port to expose client HTTP interface
  private StorageType storageType;  // How to store spans
  private long maxSpans; // Max number of spans to store
  private boolean enabled; // Whether to participate in tracing

  private ThreadLocal<Span> currentSpan; // span in which we are server
  private ThreadLocal<Span> childSpan;   // span in which we are client
  
  // Storage and serving of spans
  protected SpanStorage storage;
  protected HttpServer httpServer;
  protected SpecificResponder responder;
  
  // Client interface
  protected Server clientFacingServer;
  
  public TracePlugin(TracePluginConfiguration conf) throws IOException {
    traceProb = conf.traceProb;
    port = conf.port;
    clientPort = conf.clientPort;
    storageType = conf.storageType;
    maxSpans = conf.maxSpans;
    enabled = conf.enabled;
    
    // check bounds
    if (!(traceProb >= 0.0 && traceProb <= 1.0)) { traceProb = 0.0; }
    if (!(port > 0 && port < 65535)) { port = 51001; }
    if (!(clientPort > 0 && clientPort < 65535)) { clientPort = 51200; }
    if (maxSpans < 0) { maxSpans = 5000; }
    
    currentSpan = new ThreadLocal<Span>(){
      @Override protected Span initialValue(){
          return null;
      }
    };
    
    childSpan = new ThreadLocal<Span>(){
      @Override protected Span initialValue(){
          return null;
      }
    };

    if (storageType == StorageType.MEMORY) {
      this.storage = new InMemorySpanStorage();
    }
    else if (storageType == StorageType.DISK) {
      this.storage = new FileSpanStorage(false, conf);
    }
    else { // default
      this.storage = new InMemorySpanStorage();
    }
    
    this.storage.setMaxSpans(maxSpans);
    
    // Start serving span data
    responder = new SpecificResponder(
        AvroTrace.PROTOCOL, new TraceResponder(this.storage)); 
    
    boolean bound = false;
    
    while (!bound) {
      // rather than die if port is taken, try to fail over to another port.
      try {
        httpServer = new HttpServer(responder, this.port);
        bound = true;
      } catch (AvroRuntimeException e) {
        if (e.getCause() instanceof BindException) {
          LOG.error("Failed to bind to port: " + this.port);
          this.port = this.port +1;
        } else {
          throw e;
        }
      }
    }
    
    // Start client-facing servlet
    initializeClientServer();
  }
  
  @Override
  public void clientStartConnect(RPCContext context) {
    // There are two cases in which we will need to seed a trace
    // (1) If we probabilistically decide to seed a new trace
    // (2) If we are part of an existing trace
    
    if ((this.currentSpan.get() == null) && 
        (RANDOM.nextFloat() < this.traceProb) && enabled) {
      // Start new trace
      Span span = Util.createEventlessSpan(null, null, null);
      this.childSpan.set(span);
    }
    
    if ((this.currentSpan.get() != null) && enabled) {
      Span currSpan = this.currentSpan.get();
      Span span = Util.createEventlessSpan(
          currSpan.traceID, null, currSpan.spanID);   
      this.childSpan.set(span);
    }
    
    if (this.childSpan.get() != null) {
      Span span = this.childSpan.get();
      context.requestHandshakeMeta().put(
          TRACE_ID_KEY, ByteBuffer.wrap(span.traceID.bytes()));
      context.requestHandshakeMeta().put(
          SPAN_ID_KEY, ByteBuffer.wrap(span.spanID.bytes()));
      if (span.parentSpanID != null) {
        context.requestHandshakeMeta().put(
            PARENT_SPAN_ID_KEY, ByteBuffer.wrap(span.parentSpanID.bytes())); 
      }
    }
  }
  
  @Override
  public void serverConnecting(RPCContext context) {
    Map<CharSequence, ByteBuffer> meta = context.requestHandshakeMeta();
    // Are we being asked to propagate a trace?
    if (meta.containsKey(TRACE_ID_KEY) && enabled) {
      if (!(meta.containsKey(SPAN_ID_KEY))) {
        LOG.warn("Span ID missing for trace " +
            meta.get(TRACE_ID_KEY).toString());
        return; // should have been given full span data
      }
      byte[] spanIDBytes = new byte[8];
      meta.get(SPAN_ID_KEY).get(spanIDBytes);
      ID spanID = new ID();
      spanID.bytes(spanIDBytes);
      
      ID parentSpanID = null;
      if (meta.get(PARENT_SPAN_ID_KEY) != null) {
        parentSpanID = new ID();
        parentSpanID.bytes(meta.get(PARENT_SPAN_ID_KEY).array());
      }
      ID traceID = new ID();
      traceID.bytes(meta.get(TRACE_ID_KEY).array());
      
      Span span = Util.createEventlessSpan(traceID, spanID, parentSpanID);
      
      span.events = new GenericData.Array<TimestampedEvent>(
          100, Schema.createArray(TimestampedEvent.SCHEMA$));
      this.currentSpan.set(span);
    }
  }
  
  @Override
  public void clientFinishConnect(RPCContext context) { }

  @Override
  public void clientSendRequest(RPCContext context) {
    if (this.childSpan.get() != null) {
      Span child = this.childSpan.get();
      Util.addEvent(child, SpanEvent.CLIENT_SEND);
      child.messageName = new Utf8(
          context.getMessage().getName());
      child.requestPayloadSize = Util.getPayloadSize(
          context.getRequestPayload());
    }
  }
 
  @Override
  public void serverReceiveRequest(RPCContext context) {
    if (this.currentSpan.get() != null) {
      Span current = this.currentSpan.get();
      Util.addEvent(current, SpanEvent.SERVER_RECV);
      current.messageName = new Utf8(
          context.getMessage().getName());
      current.requestPayloadSize = Util.getPayloadSize(
          context.getRequestPayload());
    }
  }
  
  @Override
  public void serverSendResponse(RPCContext context) {
    if (this.currentSpan.get() != null) {
      Span current = this.currentSpan.get();
      Util.addEvent(current, SpanEvent.SERVER_SEND);
      current.responsePayloadSize = 
        Util.getPayloadSize(context.getResponsePayload());
      this.storage.addSpan(this.currentSpan.get());
      this.currentSpan.set(null);
    }
  }
  
  @Override
  public void clientReceiveResponse(RPCContext context) {
    if (this.childSpan.get() != null) {
      Span child = this.childSpan.get();
      Util.addEvent(child, SpanEvent.CLIENT_RECV);
      child.responsePayloadSize = 
        Util.getPayloadSize(context.getResponsePayload());
      this.storage.addSpan(this.childSpan.get());
      this.childSpan.set(null);
    }
  }
  
  public void stopClientServer() {
    try {
      this.clientFacingServer.stop();
    } catch (Exception e) {
      // ignore
    }
  }
  
  /**
   * Start a client-facing server. Can be overridden if users
   * prefer to attach client Servlet to their own server. 
   */
  protected void initializeClientServer() {
    clientFacingServer = new Server();
    Context context = new Context(clientFacingServer, "/");
    context.addServlet(new ServletHolder(new TraceClientServlet()), "/");
    boolean connected = false;
    SocketConnector socket = null;
    
    // Keep trying ports until we can connect
    while (!connected) {
      try {
        socket = new SocketConnector();
        socket.setPort(clientPort);
        clientFacingServer.addConnector(socket);
        clientFacingServer.start();
        connected = true;
      } catch (Exception e) {
        if (e instanceof BindException) {
          clientFacingServer.removeConnector(socket);
          clientPort = clientPort + 1;
          continue;
        }
        else {
          break; // Fail silently here (this is currently unused)
        }
      }
    }
  }
}
