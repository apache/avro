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

package org.apache.avro.ipc;

import java.io.IOException;

import org.apache.avro.AvroRuntimeException;

import org.mortbay.jetty.Connector;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

/** An HTTP-based RPC {@link Server}. */
public class HttpServer implements Server {
  private org.mortbay.jetty.Server server;

  /** Constructs a server to run on the named port. */
  public HttpServer(Responder responder, int port) throws IOException {
    this(new ResponderServlet(responder), null, port);
  }

  /** Constructs a server to run on the named port. */
  public HttpServer(ResponderServlet servlet, int port) throws IOException {
    this(servlet, null, port);
  }

  /** Constructs a server to run on the named port on the specified address. */
  public HttpServer(Responder responder, String bindAddress, int port) throws IOException {
    this(new ResponderServlet(responder), bindAddress, port);
  }

  /** Constructs a server to run on the named port on the specified address. */
  public HttpServer(ResponderServlet servlet, String bindAddress, int port) throws IOException {
    this.server = new org.mortbay.jetty.Server();
    SelectChannelConnector connector = new SelectChannelConnector();
    connector.setLowResourceMaxIdleTime(10000);
    connector.setAcceptQueueSize(128);
    connector.setResolveNames(false);
    connector.setUseDirectBuffers(false);
    if (bindAddress != null) {
      connector.setHost(bindAddress);
    }
    connector.setPort(port);
    server.addConnector(connector);
    new Context(server, "/").addServlet(new ServletHolder(servlet), "/*");
  }

  /** Constructs a server to run with the given connector. */
  public HttpServer(Responder responder, Connector connector) throws IOException {
    this(new ResponderServlet(responder), connector);
  }

  /** Constructs a server to run with the given connector. */
  public HttpServer(ResponderServlet servlet, Connector connector) throws IOException {
    this.server = new org.mortbay.jetty.Server();
    server.addConnector(connector);
    new Context(server, "/").addServlet(new ServletHolder(servlet), "/*");
  }

  public void addConnector(Connector connector) {
    server.addConnector(connector);
  }

  @Override
  public int getPort() { return server.getConnectors()[0].getLocalPort(); }

  @Override
  public void close() {
    try {
      server.stop();
    } catch (Exception e) {
      throw new AvroRuntimeException(e);
    }
  }

  /** Start the server.
   * @throws AvroRuntimeException if the underlying Jetty server
   * throws any exception while starting.
  */
  @Override
  public void start() {
    try {
      server.start();
    } catch (Exception e) {
      throw new AvroRuntimeException(e);
    }
  }

  @Override
  public void join() throws InterruptedException {
    server.join();
  }
}
