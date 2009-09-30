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
import java.nio.ByteBuffer;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.avro.AvroRuntimeException;

import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

public class HttpServer extends HttpServlet implements Server {
  private Responder responder;
  private org.mortbay.jetty.Server server;

  public HttpServer(Responder responder, int port) throws IOException {
    this.responder = responder;
    this.server = new org.mortbay.jetty.Server(port);
    new Context(server,"/").addServlet(new ServletHolder(this), "/*");
    try {
      server.start();
    } catch (Exception e) {
      throw new AvroRuntimeException(e);
    }
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

  public void doPost(HttpServletRequest request, HttpServletResponse response)
        throws IOException, ServletException {
    response.setContentType("avro/binary");
    List<ByteBuffer> requestBuffers =
      HttpTransceiver.readBuffers(request.getInputStream());
    try {
      List<ByteBuffer> responseBuffers =
        responder.respond(requestBuffers);
      response.setContentLength(HttpTransceiver.getLength(responseBuffers));
      HttpTransceiver.writeBuffers(responseBuffers, response.getOutputStream());
    } catch (AvroRuntimeException e) {
      throw new ServletException(e);
    }
  }
}
