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

import java.io.*;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.ClosedByInterruptException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.avro.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A simple socket-based server implementation. */
public class SocketServer extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(SocketServer.class);

  private Responder responder;
  private ServerSocketChannel channel;

  public SocketServer(Responder responder, SocketAddress addr)
    throws IOException {
    this.responder = responder;

    this.channel = ServerSocketChannel.open();
    channel.socket().bind(addr);

    setName("SocketServer on "+addr);
    setDaemon(true);
    LOG.info("starting on "+addr);
    start();
  }

  public int getPort() { return channel.socket().getLocalPort(); }

  public void run() {
    while (true) {
      try {
        LOG.info("listening on "+channel.socket().getInetAddress());
        new Connection(channel.accept());
      } catch (ClosedByInterruptException e) {
        return;
      } catch (IOException e) {
        LOG.warn("unexpected error", e);
        throw new RuntimeException(e);
      }
    }
  }

  public void close() {
    interrupt();
  }

  private class Connection extends SocketTransceiver implements Runnable {

    public Connection(SocketChannel channel) {
      super(channel);

      Thread thread = new Thread(this);
      LOG.info("connection from "+channel.socket().getRemoteSocketAddress());
      thread.setName("Connection for "+channel);
      thread.setDaemon(true);
      thread.start();
    }

    public void run() {
      try {
        Protocol remote = responder.handshake(this);
        while (true) {
          writeBuffers(responder.respond(remote, readBuffers()));
        }
      } catch (ClosedByInterruptException e) {
        return;
      } catch (IOException e) {
        LOG.warn("unexpected error", e);
        throw new RuntimeException(e);
      } finally {
        try {
          super.close();
        } catch (IOException e) {
          LOG.warn("unexpected error", e);
          throw new RuntimeException(e);
        }
      }
    }

    public void close() {
      interrupt();
    }
  }
  
  public static void main(String arg[]) throws Exception {
    SocketServer server = new SocketServer(null, new InetSocketAddress(0));
    System.out.println("started");
    server.join();
  }
}
