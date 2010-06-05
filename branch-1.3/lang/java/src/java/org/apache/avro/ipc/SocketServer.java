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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A socket-based server implementation. This uses a simple, non-standard wire
 * protocol and is not intended for production services. */
public class SocketServer extends Thread implements Server {
  private static final Logger LOG = LoggerFactory.getLogger(SocketServer.class);

  private Responder responder;
  private ServerSocketChannel channel;
  private ThreadGroup group;

  public SocketServer(Responder responder, SocketAddress addr)
    throws IOException {
    String name = "SocketServer on "+addr;

    this.responder = responder;
    this.group = new ThreadGroup(name);
    this.channel = ServerSocketChannel.open();

    channel.socket().bind(addr);

    setName(name);
    setDaemon(true);
    start();
  }

  public int getPort() { return channel.socket().getLocalPort(); }

  public void run() {
    LOG.info("starting "+channel.socket().getInetAddress());
    while (true) {
      try {
        new Connection(channel.accept());
      } catch (ClosedChannelException e) {
        return;
      } catch (IOException e) {
        LOG.warn("unexpected error", e);
        throw new RuntimeException(e);
      } finally {
        LOG.info("stopping "+channel.socket().getInetAddress());
      }
    }
  }

  public void close() {
    group.interrupt();
  }

  private class Connection extends SocketTransceiver implements Runnable {

    public Connection(SocketChannel channel) throws IOException {
      super(channel);

      Thread thread = new Thread(group, this);
      thread.setName("Connection to "+channel.socket().getRemoteSocketAddress());
      thread.setDaemon(true);
      thread.start();
    }

    public void run() {
      try {
        try {
          while (true) {
            writeBuffers(responder.respond(readBuffers()));
          }
        } catch (ClosedChannelException e) {
          return;
        } finally {
          close();
        }
      } catch (IOException e) {
        LOG.warn("unexpected error", e);
      }
    }

  }
  
  public static void main(String[] arg) throws Exception {
    SocketServer server = new SocketServer(null, new InetSocketAddress(0));
    System.out.println("started");
    server.join();
  }
}

