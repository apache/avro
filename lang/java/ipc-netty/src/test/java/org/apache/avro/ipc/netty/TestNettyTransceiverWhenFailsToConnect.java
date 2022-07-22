/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.ipc.netty;

import org.apache.avro.ipc.Transceiver;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.InetSocketAddress;
import java.net.ServerSocket;

import io.netty.channel.socket.SocketChannel;

/**
 * This is a very specific test that verifies that if the NettyTransceiver fails
 * to connect it cleans up the netty channel that it has created.
 */
public class TestNettyTransceiverWhenFailsToConnect {
  SocketChannel channel = null;

  @Test
  void nettyTransceiverReleasesNettyChannelOnFailingToConnect() throws Exception {
    assertThrows(IOException.class, () -> {
      try (ServerSocket serverSocket = new ServerSocket(0)) {
        try (Transceiver t = new NettyTransceiver(new InetSocketAddress(serverSocket.getLocalPort()), 0, c -> {
          channel = c;
        })) {
          fail("should have thrown an exception");
        }
      } finally {
        assertTrue(channel == null || channel.isShutdown(), "Channel not shut down");
      }
    });
  }
}
