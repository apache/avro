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

import junit.framework.Assert;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.socket.SocketChannel;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

/**
 * This is a very specific test that verifies that if the NettyTransceiver fails
 * to connect it cleans up the netty channel that it has created.
 */
public class NettyTransceiverWhenFailsToConnect {

    @Test(expected = IOException.class)
    public void testNettyTransceiverReleasesNettyChannelOnFailingToConnect() throws Exception {
        ServerSocket serverSocket = null;
        LastChannelRememberingChannelFactory socketChannelFactory = null;

        try {
            serverSocket = new ServerSocket(0);
            socketChannelFactory = new LastChannelRememberingChannelFactory();

            try {
                new NettyTransceiver(
                        new InetSocketAddress(serverSocket.getLocalPort()),
                        socketChannelFactory,
                        1L
                );
            } finally {
                assertEquals("expected that the channel opened by the transceiver is closed",
                        false, socketChannelFactory.lastChannel.isOpen());
            }
        } finally {

            if (serverSocket != null) {
                // closing the server socket will actually free up the open channel in the
                // transceiver, which would have hung otherwise (pre AVRO-1407)
                serverSocket.close();
            }

            if (socketChannelFactory != null) {
                socketChannelFactory.releaseExternalResources();
            }
        }
    }

    class LastChannelRememberingChannelFactory extends NioClientSocketChannelFactory implements ChannelFactory {

        volatile SocketChannel lastChannel;

        @Override
        public SocketChannel newChannel(ChannelPipeline pipeline) {
            return lastChannel= super.newChannel(pipeline);
        }
    }
}
