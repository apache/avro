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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.avro.Protocol;
import org.apache.avro.ipc.NettyTransportCodec.NettyDataPack;
import org.apache.avro.ipc.NettyTransportCodec.NettyFrameDecoder;
import org.apache.avro.ipc.NettyTransportCodec.NettyFrameEncoder;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelState;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Netty-based {@link Transceiver} implementation.
 */
public class NettyTransceiver extends Transceiver {
  /** If not specified, the default connection timeout will be used (60 sec). */
  public static final long DEFAULT_CONNECTION_TIMEOUT_MILLIS = 60 * 1000L;
  public static final String NETTY_CONNECT_TIMEOUT_OPTION = 
      "connectTimeoutMillis";
  public static final String NETTY_TCP_NODELAY_OPTION = "tcpNoDelay";
  public static final boolean DEFAULT_TCP_NODELAY_VALUE = true;
  
  private static final Logger LOG = LoggerFactory.getLogger(NettyTransceiver.class
      .getName());

  private final AtomicInteger serialGenerator = new AtomicInteger(0);
  private final Map<Integer, Callback<List<ByteBuffer>>> requests = 
    new ConcurrentHashMap<Integer, Callback<List<ByteBuffer>>>();
  
  private final ChannelFactory channelFactory;
  private final long connectTimeoutMillis;
  private final ClientBootstrap bootstrap;
  private final InetSocketAddress remoteAddr;
  
  volatile ChannelFuture channelFuture;
  volatile boolean stopping;
  private final Object channelFutureLock = new Object();

  /**
   * Read lock must be acquired whenever using non-final state.
   * Write lock must be acquired whenever modifying state.
   */
  private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
  private Channel channel;       // Synchronized on stateLock
  private Protocol remote;       // Synchronized on stateLock

  NettyTransceiver() {
    channelFactory = null;
    connectTimeoutMillis = 0L;
    bootstrap = null;
    remoteAddr = null;
    channelFuture = null;
  }

  /**
   * Creates a NettyTransceiver, and attempts to connect to the given address.
   * {@link #DEFAULT_CONNECTION_TIMEOUT_MILLIS} is used for the connection 
   * timeout.
   * @param addr the address to connect to.
   * @throws IOException if an error occurs connecting to the given address.
   */
  public NettyTransceiver(InetSocketAddress addr) throws IOException {
    this(addr, DEFAULT_CONNECTION_TIMEOUT_MILLIS);
  }
  
  /**
   * Creates a NettyTransceiver, and attempts to connect to the given address.
   * @param addr the address to connect to.
   * @param connectTimeoutMillis maximum amount of time to wait for connection 
   * establishment in milliseconds, or null to use 
   * {@link #DEFAULT_CONNECTION_TIMEOUT_MILLIS}.
   * @throws IOException if an error occurs connecting to the given address.
   */
  public NettyTransceiver(InetSocketAddress addr, 
      Long connectTimeoutMillis) throws IOException {
    this(addr, new NioClientSocketChannelFactory(
        Executors.newCachedThreadPool(new NettyTransceiverThreadFactory(
            "Avro " + NettyTransceiver.class.getSimpleName() + " Boss")), 
        Executors.newCachedThreadPool(new NettyTransceiverThreadFactory(
            "Avro " + NettyTransceiver.class.getSimpleName() + " I/O Worker"))), 
        connectTimeoutMillis);
  }

  /**
   * Creates a NettyTransceiver, and attempts to connect to the given address.
   * {@link #DEFAULT_CONNECTION_TIMEOUT_MILLIS} is used for the connection 
   * timeout.
   * @param addr the address to connect to.
   * @param channelFactory the factory to use to create a new Netty Channel.
   * @throws IOException if an error occurs connecting to the given address.
   */
  public NettyTransceiver(InetSocketAddress addr, ChannelFactory channelFactory) 
    throws IOException {
    this(addr, channelFactory, buildDefaultBootstrapOptions(null));
  }
  
  /**
   * Creates a NettyTransceiver, and attempts to connect to the given address.
   * @param addr the address to connect to.
   * @param channelFactory the factory to use to create a new Netty Channel.
   * @param connectTimeoutMillis maximum amount of time to wait for connection 
   * establishment in milliseconds, or null to use 
   * {@link #DEFAULT_CONNECTION_TIMEOUT_MILLIS}.
   * @throws IOException if an error occurs connecting to the given address.
   */
  public NettyTransceiver(InetSocketAddress addr, ChannelFactory channelFactory, 
      Long connectTimeoutMillis) throws IOException {
    this(addr, channelFactory, 
        buildDefaultBootstrapOptions(connectTimeoutMillis));
  }
  
  /**
   * Creates a NettyTransceiver, and attempts to connect to the given address.
   * It is strongly recommended that the {@link #NETTY_CONNECT_TIMEOUT_OPTION} 
   * option be set to a reasonable timeout value (a Long value in milliseconds) 
   * to prevent connect/disconnect attempts from hanging indefinitely.  It is 
   * also recommended that the {@link #NETTY_TCP_NODELAY_OPTION} option be set 
   * to true to minimize RPC latency.
   * @param addr the address to connect to.
   * @param channelFactory the factory to use to create a new Netty Channel.
   * @param nettyClientBootstrapOptions map of Netty ClientBootstrap options 
   * to use.
   * @throws IOException if an error occurs connecting to the given address.
   */
  public NettyTransceiver(InetSocketAddress addr, ChannelFactory channelFactory, 
      Map<String, Object> nettyClientBootstrapOptions) throws IOException {
    if (channelFactory == null) {
      throw new NullPointerException("channelFactory is null");
    }
    
    // Set up.
    this.channelFactory = channelFactory;
    this.connectTimeoutMillis = (Long) 
        nettyClientBootstrapOptions.get(NETTY_CONNECT_TIMEOUT_OPTION);
    bootstrap = new ClientBootstrap(channelFactory);
    remoteAddr = addr;

    // Configure the event pipeline factory.
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline p = Channels.pipeline();
        p.addLast("frameDecoder", new NettyFrameDecoder());
        p.addLast("frameEncoder", new NettyFrameEncoder());
        p.addLast("handler", new NettyClientAvroHandler());
        return p;
      }
    });

    if (nettyClientBootstrapOptions != null) {
      LOG.debug("Using Netty bootstrap options: " + 
          nettyClientBootstrapOptions);
      bootstrap.setOptions(nettyClientBootstrapOptions);
    }

    // Make a new connection.
    stateLock.readLock().lock();
    try {
      getChannel();
    } finally {
      stateLock.readLock().unlock();
    }
  }
  
  /**
   * Creates the default options map for the Netty ClientBootstrap.
   * @param connectTimeoutMillis connection timeout in milliseconds, or null 
   * if no timeout is desired.
   * @return the map of Netty bootstrap options.
   */
  private static Map<String, Object> buildDefaultBootstrapOptions(
      Long connectTimeoutMillis) {
    Map<String, Object> options = new HashMap<String, Object>(2);
    options.put(NETTY_TCP_NODELAY_OPTION, DEFAULT_TCP_NODELAY_VALUE);
    options.put(NETTY_CONNECT_TIMEOUT_OPTION, 
        connectTimeoutMillis == null ? DEFAULT_CONNECTION_TIMEOUT_MILLIS : 
          connectTimeoutMillis);
    return options;
  }
  
  /**
   * Tests whether the given channel is ready for writing.
   * @return true if the channel is open and ready; false otherwise.
   */
  private static boolean isChannelReady(Channel channel) {
    return (channel != null) && 
      channel.isOpen() && channel.isBound() && channel.isConnected();
  }
  
  /**
   * Gets the Netty channel.  If the channel is not connected, first attempts 
   * to connect.
   * NOTE: The stateLock read lock *must* be acquired before calling this 
   * method.
   * @return the Netty channel
   * @throws IOException if an error occurs connecting the channel.
   */
  private Channel getChannel() throws IOException {
    if (!isChannelReady(channel)) {
      // Need to reconnect
      // Upgrade to write lock
      stateLock.readLock().unlock();
      stateLock.writeLock().lock();
      try {
        if (!isChannelReady(channel)) {
          synchronized(channelFutureLock) {
            if (!stopping) {
          LOG.debug("Connecting to " + remoteAddr);
              channelFuture = bootstrap.connect(remoteAddr);
            }
          }
          if (channelFuture != null) {
          channelFuture.awaitUninterruptibly(connectTimeoutMillis);

            synchronized(channelFutureLock) {
          if (!channelFuture.isSuccess()) {
            throw new IOException("Error connecting to " + remoteAddr, 
                channelFuture.getCause());
          }
          channel = channelFuture.getChannel();
              channelFuture = null;
            }
          }
        }
      } finally {
        // Downgrade to read lock:
        stateLock.readLock().lock();
        stateLock.writeLock().unlock();
      }
    }
    return channel;
  }
  
  /**
   * Closes the connection to the remote peer if connected.
   */
  private void disconnect() {
    disconnect(false, false, null);
  }
  
  /**
   * Closes the connection to the remote peer if connected.
   * @param awaitCompletion if true, will block until the close has completed.
   * @param cancelPendingRequests if true, will drain the requests map and 
   * send an IOException to all Callbacks.
   * @param cause if non-null and cancelPendingRequests is true, this Throwable 
   * will be passed to all Callbacks.
   */
  private void disconnect(boolean awaitCompletion, boolean cancelPendingRequests,
      Throwable cause) {
    Channel channelToClose = null;
    Map<Integer, Callback<List<ByteBuffer>>> requestsToCancel = null;
    boolean stateReadLockHeld = stateLock.getReadHoldCount() != 0;

    ChannelFuture channelFutureToCancel = null;
    synchronized(channelFutureLock) {
        if (stopping && channelFuture != null) {
          channelFutureToCancel = channelFuture;
          channelFuture = null;
        }
    }
    if (channelFutureToCancel != null) {
      channelFutureToCancel.cancel();
    }
    
    if (stateReadLockHeld) {
      stateLock.readLock().unlock();
    }
    stateLock.writeLock().lock();
    try {
      if (channel != null) {
        if (cause != null) {
          LOG.debug("Disconnecting from " + remoteAddr, cause);
        }
        else {
          LOG.debug("Disconnecting from " + remoteAddr);
        }
        channelToClose = channel;
        channel = null;
        remote = null;
        if (cancelPendingRequests) {
          // Remove all pending requests (will be canceled after relinquishing 
          // write lock).
          requestsToCancel = 
            new ConcurrentHashMap<Integer, Callback<List<ByteBuffer>>>(requests);
          requests.clear();
        }
      }
    } finally {
      if (stateReadLockHeld) {
        stateLock.readLock().lock();
      }
      stateLock.writeLock().unlock();
    }
    
    // Cancel any pending requests by sending errors to the callbacks:
    if ((requestsToCancel != null) && !requestsToCancel.isEmpty()) {
      LOG.debug("Removing " + requestsToCancel.size() + " pending request(s).");
      for (Callback<List<ByteBuffer>> request : requestsToCancel.values()) {
        request.handleError(
            cause != null ? cause : 
              new IOException(getClass().getSimpleName() + " closed"));
      }
    }
    
    // Close the channel:
    if (channelToClose != null) {
      ChannelFuture closeFuture = channelToClose.close();
      if (awaitCompletion && (closeFuture != null)) {
        closeFuture.awaitUninterruptibly(connectTimeoutMillis);
      }
    }
  }
  
  /**
   * Netty channels are thread-safe, so there is no need to acquire locks.
   * This method is a no-op.
   */
  @Override
  public void lockChannel() {
    
  }
  
  /**
   * Netty channels are thread-safe, so there is no need to acquire locks.
   * This method is a no-op.
   */
  @Override
  public void unlockChannel() {
    
  }

  public void close() {
    try {
      // Close the connection:
      stopping = true;
      disconnect(true, true, null);
    } finally {
      // Shut down all thread pools to exit.
      channelFactory.releaseExternalResources();
    }
  }

  @Override
  public String getRemoteName() throws IOException {
    stateLock.readLock().lock();
    try {
      return getChannel().getRemoteAddress().toString();
    } finally {
      stateLock.readLock().unlock();
    }
  }

  /**
   * Override as non-synchronized method because the method is thread safe.
   */
  @Override
  public List<ByteBuffer> transceive(List<ByteBuffer> request) 
    throws IOException {
    try {
      CallFuture<List<ByteBuffer>> transceiverFuture = new CallFuture<List<ByteBuffer>>();
      transceive(request, transceiverFuture);
      return transceiverFuture.get();
    } catch (InterruptedException e) {
      LOG.debug("failed to get the response", e);
      return null;
    } catch (ExecutionException e) {
      LOG.debug("failed to get the response", e);
      return null;
    }
  }
  
  @Override
  public void transceive(List<ByteBuffer> request, 
      Callback<List<ByteBuffer>> callback) throws IOException {
    stateLock.readLock().lock();
    try {
      int serial = serialGenerator.incrementAndGet();
      NettyDataPack dataPack = new NettyDataPack(serial, request);
      requests.put(serial, callback);
      writeDataPack(dataPack);
    } finally {
      stateLock.readLock().unlock();
    }
  }
  
  @Override
  public void writeBuffers(List<ByteBuffer> buffers) throws IOException {
    stateLock.readLock().lock();
    try {
      writeDataPack(
          new NettyDataPack(serialGenerator.incrementAndGet(), buffers));
    } finally {
      stateLock.readLock().unlock();
    }
  }
  
  /**
   * Writes a NettyDataPack, reconnecting to the remote peer if necessary.
   * NOTE: The stateLock read lock *must* be acquired before calling this 
   * method.
   * @param dataPack the data pack to write.
   * @throws IOException if an error occurs connecting to the remote peer.
   */
  private void writeDataPack(NettyDataPack dataPack) throws IOException {
    getChannel().write(dataPack);
  }

  @Override
  public List<ByteBuffer> readBuffers() throws IOException {
    throw new UnsupportedOperationException();  
  }
  
  @Override
  public Protocol getRemote() {
    stateLock.readLock().lock();
    try {
      return remote;
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public boolean isConnected() {
    stateLock.readLock().lock();
    try {
      return remote!=null;
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public void setRemote(Protocol protocol) {
    stateLock.writeLock().lock();
    try {
      this.remote = protocol;
    } finally {
      stateLock.writeLock().unlock();
    }
  }

  /**
   * Avro client handler for the Netty transport 
   */
  class NettyClientAvroHandler extends SimpleChannelUpstreamHandler {

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e)
        throws Exception {
      if (e instanceof ChannelStateEvent) {
        LOG.debug(e.toString());
        ChannelStateEvent cse = (ChannelStateEvent)e;
        if ((cse.getState() == ChannelState.OPEN) && (Boolean.FALSE.equals(cse.getValue()))) {
          // Server closed connection; disconnect client side
          LOG.debug("Remote peer " + remoteAddr + " closed connection.");
          disconnect(false, true, null);
        }
      }
      super.handleUpstream(ctx, e);
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
        throws Exception {
      // channel = e.getChannel();
      super.channelOpen(ctx, e);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, final MessageEvent e) {
      NettyDataPack dataPack = (NettyDataPack)e.getMessage();
      Callback<List<ByteBuffer>> callback = requests.get(dataPack.getSerial());
      if (callback==null) {
        throw new RuntimeException("Missing previous call info");
      }
      try {
        callback.handleResult(dataPack.getDatas());
      } finally {
        requests.remove(dataPack.getSerial());
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      disconnect(false, true, e.getCause());      
    }

  }

  /**
   * Creates threads with unique names based on a specified name prefix.
   */
  private static class NettyTransceiverThreadFactory implements ThreadFactory {
    private final AtomicInteger threadId = new AtomicInteger(0);
    private final String prefix;
    
    /**
     * Creates a NettyTransceiverThreadFactory that creates threads with the 
     * specified name.
     * @param prefix the name prefix to use for all threads created by this 
     * ThreadFactory.  A unique ID will be appended to this prefix to form the 
     * final thread name.
     */
    public NettyTransceiverThreadFactory(String prefix) {
      this.prefix = prefix;
    }
    
    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(r);
      thread.setName(prefix + " " + threadId.incrementAndGet());
      return thread;
    }
  }
}
