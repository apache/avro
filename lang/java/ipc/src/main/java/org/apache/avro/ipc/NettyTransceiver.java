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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

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
  private static final Logger LOG = LoggerFactory.getLogger(NettyTransceiver.class
      .getName());

  private ChannelFactory channelFactory;
  private Channel channel;
  
  private AtomicInteger serialGenerator = new AtomicInteger(0);
  private Map<Integer, CallFuture> requests = 
    new ConcurrentHashMap<Integer, CallFuture>();
  
  private Protocol remote;

  NettyTransceiver() {}
  
  public NettyTransceiver(InetSocketAddress addr) {
    // Set up.
    channelFactory = new NioClientSocketChannelFactory(Executors
        .newCachedThreadPool(), Executors.newCachedThreadPool());
    ClientBootstrap bootstrap = new ClientBootstrap(channelFactory);

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

    bootstrap.setOption("tcpNoDelay", true);

    // Make a new connection.
    ChannelFuture channelFuture = bootstrap.connect(addr);
    channelFuture.awaitUninterruptibly();
    if (!channelFuture.isSuccess()) {
      channelFuture.getCause().printStackTrace();
      throw new RuntimeException(channelFuture.getCause());
    }
    channel = channelFuture.getChannel();
  }

  public void close() {
    // Close the connection.
    channel.close().awaitUninterruptibly();
    // Shut down all thread pools to exit.
    channelFactory.releaseExternalResources();
  }

  @Override
  public String getRemoteName() {
    return channel.getRemoteAddress().toString();
  }

  /**
   * Override as non-synchronized method because the method is thread safe.
   */
  @Override
  public List<ByteBuffer> transceive(List<ByteBuffer> request)
      throws IOException {
    int serial = serialGenerator.incrementAndGet();
    NettyDataPack dataPack = new NettyDataPack(serial, request);
    CallFuture callFuture = new CallFuture();
    requests.put(serial, callFuture);
    channel.write(dataPack);
    try {
      return callFuture.get();
    } catch (InterruptedException e) {
      LOG.warn("failed to get the response", e);
      return null;
    } catch (ExecutionException e) {
      LOG.warn("failed to get the response", e);
      return null;
    } finally {
      requests.remove(serial);
    }
  }

  @Override
  public void writeBuffers(List<ByteBuffer> buffers) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ByteBuffer> readBuffers() throws IOException {
    throw new UnsupportedOperationException();  
  }
  
  @Override
  public Protocol getRemote() {
    return remote;
  }

  @Override
  public boolean isConnected() {
    return remote!=null;
  }

  @Override
  public void setRemote(Protocol protocol) {
    this.remote = protocol;
  }

  /**
   * Future class for a RPC call
   */
  class CallFuture implements Future<List<ByteBuffer>>{
    private Semaphore sem = new Semaphore(0);
    private List<ByteBuffer> response = null;
    
    public void setResponse(List<ByteBuffer> response) {
      this.response = response;
      sem.release();
    }
    
    public void releaseSemphore() {
      sem.release();
    }

    public List<ByteBuffer> getResponse() {
      return response;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public List<ByteBuffer> get() throws InterruptedException,
        ExecutionException {
      sem.acquire();
      return response;
    }

    @Override
    public List<ByteBuffer> get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if (sem.tryAcquire(timeout, unit)) {
        return response;
      } else {
        throw new TimeoutException();
      }
    }

    @Override
    public boolean isDone() {
      return sem.availablePermits()>0;
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
        LOG.info(e.toString());
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
      CallFuture callFuture = requests.get(dataPack.getSerial());
      if (callFuture==null) {
        throw new RuntimeException("Missing previous call info");
      }
      callFuture.setResponse(dataPack.getDatas());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      LOG.warn("Unexpected exception from downstream.", e.getCause());
      e.getChannel().close();
      // let the blocking waiting exit
      Iterator<CallFuture> it = requests.values().iterator();
      while(it.hasNext()) {
        it.next().releaseSemphore();
        it.remove();
      }
      
    }

  }

}
