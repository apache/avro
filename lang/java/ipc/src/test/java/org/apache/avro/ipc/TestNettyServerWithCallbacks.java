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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.test.Simple;
import org.apache.avro.test.TestError;
import org.apache.avro.test.TestRecord;
import org.apache.avro.util.Utf8;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests asynchronous RPCs with Netty.
 */
public class TestNettyServerWithCallbacks {
  private static Server server;
  private static Transceiver transceiver;
  private static Simple.Callback simpleClient;
  private static final AtomicBoolean ackFlag = new AtomicBoolean(false);
  private static final AtomicReference<CountDownLatch> ackLatch = 
    new AtomicReference<CountDownLatch>(new CountDownLatch(1));
  private static Simple simpleService = new SimpleImpl(ackFlag);
  
  @BeforeClass
  public static void initializeConnections() throws Exception {
    // start server
    Responder responder = new SpecificResponder(Simple.class, simpleService);
    server = new NettyServer(responder, new InetSocketAddress(0));
    server.start();
  
    int serverPort = server.getPort();
    System.out.println("server port : " + serverPort);

    transceiver = new NettyTransceiver(new InetSocketAddress(
        serverPort));
    simpleClient = SpecificRequestor.getClient(Simple.Callback.class, transceiver);
  }
  
  @AfterClass
  public static void tearDownConnections() throws Exception {
    if (transceiver != null) {
      transceiver.close();
    }
    if (server != null) {
      server.close();
    }
  }
  
  @Test
  public void greeting() throws Exception {
    // Test synchronous RPC:
    Assert.assertEquals(new Utf8("Hello, how are you?"), simpleClient.hello("how are you?"));
    
    // Test asynchronous RPC (future):
    CallFuture<CharSequence> future1 = new CallFuture<CharSequence>();
    simpleClient.hello("World!", future1);
    Assert.assertEquals(new Utf8("Hello, World!"), future1.get(2, TimeUnit.SECONDS));
    Assert.assertNull(future1.getError());
    
    // Test asynchronous RPC (callback):
    final CallFuture<CharSequence> future2 = new CallFuture<CharSequence>();
    simpleClient.hello("what's up?", new Callback<CharSequence>() {
      @Override
      public void handleResult(CharSequence result) {
        future2.handleResult(result);
      }
      @Override
      public void handleError(Throwable error) {
        future2.handleError(error);
      }
    });
    Assert.assertEquals(new Utf8("Hello, what's up?"), future2.get(2, TimeUnit.SECONDS));
    Assert.assertNull(future2.getError());
  }
  
  @Test
  public void echo() throws Exception {
    TestRecord record = new TestRecord();
    record.hash = new org.apache.avro.test.MD5();
    record.hash.bytes(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8 } );
    record.kind = org.apache.avro.test.Kind.FOO;
    record.name = "My Record";
    
    // Test synchronous RPC:
    Assert.assertEquals(record, simpleClient.echo(record));
    
    // Test asynchronous RPC (future):
    CallFuture<TestRecord> future1 = new CallFuture<TestRecord>();
    simpleClient.echo(record, future1);
    Assert.assertEquals(record, future1.get(2, TimeUnit.SECONDS));
    Assert.assertNull(future1.getError());
    
    // Test asynchronous RPC (callback):
    final CallFuture<TestRecord> future2 = new CallFuture<TestRecord>();
    simpleClient.echo(record, new Callback<TestRecord>() {
      @Override
      public void handleResult(TestRecord result) {
        future2.handleResult(result);
      }
      @Override
      public void handleError(Throwable error) {
        future2.handleError(error);
      }
    });
    Assert.assertEquals(record, future2.get(2, TimeUnit.SECONDS));
    Assert.assertNull(future2.getError());
  }
  
  @Test
  public void add() throws Exception {
    // Test synchronous RPC:
    Assert.assertEquals(8, simpleClient.add(2, 6));
    
    // Test asynchronous RPC (future):
    CallFuture<Integer> future1 = new CallFuture<Integer>();
    simpleClient.add(8, 8, future1);
    Assert.assertEquals(new Integer(16), future1.get(2, TimeUnit.SECONDS));
    Assert.assertNull(future1.getError());
    
    // Test asynchronous RPC (callback):
    final CallFuture<Integer> future2 = new CallFuture<Integer>();
    simpleClient.add(512, 256, new Callback<Integer>() {
      @Override
      public void handleResult(Integer result) {
        future2.handleResult(result);
      }
      @Override
      public void handleError(Throwable error) {
        future2.handleError(error);
      }
    });
    Assert.assertEquals(new Integer(768), future2.get(2, TimeUnit.SECONDS));
    Assert.assertNull(future2.getError());
  }
  
  @Test
  public void echoBytes() throws Exception {
    ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
    
    // Test synchronous RPC:
    Assert.assertEquals(byteBuffer, simpleClient.echoBytes(byteBuffer));
    
    // Test asynchronous RPC (future):
    CallFuture<ByteBuffer> future1 = new CallFuture<ByteBuffer>();
    simpleClient.echoBytes(byteBuffer, future1);
    Assert.assertEquals(byteBuffer, future1.get(2, TimeUnit.SECONDS));
    Assert.assertNull(future1.getError());
    
    // Test asynchronous RPC (callback):
    final CallFuture<ByteBuffer> future2 = new CallFuture<ByteBuffer>();
    simpleClient.echoBytes(byteBuffer, new Callback<ByteBuffer>() {
      @Override
      public void handleResult(ByteBuffer result) {
        future2.handleResult(result);
      }
      @Override
      public void handleError(Throwable error) {
        future2.handleError(error);
      }
    });
    Assert.assertEquals(byteBuffer, future2.get(2, TimeUnit.SECONDS));
    Assert.assertNull(future2.getError());
  }
  
  @Test()
  public void error() throws IOException, InterruptedException, TimeoutException {
    // Test synchronous RPC:
    try {
      simpleClient.error();
      Assert.fail("Expected " + TestError.class.getCanonicalName());
    } catch (TestError e) {
      // Expected
    } catch (AvroRemoteException e) {
      e.printStackTrace();
      Assert.fail("Unexpected error: " + e.toString());
    }
    
    // Test asynchronous RPC (future):
    CallFuture<Void> future = new CallFuture<Void>();
    simpleClient.error(future);
    try {
      future.get(2, TimeUnit.SECONDS);
      Assert.fail("Expected " + TestError.class.getCanonicalName() + " to be thrown");
    } catch (ExecutionException e) {
      Assert.assertTrue("Expected " + TestError.class.getCanonicalName(), 
          e.getCause() instanceof TestError);
    }
    Assert.assertNotNull(future.getError());
    Assert.assertTrue("Expected " + TestError.class.getCanonicalName(), 
        future.getError() instanceof TestError);
    Assert.assertNull(future.getResult());
    
    // Test asynchronous RPC (callback):
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Throwable> errorRef = new AtomicReference<Throwable>();
    simpleClient.error(new Callback<Void>() {
      @Override
      public void handleResult(Void result) {
        Assert.fail("Expected " + TestError.class.getCanonicalName());
      }
      @Override
      public void handleError(Throwable error) {
        errorRef.set(error);
        latch.countDown();
      }
    });
    Assert.assertTrue("Timed out waiting for error", latch.await(2, TimeUnit.SECONDS));
    Assert.assertNotNull(errorRef.get());
    Assert.assertTrue(errorRef.get() instanceof TestError);
  }
  
  @Test
  public void ack() throws Exception {
    simpleClient.ack();
    ackLatch.get().await(2, TimeUnit.SECONDS);
    Assert.assertTrue("Expected ack flag to be set", ackFlag.get());
    
    ackLatch.set(new CountDownLatch(1));
    simpleClient.ack();
    ackLatch.get().await(2, TimeUnit.SECONDS);
    Assert.assertFalse("Expected ack flag to be cleared", ackFlag.get());
  }
  
  @Ignore
  @Test
  public void performanceTest() throws Exception {
    final int threadCount = 8;
    final long runTimeMillis = 10 * 1000L;
    ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);
    
    System.out.println("Running performance test for " + runTimeMillis + "ms...");
    final AtomicLong rpcCount = new AtomicLong(0L);
    final AtomicBoolean runFlag = new AtomicBoolean(true);
    final CountDownLatch startLatch = new CountDownLatch(threadCount);
    for (int ii = 0; ii < threadCount; ii++) {
      threadPool.submit(new Runnable() {
        @Override
        public void run() {
          try {
            startLatch.countDown();
            startLatch.await(2, TimeUnit.SECONDS);
            while (runFlag.get()) {
              rpcCount.incrementAndGet();
              Assert.assertEquals(new Utf8("Hello, World!"), simpleClient.hello("World!"));
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      });
    }
    
    startLatch.await(2, TimeUnit.SECONDS);
    Thread.sleep(runTimeMillis);
    runFlag.set(false);
    threadPool.shutdown();
    Assert.assertTrue("Timed out shutting down thread pool", threadPool.awaitTermination(2, TimeUnit.SECONDS));
    System.out.println("Completed " + rpcCount.get() + " RPCs in " + runTimeMillis + 
        "ms => " + (((double)rpcCount.get() / (double)runTimeMillis) * 1000) + " RPCs/sec, " + 
        ((double)runTimeMillis / (double)rpcCount.get()) + " ms/RPC.");
  }
  
  /**
   * Implementation of the Simple interface.
   */
  private static class SimpleImpl implements Simple {
    private final AtomicBoolean ackFlag;
    
    /**
     * Creates a SimpleImpl.
     * @param ackFlag the AtomicBoolean to toggle when ack() is called.
     */
    public SimpleImpl(final AtomicBoolean ackFlag) {
      this.ackFlag = ackFlag;
    }
    
    @Override
    public CharSequence hello(CharSequence greeting) throws AvroRemoteException {
      return "Hello, " + greeting;
    }

    @Override
    public TestRecord echo(TestRecord record) throws AvroRemoteException {
      return record;
    }

    @Override
    public int add(int arg1, int arg2) throws AvroRemoteException {
      return arg1 + arg2;
    }

    @Override
    public ByteBuffer echoBytes(ByteBuffer data) throws AvroRemoteException {
      return data;
    }

    @Override
    public Void error() throws AvroRemoteException, TestError {
      TestError error = new TestError();
      error.message = "Test Message";
      throw error;
    }

    @Override
    synchronized public void ack() {
      ackFlag.set(!ackFlag.get());
      ackLatch.get().countDown();
    }
  }
}
