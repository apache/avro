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
package org.apache.avro;

import java.io.*;
import java.net.*;
import java.util.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import junit.framework.TestCase;
import org.codehaus.jackson.map.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.Protocol.Message;
import org.apache.avro.io.*;
import org.apache.avro.ipc.*;
import org.apache.avro.generic.*;
import org.apache.avro.util.*;

public class TestFsData extends TestCase {
  private static final Logger LOG
    = LoggerFactory.getLogger(TestProtocolGeneric.class);

  private static final int COUNT =
    Integer.parseInt(System.getProperty("test.count", "10"));
  private static final int BUFFER_SIZE = 64 * 1024;
  private static final File DIR=new File(System.getProperty("test.dir", "/tmp"));
  private static final File FILE = new File("src/test/schemata/fs-data.js");
  private static final Protocol PROTOCOL;
  static {
    try {
      PROTOCOL = Protocol.parse(FILE);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private class TestResponder extends GenericResponder {
    public TestResponder() { super(PROTOCOL); }
    public Object respond(Message message, Object request)
      throws AvroRemoteException {
      @SuppressWarnings(value="unchecked")
      GenericRecord params = (GenericRecord)request;

      if ("read".equals(message.getName())) {
        Utf8 block = (Utf8)params.get("block");
        long start = (Long)params.get("start");
        long length = (Long)params.get("length");
        
        ByteBuffer buffer = ByteBuffer.allocateDirect((int)length);
        while (buffer.hasRemaining()) {
          int n;
          try {
            n = fileChannel.read(buffer, start);
            if (n <= 0)
              throw new IOException("No bytes read: "+n);
          } catch (IOException e) {
            throw new AvroRemoteException(e);
          }
          length -= n;
          start += n;
        }
        buffer.flip();
        return buffer;
      }

      throw new AvroRuntimeException("unexpected message: "+message.getName());
    }
  }

  private static SocketServer server;
  private static Transceiver client;
  private static Requestor requestor;
  private static FileChannel fileChannel;

  public void testStartServer() throws Exception {
    // create a file that has COUNT * BUFFER_SIZE bytes of random data
    Random rand = new Random();
    File file = File.createTempFile("block-", ".dat", DIR);
    FileChannel out = new RandomAccessFile(file, "rw").getChannel();
    ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
    for (int i = 0; i < COUNT; i++) {
      buffer.limit(buffer.capacity());
      buffer.position(0);
      rand.nextBytes(buffer.array());
      while (buffer.hasRemaining()) {
        out.write(buffer);
      }
    }
    out.close();

    fileChannel = new FileInputStream(file).getChannel();

    server = new SocketServer(new TestResponder(), new InetSocketAddress(0));
    client = new SocketTransceiver(new InetSocketAddress(server.getPort()));
    requestor = new GenericRequestor(PROTOCOL, client);

  }

  public void testFsRead() throws IOException {
    for (int i = 0; i < COUNT; i++) {
      GenericRecord params =
        new GenericData.Record(PROTOCOL.getMessages().get("read").getRequest());
      params.put("block", new Utf8("foo"));
      params.put("start", new Long(i*BUFFER_SIZE));
      params.put("length", new Long(BUFFER_SIZE));
      ByteBuffer response = (ByteBuffer)requestor.request("read", params);
    }
  }

  public void testStopServer() throws Exception {
    server.close();
    fileChannel.close();
  }


}
