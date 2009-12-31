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
package org.apache.avro.tool;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;

import org.apache.avro.Protocol;
import org.junit.Test;

public class TestRpcReceiveAndSendTools {
  
  /**
   * Starts a server (using the tool) and sends a single message to it.
   */
  @Test
  public void testServeAndSend() throws Exception {
    Protocol protocol = Protocol.parse("" +
                "{\"protocol\": \"Minimal\", " +
                "\"messages\": { \"sink\": {" +
                "   \"request\": [{\"name\": \"a\", \"type\": \"string\"}], " +
                "   \"response\": \"string\"} } }");
    ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
    PrintStream p1 = new PrintStream(baos1);
    RpcReceiveTool receive = new RpcReceiveTool();
    receive.run1(null, p1, System.err, 
        Arrays.asList(protocol.toString(), "0", "sink", "\"omega\""));
    int port = receive.server.getPort();
    ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
    PrintStream p2 = new PrintStream(baos2);
    RpcSendTool send = new RpcSendTool();
    send.run(null, p2, System.err,
        Arrays.asList(protocol.toString(), "sink", "localhost", 
            Integer.toString(port), "{ \"a\": \"alpha\" }"));
    receive.run2(System.err);
    
    assertEquals("sink\t{\"a\":\"alpha\"}\n", baos1.toString("UTF-8"));
    assertEquals("\"omega\"\n", baos2.toString("UTF-8"));
  }
}
