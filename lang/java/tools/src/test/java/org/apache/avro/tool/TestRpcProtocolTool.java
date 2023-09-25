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
package org.apache.avro.tool;

import org.apache.avro.Protocol;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;

/**
 *
 */
public class TestRpcProtocolTool {

  @ParameterizedTest
  @ValueSource(strings = { "http", "avro" })
  void rpcProtocol(String uriScheme) throws Exception {

    String protocolFile = System.getProperty("share.dir", "../../../share") + "/test/schemas/simple.avpr";

    Protocol simpleProtocol = Protocol.parse(new File(protocolFile));

    // start a simple server
    ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
    PrintStream p1 = new PrintStream(baos1);
    RpcReceiveTool receive = new RpcReceiveTool();

    receive.run1(null, p1, System.err,
        Arrays.asList(uriScheme + "://0.0.0.0:0/", protocolFile, "hello", "-data", "\"Hello!\""));

    // run the actual test
    ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
    PrintStream p2 = new PrintStream(baos2, true, "UTF-8");
    RpcProtocolTool testObject = new RpcProtocolTool();

    testObject.run(null, p2, System.err,
        Collections.singletonList(uriScheme + "://127.0.0.1:" + receive.server.getPort() + "/"));

    p2.flush();

    Assertions.assertEquals(simpleProtocol, Protocol.parse(baos2.toString("UTF-8")),
        "Expected the simple.avpr protocol to be echoed to standout");

    receive.server.close(); // force the server to finish
  }
}
