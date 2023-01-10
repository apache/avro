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
package org.apache.avro;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.Test;

import org.apache.avro.Protocol.Message;

public class TestProtocolParsing {
  public static Protocol getSimpleProtocol() throws IOException {
    File file = new File("../../../share/test/schemas/simple.avpr");
    return Protocol.parse(file);
  }

  @Test
  void parsing() throws IOException {
    Protocol protocol = getSimpleProtocol();

    assertEquals(protocol.getDoc(), "Protocol used for testing.");
    assertEquals(6, protocol.getMessages().size());
    assertEquals("Pretend you're in a cave!", protocol.getMessages().get("echo").getDoc());
  }

  private static Message parseMessage(String message) throws Exception {
    return Protocol.parse("{\"protocol\": \"org.foo.Bar\"," + "\"types\": []," + "\"messages\": {" + message + "}}")
        .getMessages().values().iterator().next();
  }

  @Test
  void oneWay() throws Exception {
    Message m;
    // permit one-way messages w/ null response
    m = parseMessage("\"ack\": {" + "\"request\": []," + "\"response\": \"null\"," + "\"one-way\": true}");
    assertTrue(m.isOneWay());
    // permit one-way messages w/o response
    m = parseMessage("\"ack\": {" + "\"request\": []," + "\"one-way\": true}");
    assertTrue(m.isOneWay());
  }

  @Test
  void oneWayResponse() throws Exception {
    assertThrows(SchemaParseException.class, () -> {
      // prohibit one-way messages with a non-null response type
      parseMessage("\"ack\": {" + "\"request\": [\"string\"]," + "\"response\": \"string\"," + "\"one-way\": true}");
    });
  }

  @Test
  void oneWayError() throws Exception {
    assertThrows(SchemaParseException.class, () -> {
      // prohibit one-way messages with errors
      parseMessage("\"ack\": {" + "\"request\": [\"string\"]," + "\"errors\": []," + "\"one-way\": true}");
    });
  }

  @Test
  void messageFieldAliases() throws IOException {
    Protocol protocol = getSimpleProtocol();
    final Message msg = protocol.getMessages().get("hello");
    assertNotNull(msg);
    final Schema.Field field = msg.getRequest().getField("greeting");
    assertNotNull(field);
    assertTrue(field.aliases().contains("salute"));
  }

  @Test
  void messageCustomProperties() throws IOException {
    Protocol protocol = getSimpleProtocol();
    final Message msg = protocol.getMessages().get("hello");
    assertNotNull(msg);
    final Schema.Field field = msg.getRequest().getField("greeting");
    assertNotNull(field);
    assertEquals("customValue", field.getProp("customProp"));
  }
}
