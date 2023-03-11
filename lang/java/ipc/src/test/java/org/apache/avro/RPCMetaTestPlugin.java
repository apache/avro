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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.avro.ipc.RPCContext;
import org.apache.avro.ipc.RPCPlugin;

/**
 * An implementation of an RPC metadata plugin API designed for unit testing.
 * This plugin tests handshake and call state by passing a string as metadata,
 * slowly building it up at each instrumentation point, testing it as it goes.
 * Finally, after the call or handshake is complete, the constructed string is
 * tested. It also tests that RPC context data is appropriately filled in along
 * the way by Requestor and Responder classes.
 */
public final class RPCMetaTestPlugin extends RPCPlugin {

  protected final String key;

  public RPCMetaTestPlugin(String keyname) {
    key = keyname;
  }

  @Override
  public void clientStartConnect(RPCContext context) {
    ByteBuffer buf = ByteBuffer.wrap("ap".getBytes(StandardCharsets.UTF_8));
    context.requestHandshakeMeta().put(key, buf);
  }

  @Override
  public void serverConnecting(RPCContext context) {

    assertNotNull(context.requestHandshakeMeta());
    assertNotNull(context.responseHandshakeMeta());
    assertNull(context.getRequestPayload());
    assertNull(context.getResponsePayload());

    if (!context.requestHandshakeMeta().containsKey(key))
      return;

    ByteBuffer buf = context.requestHandshakeMeta().get(key);
    assertNotNull(buf);
    assertNotNull(buf.array());

    String partialstr = new String(buf.array(), StandardCharsets.UTF_8);
    assertNotNull(partialstr);
    assertEquals("ap", partialstr, "partial string mismatch");

    buf = ByteBuffer.wrap((partialstr + "ac").getBytes(StandardCharsets.UTF_8));
    assertTrue(buf.remaining() > 0);
    context.responseHandshakeMeta().put(key, buf);
  }

  @Override
  public void clientFinishConnect(RPCContext context) {
    Map<String, ByteBuffer> handshakeMeta = context.responseHandshakeMeta();

    assertNull(context.getRequestPayload());
    assertNull(context.getResponsePayload());
    assertNotNull(handshakeMeta);

    if (!handshakeMeta.containsKey(key))
      return;

    ByteBuffer buf = handshakeMeta.get(key);
    assertNotNull(buf);
    assertNotNull(buf.array());

    String partialstr = new String(buf.array(), StandardCharsets.UTF_8);
    assertNotNull(partialstr);
    assertEquals("apac", partialstr, "partial string mismatch");

    buf = ByteBuffer.wrap((partialstr + "he").getBytes(StandardCharsets.UTF_8));
    assertTrue(buf.remaining() > 0);
    handshakeMeta.put(key, buf);

    checkRPCMetaMap(handshakeMeta);
  }

  @Override
  public void clientSendRequest(RPCContext context) {
    ByteBuffer buf = ByteBuffer.wrap("ap".getBytes(StandardCharsets.UTF_8));
    context.requestCallMeta().put(key, buf);
    assertNotNull(context.getMessage());
    assertNotNull(context.getRequestPayload());
    assertNull(context.getResponsePayload());
  }

  @Override
  public void serverReceiveRequest(RPCContext context) {
    Map<String, ByteBuffer> meta = context.requestCallMeta();

    assertNotNull(meta);
    assertNotNull(context.getMessage());
    assertNull(context.getResponsePayload());

    if (!meta.containsKey(key))
      return;

    ByteBuffer buf = meta.get(key);
    assertNotNull(buf);
    assertNotNull(buf.array());

    String partialstr = new String(buf.array(), StandardCharsets.UTF_8);
    assertNotNull(partialstr);
    assertEquals("ap", partialstr, "partial string mismatch");

    buf = ByteBuffer.wrap((partialstr + "a").getBytes(StandardCharsets.UTF_8));
    assertTrue(buf.remaining() > 0);
    meta.put(key, buf);
  }

  @Override
  public void serverSendResponse(RPCContext context) {
    assertNotNull(context.requestCallMeta());
    assertNotNull(context.responseCallMeta());

    assertNotNull(context.getResponsePayload());

    if (!context.requestCallMeta().containsKey(key))
      return;

    ByteBuffer buf = context.requestCallMeta().get(key);
    assertNotNull(buf);
    assertNotNull(buf.array());

    String partialstr = new String(buf.array(), StandardCharsets.UTF_8);
    assertNotNull(partialstr);
    assertEquals("apa", partialstr, "partial string mismatch");

    buf = ByteBuffer.wrap((partialstr + "c").getBytes(StandardCharsets.UTF_8));
    assertTrue(buf.remaining() > 0);
    context.responseCallMeta().put(key, buf);
  }

  @Override
  public void clientReceiveResponse(RPCContext context) {
    assertNotNull(context.responseCallMeta());
    assertNotNull(context.getRequestPayload());

    if (!context.responseCallMeta().containsKey(key))
      return;

    ByteBuffer buf = context.responseCallMeta().get(key);
    assertNotNull(buf);
    assertNotNull(buf.array());

    String partialstr = new String(buf.array(), StandardCharsets.UTF_8);
    assertNotNull(partialstr);
    assertEquals("apac", partialstr, "partial string mismatch");

    buf = ByteBuffer.wrap((partialstr + "he").getBytes(StandardCharsets.UTF_8));
    assertTrue(buf.remaining() > 0);
    context.responseCallMeta().put(key, buf);

    checkRPCMetaMap(context.responseCallMeta());
  }

  protected void checkRPCMetaMap(Map<String, ByteBuffer> rpcMeta) {
    assertNotNull(rpcMeta);
    assertTrue(rpcMeta.containsKey(key), "key not present in map");

    ByteBuffer keybuf = rpcMeta.get(key);
    assertNotNull(keybuf);
    assertTrue(keybuf.remaining() > 0, "key BB had nothing remaining");

    String str = new String(keybuf.array(), StandardCharsets.UTF_8);
    assertEquals("apache", str);
  }

}
