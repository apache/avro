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

import java.nio.ByteBuffer;
import java.util.Map;

import junit.framework.Assert;

import org.apache.avro.ipc.RPCContext;
import org.apache.avro.ipc.RPCPlugin;
import org.apache.avro.util.Utf8;

/**
 * An implementation of an RPC metadata plugin API designed for unit testing.
 * This plugin tests handshake and call state by passing a string as metadata,
 * slowly building it up at each instrumentation point, testing it as it goes.
 * Finally, after the call or handshake is complete, the constructed string is
 * tested.
 */
public final class RPCMetaTestPlugin extends RPCPlugin {
  
  protected final Utf8 key;
  
  public RPCMetaTestPlugin(String keyname) {
    key = new Utf8(keyname);
  }
  
  @Override
  public void clientStartConnect(RPCContext context) {
    ByteBuffer buf = ByteBuffer.wrap("ap".getBytes());
    context.requestHandshakeMeta().put(key, buf);
  }
  
  @Override
  public void serverConnecting(RPCContext context) {
    
    Assert.assertNotNull(context.requestHandshakeMeta());
    Assert.assertNotNull(context.responseHandshakeMeta());
    
    if (!context.requestHandshakeMeta().containsKey(key)) return;
    
    ByteBuffer buf = context.requestHandshakeMeta().get(key);
    Assert.assertNotNull(buf);
    Assert.assertNotNull(buf.array());
    
    String partialstr = new String(buf.array());
    Assert.assertNotNull(partialstr);
    Assert.assertEquals("partial string mismatch", "ap", partialstr);
    
    buf = ByteBuffer.wrap((partialstr + "ac").getBytes());
    Assert.assertTrue(buf.remaining() > 0);
    context.responseHandshakeMeta().put(key, buf);
  }
  
  @Override
  public void clientFinishConnect(RPCContext context) {
    Map<Utf8,ByteBuffer> handshakeMeta = context.responseHandshakeMeta();
    
    Assert.assertNotNull(handshakeMeta);
    
    if (!handshakeMeta.containsKey(key)) return;
    
    ByteBuffer buf = handshakeMeta.get(key);
    Assert.assertNotNull(buf);
    Assert.assertNotNull(buf.array());
    
    String partialstr = new String(buf.array());
    Assert.assertNotNull(partialstr);
    Assert.assertEquals("partial string mismatch", "apac", partialstr);
    
    buf = ByteBuffer.wrap((partialstr + "he").getBytes());
    Assert.assertTrue(buf.remaining() > 0);
    handshakeMeta.put(key, buf);
    
    checkRPCMetaMap(handshakeMeta);
  }
  
  @Override
  public void clientSendRequest(RPCContext context) { 
    ByteBuffer buf = ByteBuffer.wrap("ap".getBytes());
    context.requestCallMeta().put(key, buf);
    Assert.assertNotNull(context.getMessage());
  }
  
  @Override
  public void serverReceiveRequest(RPCContext context) {
    Map<Utf8,ByteBuffer> meta = context.requestCallMeta();
    
    Assert.assertNotNull(meta);
    
    if (!meta.containsKey(key)) return;
    
    ByteBuffer buf = meta.get(key);
    Assert.assertNotNull(buf);
    Assert.assertNotNull(buf.array());
    
    String partialstr = new String(buf.array());
    Assert.assertNotNull(partialstr);
    Assert.assertEquals("partial string mismatch", "ap", partialstr);
    
    buf = ByteBuffer.wrap((partialstr + "a").getBytes());
    Assert.assertTrue(buf.remaining() > 0);
    meta.put(key, buf);
    
    Assert.assertNotNull(context.getMessage());
  }
  
  @Override
  public void serverSendResponse(RPCContext context) {
    Assert.assertNotNull(context.requestCallMeta());
    Assert.assertNotNull(context.responseCallMeta());
    
    if (!context.requestCallMeta().containsKey(key)) return;
    
    ByteBuffer buf = context.requestCallMeta().get(key);
    Assert.assertNotNull(buf);
    Assert.assertNotNull(buf.array());
    
    String partialstr = new String(buf.array());
    Assert.assertNotNull(partialstr);
    Assert.assertEquals("partial string mismatch", "apa", partialstr);
    
    buf = ByteBuffer.wrap((partialstr + "c").getBytes());
    Assert.assertTrue(buf.remaining() > 0);
    context.responseCallMeta().put(key, buf);
  }
  
  @Override
  public void clientReceiveResponse(RPCContext context) {
    Assert.assertNotNull(context.responseCallMeta());
    
    if (!context.responseCallMeta().containsKey(key)) return;
    
    ByteBuffer buf = context.responseCallMeta().get(key);
    Assert.assertNotNull(buf);
    Assert.assertNotNull(buf.array());
    
    String partialstr = new String(buf.array());
    Assert.assertNotNull(partialstr);
    Assert.assertEquals("partial string mismatch", "apac", partialstr);
    
    buf = ByteBuffer.wrap((partialstr + "he").getBytes());
    Assert.assertTrue(buf.remaining() > 0);
    context.responseCallMeta().put(key, buf);
    
    checkRPCMetaMap(context.responseCallMeta());
  }
  
  protected void checkRPCMetaMap(Map<Utf8,ByteBuffer> rpcMeta) {
    Assert.assertNotNull(rpcMeta);
    Assert.assertTrue("key not present in map", rpcMeta.containsKey(key));
    
    ByteBuffer keybuf = rpcMeta.get(key);
    Assert.assertNotNull(keybuf);
    Assert.assertTrue("key BB had nothing remaining", keybuf.remaining() > 0);
    
    String str = new String(keybuf.array());
    Assert.assertEquals("apache", str);
  }
  
}
