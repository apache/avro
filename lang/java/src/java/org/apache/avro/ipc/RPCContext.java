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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Protocol.Message;
import org.apache.avro.util.Utf8;

/**
 * This class represents the context of an RPC call or RPC handshake.
 * Designed to provide information to RPC plugin writers,
 * this class encapsulates information about the rpc exchange,
 * including handshake and call metadata.
 *
 */
public class RPCContext {
  
  protected Map<Utf8,ByteBuffer> requestHandshakeMeta, responseHandshakeMeta;
  protected Map<Utf8,ByteBuffer> requestCallMeta, responseCallMeta;
  
  protected Object response;
  protected Exception error;
  private Message message;
  
  /**
   * This is an access method for the handshake state
   * provided by the client to the server.
   * @return a map representing handshake state from
   * the client to the server
   */
  public Map<Utf8,ByteBuffer> requestHandshakeMeta() {
    if (requestHandshakeMeta == null) {
      requestHandshakeMeta = new HashMap<Utf8,ByteBuffer>();
    }
    return requestHandshakeMeta;
  }
  
  void setRequestHandshakeMeta(Map<Utf8,ByteBuffer> newmeta) {
    requestHandshakeMeta = newmeta;
  }
  
  /**
   * This is an access method for the handshake state
   * provided by the server back to the client
   * @return a map representing handshake state from
   * the server to the client
   */
  public Map<Utf8,ByteBuffer> responseHandshakeMeta() {
    if (responseHandshakeMeta == null) {
      responseHandshakeMeta = new HashMap<Utf8,ByteBuffer>();
    }
    return responseHandshakeMeta;
  }
  
  void setResponseHandshakeMeta(Map<Utf8,ByteBuffer> newmeta) {
    responseHandshakeMeta = newmeta;
  }
  
  /**
   * This is an access method for the per-call state
   * provided by the client to the server.
   * @return a map representing per-call state from
   * the client to the server
   */
  public Map<Utf8,ByteBuffer> requestCallMeta() {
    if (requestCallMeta == null) {
      requestCallMeta = new HashMap<Utf8,ByteBuffer>();
    }
    return requestCallMeta;
  }
  
  void setRequestCallMeta(Map<Utf8,ByteBuffer> newmeta) {
    requestCallMeta = newmeta;
  }
  
  /**
   * This is an access method for the per-call state
   * provided by the server back to the client.
   * @return a map representing per-call state from
   * the server to the client
   */
  public Map<Utf8,ByteBuffer> responseCallMeta() {
    if (responseCallMeta == null) {
      responseCallMeta = new HashMap<Utf8,ByteBuffer>();
    }
    return responseCallMeta;
  }
  
  void setResponseCallMeta(Map<Utf8,ByteBuffer> newmeta) {
    responseCallMeta = newmeta;
  }
  
  void setResponse(Object response) {
    this.response = response;
    this.error = null;
  }
  
  /**
   * The response object generated at the server,
   * if it exists.  If an exception was generated,
   * this will be null.
   * @return the response created by this RPC, no
   * null if an exception was generated
   */
  public Object response() {
    return response;
  }
  
  void setError(Exception error) {
    this.response = null;
    this.error = error;
  }
  
  /**
   * The exception generated at the server,
   * or null if no such exception has occured
   * @return the exception generated at the server, or
   * null if no such exception
   */
  public Exception error() {
    return error;
  }
  
  /**
   * Indicates whether an exception was generated
   * at the server
   * @return true is an exception was generated at
   * the server, or false if not
   */
  public boolean isError() {
    return error != null;
  }

  public void setMessage(Message message) {
    this.message = message;    
  }
  
  public Message getMessage() { return message; }
}
