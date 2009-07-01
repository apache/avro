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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.*;
import org.apache.avro.Protocol.Message;
import org.apache.avro.util.*;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.*;
import org.apache.avro.specific.*;

/** Base class for the server side of a protocol interaction. */
public abstract class Responder {
  private static final Logger LOG = LoggerFactory.getLogger(Responder.class);

  private static final Schema META =
    Schema.createMap(Schema.create(Schema.Type.BYTES));
  private static final GenericDatumReader<Map<Utf8,ByteBuffer>> META_READER =
    new GenericDatumReader<Map<Utf8,ByteBuffer>>(META);
  private static final GenericDatumWriter<Map<Utf8,ByteBuffer>> META_WRITER =
    new GenericDatumWriter<Map<Utf8,ByteBuffer>>(META);

  private Map<Transceiver,Protocol> remotes
    = Collections.synchronizedMap(new WeakHashMap<Transceiver,Protocol>());
  private Map<MD5,Protocol> protocols
    = Collections.synchronizedMap(new HashMap<MD5,Protocol>());

  private Protocol local;
  private MD5 localHash;

  protected Responder(Protocol local) {
    this.local = local;
    this.localHash = new MD5();
    localHash.bytes(local.getMD5());
    protocols.put(localHash, local);
  }

  public Protocol getLocal() { return local; }

  /** Called by a server to deserialize a request, compute and serialize
   * a response or error. */
  public List<ByteBuffer> respond(Transceiver transceiver) throws IOException {
    ByteBufferInputStream bbi =
      new ByteBufferInputStream(transceiver.readBuffers());
    
    Decoder in = new BinaryDecoder(bbi);
    ByteBufferOutputStream bbo =
      new ByteBufferOutputStream();
    Encoder out = new BinaryEncoder(bbo);
    AvroRemoteException error = null;
    try {
      Protocol remote = handshake(transceiver, in, out);
      if (remote == null)                        // handshake failed
        return bbo.getBufferList();

      // read request using remote protocol specification
      Map<Utf8,ByteBuffer> requestMeta = META_READER.read(null, in);
      String messageName = in.readString(null).toString();
      Message m = remote.getMessages().get(messageName);
      if (m == null)
        throw new AvroRuntimeException("No such remote message: "+messageName);
      
      Object request = readRequest(m.getRequest(), in);

      // create response using local protocol specification
      m = getLocal().getMessages().get(messageName);
      if (m == null)
        throw new AvroRuntimeException("No such local message: "+messageName);
      Object response = null;
      try {
        response = respond(m, request);
      } catch (AvroRemoteException e) {
        error = e;
      } catch (Exception e) {
        LOG.warn("application error", e);
        error = new AvroRemoteException(new Utf8(e.toString()));
      }

      Map<Utf8,ByteBuffer> responseMeta = new HashMap<Utf8,ByteBuffer>();
      META_WRITER.write(responseMeta, out);
      out.writeBoolean(error != null);
      if (error == null)
        writeResponse(m.getResponse(), response, out);
      else
        writeError(m.getErrors(), error, out);

    } catch (AvroRuntimeException e) {            // system error
      LOG.warn("system error", e);
      error = new AvroRemoteException(e);
      bbo = new ByteBufferOutputStream();
      out = new BinaryEncoder(bbo);
      out.writeBoolean(true);
      writeError(Protocol.SYSTEM_ERRORS, error, out);
    }
      
    return bbo.getBufferList();
  }

  private SpecificDatumWriter handshakeWriter =
    new SpecificDatumWriter(HandshakeResponse._SCHEMA);
  private SpecificDatumReader handshakeReader =
    new SpecificDatumReader(HandshakeRequest._SCHEMA);

  private Protocol handshake(Transceiver transceiver,
                             Decoder in, Encoder out)
    throws IOException {
    Protocol remote = remotes.get(transceiver);
    if (remote != null) return remote;            // already established
      
    HandshakeRequest request = (HandshakeRequest)handshakeReader.read(null, in);
    remote = protocols.get(request.clientHash);
    if (remote == null && request.clientProtocol != null) {
      remote = Protocol.parse(request.clientProtocol.toString());
      protocols.put(request.clientHash, remote);
    }
    if (remote != null)
      remotes.put(transceiver, remote);
    HandshakeResponse response = new HandshakeResponse();
    if (localHash.equals(request.serverHash)) {
      response.match =
        remote == null ? HandshakeMatch.NONE : HandshakeMatch.BOTH;
    } else {
      response.match =
        remote == null ? HandshakeMatch.NONE : HandshakeMatch.CLIENT;
    }
    if (response.match != HandshakeMatch.BOTH) {
      response.serverProtocol = new Utf8(local.toString());
      response.serverHash = localHash;
    }
    handshakeWriter.write(response, out);
    return remote;
  }

  /** Computes the response for a message. */
  public abstract Object respond(Message message, Object request)
    throws AvroRemoteException;

  /** Reads a request message. */
  public abstract Object readRequest(Schema schema, Decoder in)
    throws IOException;

  /** Writes a response message. */
  public abstract void writeResponse(Schema schema, Object response,
                                     Encoder out) throws IOException;

  /** Writes an error message. */
  public abstract void writeError(Schema schema, AvroRemoteException error,
                                  Encoder out) throws IOException;

}
