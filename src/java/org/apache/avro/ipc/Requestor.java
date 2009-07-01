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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.Protocol.Message;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class for the client side of a protocol interaction. */
public abstract class Requestor {
  private static final Logger LOG = LoggerFactory.getLogger(Requestor.class);

  private static final Schema META =
    Schema.createMap(Schema.create(Schema.Type.BYTES));
  private static final GenericDatumReader<Map<Utf8,ByteBuffer>> META_READER =
    new GenericDatumReader<Map<Utf8,ByteBuffer>>(META);
  private static final GenericDatumWriter<Map<Utf8,ByteBuffer>> META_WRITER =
    new GenericDatumWriter<Map<Utf8,ByteBuffer>>(META);

  private Protocol local;
  private Protocol remote;
  private boolean established, sendLocalText;
  private Transceiver transceiver;

  public Protocol getLocal() { return local; }
  public Protocol getRemote() { return remote; }
  public Transceiver getTransceiver() { return transceiver; }

  protected Requestor(Protocol local, Transceiver transceiver)
    throws IOException {
    this.local = local;
    this.transceiver = transceiver;
  }

  /** Writes a request message and reads a response or error message. */
  public Object request(String messageName, Object request)
    throws IOException {
    Decoder in;
    Message m;
    Map<Utf8,ByteBuffer> requestMeta = new HashMap<Utf8,ByteBuffer>();
    do {
      ByteBufferOutputStream bbo = new ByteBufferOutputStream();
      Encoder out = new BinaryEncoder(bbo);

      if (!established)                           // if not established
        writeHandshake(out);                      // prepend handshake

      // use local protocol to write request
      m = getLocal().getMessages().get(messageName);
      if (m == null)
        throw new AvroRuntimeException("Not a local message: "+messageName);
      
      META_WRITER.write(requestMeta, out);
      out.writeString(m.getName());       // write message name
      writeRequest(m.getRequest(), request, out); // write request payload
      
      List<ByteBuffer> response =                 // transceive
        getTransceiver().transceive(bbo.getBufferList());
      
      ByteBufferInputStream bbi = new ByteBufferInputStream(response);
      in = new BinaryDecoder(bbi);
      if (!established)                           // if not established
        readHandshake(in);                        // process handshake
    } while (!established);

    // use remote protocol to read response
    m = getRemote().getMessages().get(messageName);
    if (m == null)
      throw new AvroRuntimeException("Not a remote message: "+messageName);
    Map<Utf8,ByteBuffer> responseMeta = META_READER.read(null, in);
    if (!in.readBoolean()) {                      // no error
      return readResponse(m.getResponse(), in);
    } else {
      throw readError(m.getErrors(), in);
    }
  }

  private static final Map<String,MD5> REMOTE_HASHES =
    Collections.synchronizedMap(new HashMap<String,MD5>());
  private static final Map<MD5,Protocol> REMOTE_PROTOCOLS =
    Collections.synchronizedMap(new HashMap<MD5,Protocol>());

  private static final SpecificDatumWriter HANDSHAKE_WRITER =
    new SpecificDatumWriter(HandshakeRequest._SCHEMA);

  private static final SpecificDatumReader HANDSHAKE_READER =
    new SpecificDatumReader(HandshakeResponse._SCHEMA);

  private void writeHandshake(Encoder out) throws IOException {
    MD5 localHash = new MD5();
    localHash.bytes(local.getMD5());
    String remoteName = transceiver.getRemoteName();
    MD5 remoteHash = REMOTE_HASHES.get(remoteName);
    remote = REMOTE_PROTOCOLS.get(remoteHash);
    if (remoteHash == null) {                     // guess remote is local
      remoteHash = localHash;
      remote = local;
    }
    HandshakeRequest handshake = new HandshakeRequest();
    handshake.clientHash = localHash;
    handshake.serverHash = remoteHash;
    if (sendLocalText)
      handshake.clientProtocol = new Utf8(local.toString());
    HANDSHAKE_WRITER.write(handshake, out);
  }

  private void readHandshake(Decoder in) throws IOException {
    HandshakeResponse handshake =
      (HandshakeResponse)HANDSHAKE_READER.read(null, in);
    switch (handshake.match) {
    case BOTH:
      established = true;
      break;
    case CLIENT:
      LOG.debug("Handshake match = CLIENT");
      setRemote(handshake);
      established = true;
      break;
    case NONE:
      LOG.debug("Handshake match = NONE");
      setRemote(handshake);
      sendLocalText = true;
      break;
    default:
      throw new AvroRuntimeException("Unexpected match: "+handshake.match);
    }
  }

  private void setRemote(HandshakeResponse handshake) {
    remote = Protocol.parse(handshake.serverProtocol.toString());
    MD5 remoteHash = (MD5)handshake.serverHash;
    REMOTE_HASHES.put(transceiver.getRemoteName(), remoteHash);
    if (!REMOTE_PROTOCOLS.containsKey(remoteHash))
      REMOTE_PROTOCOLS.put(remoteHash, remote);
  }

  /** Writes a request message. */
  public abstract void writeRequest(Schema schema, Object request,
                                    Encoder out) throws IOException;

  /** Reads a response message. */
  public abstract Object readResponse(Schema schema, Decoder in)
    throws IOException;

  /** Reads an error message. */
  public abstract AvroRemoteException readError(Schema schema, Decoder in)
    throws IOException;
}
