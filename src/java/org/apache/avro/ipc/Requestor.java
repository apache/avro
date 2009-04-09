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
import java.security.*;

import org.apache.avro.*;
import org.apache.avro.Protocol.Message;
import org.apache.avro.util.*;
import org.apache.avro.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class for the client side of a protocol interaction. */
public abstract class Requestor {
  private static final Logger LOG = LoggerFactory.getLogger(Requestor.class);

  private Protocol local;
  private Protocol remote;
  private Transceiver transceiver;

  public Protocol getLocal() { return local; }
  public Protocol getRemote() { return remote; }
  public Transceiver getTransceiver() { return transceiver; }

  protected Requestor(Protocol local, Transceiver transceiver)
    throws IOException {
    this.local = local;
    this.transceiver = transceiver;
    this.remote = handshake();
  }

  private Protocol handshake() throws IOException {
    ByteBufferValueWriter out = new ByteBufferValueWriter();
    out.writeLong(Protocol.VERSION);              // send handshake version
    out.writeUtf8(new Utf8(local.toString()));    // send local protocol
    List<ByteBuffer> response = transceiver.transceive(out.getBufferList());
    ValueReader in = new ByteBufferValueReader(response);
    String remote = in.readUtf8(null).toString(); // read remote protocol
    return Protocol.parse(remote);                // parse & return it
  }

  /** Writes a request message and reads a response or error message. */
  public Object request(String messageName, Object request)
    throws IOException {

    // use local protocol to write request
    Message m = getLocal().getMessages().get(messageName);
    if (m == null)
      throw new AvroRuntimeException("Not a local message: "+messageName);

    ByteBufferValueWriter out = new ByteBufferValueWriter();

    out.writeUtf8(new Utf8(m.getName()));         // write message name
  
    writeRequest(m.getRequest(), request, out);

    List<ByteBuffer> response =
      getTransceiver().transceive(out.getBufferList());

    ValueReader in = new ByteBufferValueReader(response);

    // use remote protocol to read response
    m = getRemote().getMessages().get(messageName);
    if (m == null)
      throw new AvroRuntimeException("Not a remote message: "+messageName);
    if (!in.readBoolean()) {                      // no error
      return readResponse(m.getResponse(), in);
    } else {
      throw readError(m.getErrors(), in);
    }
  }

  /** Writes a request message. */
  public abstract void writeRequest(Schema schema, Object request,
                                    ValueWriter out) throws IOException;

  /** Reads a response message. */
  public abstract Object readResponse(Schema schema, ValueReader in)
    throws IOException;

  /** Reads an error message. */
  public abstract AvroRemoteException readError(Schema schema, ValueReader in)
    throws IOException;
}
