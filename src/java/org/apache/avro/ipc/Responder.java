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
import org.apache.avro.io.*;

/** Base class for the server side of a protocol interaction. */
public abstract class Responder {
  private static final Logger LOG = LoggerFactory.getLogger(Responder.class);

  private Protocol local;

  protected Responder(Protocol local) {
    this.local = local;
  }

  public Protocol getLocal() { return local; }

  /** Returns the remote protocol. */
  protected Protocol handshake(Transceiver server) throws IOException {
    ValueReader in = new ByteBufferValueReader(server.readBuffers());
    ByteBufferValueWriter out = new ByteBufferValueWriter();

    long version = in.readLong();                 // read handshake version
    if (version != Protocol.VERSION)
      throw new AvroRuntimeException("Incompatible request version: "+version);
                                                  // read remote protocol
    Protocol remote = Protocol.parse(in.readUtf8(null).toString());

    out.writeUtf8(new Utf8(local.toString()));    // write local protocol
    server.writeBuffers(out.getBufferList());

    return remote;                                // return remote protocol
  }

  /** Called by a server to deserialize requests, compute and serialize
   * responses and errors. */
  public List<ByteBuffer> respond(Protocol remote, List<ByteBuffer> input)
    throws IOException {
    ByteBufferValueWriter out = new ByteBufferValueWriter();
    AvroRemoteException error = null;
    try {
      // read request using remote protocol specification
      ValueReader in = new ByteBufferValueReader(input);
      String messageName = in.readUtf8(null).toString();

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

      out.writeBoolean(error != null);
      if (error == null)
        writeResponse(m.getResponse(), response, out);
      else
        writeError(m.getErrors(), error, out);

    } catch (AvroRuntimeException e) {            // system error
      LOG.warn("system error", e);
      error = new AvroRemoteException(e);
      out = new ByteBufferValueWriter();
      out.writeBoolean(true);
      writeError(Protocol.SYSTEM_ERRORS, error, out);
    }
      
    return out.getBufferList();
  }


  /** Computes the response for a message. */
  public abstract Object respond(Message message, Object request)
    throws AvroRemoteException;

  /** Reads a request message. */
  public abstract Object readRequest(Schema schema, ValueReader in)
    throws IOException;

  /** Writes a response message. */
  public abstract void writeResponse(Schema schema, Object response,
                                     ValueWriter out) throws IOException;

  /** Writes an error message. */
  public abstract void writeError(Schema schema, AvroRemoteException error,
                                  ValueWriter out) throws IOException;

}
