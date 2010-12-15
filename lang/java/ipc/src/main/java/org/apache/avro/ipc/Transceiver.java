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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.avro.Protocol;

/** Base transport class used by {@link Requestor}. */
public abstract class Transceiver implements Closeable {

  public abstract String getRemoteName();

  /** Called by {@link Requestor#request(String,Object)} for two-way messages.
   * By default calls {@link #writeBuffers(List)} followed by
   * {@link #readBuffers()}. */
  public synchronized List<ByteBuffer> transceive(List<ByteBuffer> request)
    throws IOException {
    writeBuffers(request);
    return readBuffers();
  }

  /** Called by the default definition of {@link #transceive(List)}.*/
  public abstract List<ByteBuffer> readBuffers() throws IOException;

  /** Called by {@link Requestor#request(String,Object)} for one-way messages.*/
  public abstract void writeBuffers(List<ByteBuffer> buffers)
    throws IOException;

  /** True if a handshake has been completed for this connection.  Used to
   * determine whether a handshake need be completed prior to a one-way
   * message.  Requests and responses are always prefixed by handshakes, but
   * one-way messages.  If the first request sent over a connection is one-way,
   * then a handshake-only response is returned.  Subsequent one-way messages
   * over the connection will have no response data sent.  Returns false by
   * default. */
  public boolean isConnected() { return false; }

  /** Called with the remote protocol when a handshake has been completed.
   * After this has been called and while a connection is maintained, {@link
   * #isConnected()} should return true and #getRemote() should return this
   * protocol.  Does nothing by default. */
  public void setRemote(Protocol protocol) {}

  /** Returns the protocol passed to {@link #setRemote(Protocol)}.  Throws
   * IllegalStateException by default. */
  public Protocol getRemote() {
    throw new IllegalStateException("Not connected.");
  }

  public void close() throws IOException {}
}

