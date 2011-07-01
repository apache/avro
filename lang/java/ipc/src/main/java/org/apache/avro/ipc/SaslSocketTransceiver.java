/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.avro.ipc;

import java.io.IOException;
import java.io.EOFException;
import java.io.UnsupportedEncodingException;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslServer;

import org.apache.avro.Protocol;
import org.apache.avro.util.ByteBufferOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link Transceiver} that uses {@link javax.security.sasl} for
 * authentication and encryption. */
public class SaslSocketTransceiver extends Transceiver {
  private static final Logger LOG =
    LoggerFactory.getLogger(SaslSocketTransceiver.class);

  private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

  private static enum Status { START, CONTINUE, FAIL, COMPLETE }

  private SaslParticipant sasl;
  private SocketChannel channel;
  private boolean dataIsWrapped;
  private boolean saslResponsePiggybacked;

  private Protocol remote;
  
  private ByteBuffer readHeader = ByteBuffer.allocate(4);
  private ByteBuffer writeHeader = ByteBuffer.allocate(4);
  private ByteBuffer zeroHeader = ByteBuffer.allocate(4).putInt(0);

  /** Create using SASL's anonymous (<a
   * href="http://www.ietf.org/rfc/rfc2245.txt">RFC 2245) mechanism. */
  public SaslSocketTransceiver(SocketAddress address) throws IOException {
    this(address, new AnonymousClient());
  }

  /** Create using the specified {@link SaslClient}. */
  public SaslSocketTransceiver(SocketAddress address, SaslClient saslClient)
    throws IOException {
    this.sasl = new SaslParticipant(saslClient);
    this.channel = SocketChannel.open(address);
    this.channel.socket().setTcpNoDelay(true);
    LOG.debug("open to {}", getRemoteName());
    open(true);
  }

  /** Create using the specified {@link SaslServer}. */
  public SaslSocketTransceiver(SocketChannel channel, SaslServer saslServer)
    throws IOException {
    this.sasl = new SaslParticipant(saslServer);
    this.channel = channel;
    LOG.debug("open from {}", getRemoteName());
    open(false);
  }

  @Override public boolean isConnected() { return remote != null; }

  @Override public void setRemote(Protocol remote) {
    this.remote = remote;
  }

  @Override public Protocol getRemote() {
    return remote;
  }
  @Override public String getRemoteName() {
    return channel.socket().getRemoteSocketAddress().toString();
  }

  @Override
  public synchronized List<ByteBuffer> transceive(List<ByteBuffer> request)
    throws IOException {
    if (saslResponsePiggybacked) {                // still need to read response
      saslResponsePiggybacked = false;
      Status status  = readStatus();
      ByteBuffer frame = readFrame();
      switch (status) {
      case COMPLETE:
        break;
      case FAIL:
        throw new SaslException("Fail: "+toString(frame));
      default:
        throw new IOException("Unexpected SASL status: "+status);
      }
    }
    return super.transceive(request);
  }

  private void open(boolean isClient) throws IOException {
    LOG.debug("beginning SASL negotiation");

    if (isClient) {
      ByteBuffer response = EMPTY;
      if (sasl.client.hasInitialResponse())
        response = ByteBuffer.wrap(sasl.evaluate(response.array()));
      write(Status.START, sasl.getMechanismName(), response);
      if (sasl.isComplete())
        saslResponsePiggybacked = true;
    }
    
    while (!sasl.isComplete()) {
      Status status  = readStatus();
      ByteBuffer frame = readFrame();
      switch (status) {
      case START:
        String mechanism = toString(frame);
        frame = readFrame();
        if (!mechanism.equalsIgnoreCase(sasl.getMechanismName())) {
          write(Status.FAIL, "Wrong mechanism: "+mechanism);
          throw new SaslException("Wrong mechanism: "+mechanism);
        }
      case CONTINUE: 
        byte[] response;
        try {
          response = sasl.evaluate(frame.array());
          status = sasl.isComplete() ? Status.COMPLETE : Status.CONTINUE;
        } catch (SaslException e) {
          response = e.toString().getBytes("UTF-8");
          status = Status.FAIL;
        }
        write(status, response!=null ? ByteBuffer.wrap(response) : EMPTY);
        break;
      case COMPLETE:
        sasl.evaluate(frame.array());
        if (!sasl.isComplete())
          throw new SaslException("Expected completion!");
        break;
      case FAIL:
        throw new SaslException("Fail: "+toString(frame));
      default:
        throw new IOException("Unexpected SASL status: "+status);
      }
    }
    LOG.debug("SASL opened");

    String qop = (String) sasl.getNegotiatedProperty(Sasl.QOP);
    LOG.debug("QOP = {}", qop);
    dataIsWrapped = (qop != null && !qop.equalsIgnoreCase("auth"));
  }

  private String toString(ByteBuffer buffer) throws IOException {
    try {
      return new String(buffer.array(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IOException(e.toString(), e);
    }
  }

  @Override public synchronized List<ByteBuffer> readBuffers()
    throws IOException {
    List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
    while (true) {
      ByteBuffer buffer = readFrameAndUnwrap();
      if (buffer.remaining() == 0)
        return buffers;
      buffers.add(buffer);
    }
  }

  private Status readStatus() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(1);
    read(buffer);
    int status = buffer.get();
    if (status > Status.values().length)
      throw new IOException("Unexpected SASL status byte: "+status);
    return Status.values()[status];
  }

  private ByteBuffer readFrameAndUnwrap() throws IOException {
    ByteBuffer frame = readFrame();
    if (!dataIsWrapped)
      return frame;
    ByteBuffer unwrapped = ByteBuffer.wrap(sasl.unwrap(frame.array()));
    LOG.debug("unwrapped data of length: {}", unwrapped.remaining());
    return unwrapped;
  }

  private ByteBuffer readFrame() throws IOException {
    read(readHeader);
    ByteBuffer buffer = ByteBuffer.allocate(readHeader.getInt());
    LOG.debug("about to read: {} bytes", buffer.capacity());
    read(buffer);
    return buffer;
  }
  
  private void read(ByteBuffer buffer) throws IOException {
    buffer.clear();
    while (buffer.hasRemaining())
      if (channel.read(buffer) == -1)
        throw new EOFException();
    buffer.flip();
  }

  @Override public synchronized void writeBuffers(List<ByteBuffer> buffers)
    throws IOException {
    if (buffers == null) return;                  // no data to write
    List<ByteBuffer> writes = new ArrayList<ByteBuffer>(buffers.size()*2+1);
    int currentLength = 0;
    ByteBuffer currentHeader = writeHeader;
    for (ByteBuffer buffer : buffers) {           // gather writes
      if (buffer.remaining() == 0) continue;      // ignore empties
      if (dataIsWrapped) {
        LOG.debug("wrapping data of length: {}", buffer.remaining());
        buffer = ByteBuffer.wrap(sasl.wrap(buffer.array(), buffer.position(),
                                           buffer.remaining()));
      }
      int length = buffer.remaining();
      if (!dataIsWrapped                          // can append buffers on wire
          && (currentLength + length) <= ByteBufferOutputStream.BUFFER_SIZE) {
        if (currentLength == 0)
          writes.add(currentHeader);
        currentLength += length;
        currentHeader.clear();
        currentHeader.putInt(currentLength);
        LOG.debug("adding {} to write, total now {}", length, currentLength);
      } else {
        currentLength = length;
        currentHeader = ByteBuffer.allocate(4).putInt(length);
        writes.add(currentHeader);
        LOG.debug("planning write of {}", length);
      }
      currentHeader.flip();
      writes.add(buffer);
    }
    zeroHeader.flip();                            // zero-terminate
    writes.add(zeroHeader);

    channel.write(writes.toArray(new ByteBuffer[writes.size()]));
  }

  private void write(Status status, String prefix, ByteBuffer response)
    throws IOException {
    LOG.debug("write status: {} {}", status, prefix);
    write(status, prefix);
    write(response);
  }

  private void write(Status status, String response) throws IOException {
    write(status, ByteBuffer.wrap(response.getBytes("UTF-8")));
  }

  private void write(Status status, ByteBuffer response) throws IOException {
    LOG.debug("write status: {}", status);
    ByteBuffer statusBuffer = ByteBuffer.allocate(1);
    statusBuffer.clear();
    statusBuffer.put((byte)(status.ordinal())).flip();
    channel.write(statusBuffer);
    write(response);
  }

  private void write(ByteBuffer response) throws IOException {
    LOG.debug("writing: {}", response.remaining());
    writeHeader.clear();
    writeHeader.putInt(response.remaining()).flip();
    channel.write(new ByteBuffer[] { writeHeader, response });
  }

  @Override public void close() throws IOException {
    if (channel.isOpen()) {
      LOG.info("closing to "+getRemoteName());
      channel.close();
    }
    sasl.dispose();
  }

  /**
   * Used to abstract over the <code>SaslServer</code> and
   * <code>SaslClient</code> classes, which share a lot of their interface, but
   * unfortunately don't share a common superclass.
   */
  private static class SaslParticipant {
    // One of these will always be null.
    public SaslServer server;
    public SaslClient client;

    public SaslParticipant(SaslServer server) {
      this.server = server;
    }

    public SaslParticipant(SaslClient client) {
      this.client = client;
    }

    public String getMechanismName() {
      if (client != null)
        return client.getMechanismName();
      else
        return server.getMechanismName();
    }

    public boolean isComplete() {
      if (client != null)
        return client.isComplete();
      else
        return server.isComplete();
    }

    public void dispose() throws SaslException {
      if (client != null)
        client.dispose();
      else
        server.dispose();
    }

    public byte[] unwrap(byte[] buf) throws SaslException {
      if (client != null)
        return client.unwrap(buf, 0, buf.length);
      else
        return server.unwrap(buf, 0, buf.length);
    }

    public byte[] wrap(byte[] buf) throws SaslException {
      return wrap(buf, 0, buf.length);
    }

    public byte[] wrap(byte[] buf, int start, int len) throws SaslException {
      if (client != null)
        return client.wrap(buf, start, len);
      else
        return server.wrap(buf, start, len);
    }

    public Object getNegotiatedProperty(String propName) {
      if (client != null)
        return client.getNegotiatedProperty(propName);
      else
        return server.getNegotiatedProperty(propName);
    }

    public byte[] evaluate(byte[] buf) throws SaslException {
      if (client != null)
        return client.evaluateChallenge(buf);
      else
        return server.evaluateResponse(buf);
    }

  }

  private static class AnonymousClient implements SaslClient {
    public String getMechanismName() { return "ANONYMOUS"; }
    public boolean hasInitialResponse() { return true; }
    public byte[] evaluateChallenge(byte[] challenge) throws SaslException {
      try {
        return System.getProperty("user.name").getBytes("UTF-8");
      } catch (IOException e) {
        throw new SaslException(e.toString());
      }
    }
    public boolean isComplete() { return true; }
    public byte[] unwrap(byte[] incoming, int offset, int len) {
      throw new UnsupportedOperationException();
    }
    public byte[] wrap(byte[] outgoing, int offset, int len) {
      throw new UnsupportedOperationException();
    }
    public Object getNegotiatedProperty(String propName) { return null; }
    public void dispose() {}
  }
}
