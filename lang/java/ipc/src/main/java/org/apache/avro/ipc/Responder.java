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

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.avro.util.ByteBufferOutputStream;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Function;

/** Base class for the server side of a protocol interaction. */
public abstract class Responder {
  private static final Logger LOG = LoggerFactory.getLogger(Responder.class);

  private static final Schema META =
      Schema.createMap(Schema.create(Schema.Type.BYTES));
  private static final GenericDatumReader<Map<String, ByteBuffer>>
      META_READER = new GenericDatumReader<Map<String, ByteBuffer>>(META);
  private static final GenericDatumWriter<Map<String, ByteBuffer>>
      META_WRITER = new GenericDatumWriter<Map<String, ByteBuffer>>(META);

  private static final ThreadLocal<Protocol> REMOTE =
      new ThreadLocal<Protocol>();

  private final Map<MD5, Protocol> protocols
      = new ConcurrentHashMap<MD5, Protocol>();

  private final Protocol local;
  private final MD5 localHash;
  protected final List<RPCPlugin> rpcMetaPlugins;

  protected Responder(Protocol local) {
    this.local = local;
    this.localHash = new MD5();
    localHash.bytes(local.getMD5());
    protocols.put(localHash, local);
    this.rpcMetaPlugins =
        new CopyOnWriteArrayList<RPCPlugin>();
  }

  /** Return the remote protocol.  Accesses a {@link ThreadLocal} that's set
   * around calls to {@link #respond(Protocol.Message, Object)}. */
  public static Protocol getRemote() { return REMOTE.get(); }

  /** Return the local protocol. */
  public Protocol getLocal() { return local; }

  /**
   * Adds a new plugin to manipulate per-call metadata.  Plugins
   * are executed in the order that they are added.
   * @param plugin a plugin that will manipulate RPC metadata
   */
  public void addRPCPlugin(RPCPlugin plugin) {
    rpcMetaPlugins.add(plugin);
  }

  /** Called by a server to deserialize a request, compute and serialize
   * a response or error. */
  public List<ByteBuffer> respond(List<ByteBuffer> buffers) throws IOException {
    return respond(buffers, null);
  }

  /** Called by a server to deserialize a request, compute and serialize a
   * response or error.  Transciever is used by connection-based servers to
   * track handshake status of connection. */
  public List<ByteBuffer> respond(List<ByteBuffer> buffers, Transceiver connection) throws IOException {
    try {
      return respondAsync(buffers, connection).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return handleException(e, new RPCContext(), null);
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof IOException)
        throw (IOException) cause;
      throw new IOException(cause);
    }
  }

  public CompletableFuture<List<ByteBuffer>> respondAsync(List<ByteBuffer> buffers,
                                                          Transceiver connection) {
    Decoder in = DecoderFactory.get().binaryDecoder(
        new ByteBufferInputStream(buffers), null);
    ByteBufferOutputStream bbo = new ByteBufferOutputStream();
    BinaryEncoder out = EncoderFactory.get().binaryEncoder(bbo, null);
    final RPCContext context = new RPCContext();
    final boolean wasConnected = connection != null && connection.isConnected();

    final Message m;
    final Object request;
    final List<ByteBuffer> handshake;
    try {
      Protocol remote = handshake(in, out, connection);
      out.flush();
      if (remote == null) {                     // handshake failed
        return CompletableFuture.completedFuture(bbo.getBufferList());
      }
      handshake = bbo.getBufferList();

      // read request using remote protocol specification
      context.setRequestCallMeta(META_READER.read(null, in));
      String messageName = in.readString(null).toString();
      if (messageName.equals(""))            // a handshake ping
        return CompletableFuture.completedFuture(handshake);

      Message rm = remote.getMessages().get(messageName);
      if (rm == null)
        return CompletableFuture.completedFuture(compileResult(handshake, handleException(new AvroRuntimeException("No such remote message: " + messageName), context, null), context));

      m = getLocal().getMessages().get(messageName);
      if (m == null)
        return CompletableFuture.completedFuture(compileResult(handshake, handleException(new AvroRuntimeException("No message named " + messageName + " in " + getLocal()), context, null), context));

      request = readRequest(rm.getRequest(), m.getRequest(), in);
      context.setMessage(rm);
      for (RPCPlugin plugin : rpcMetaPlugins) {
        plugin.serverReceiveRequest(context);
      }
      // create response using local protocol specification
      if ((m.isOneWay() != rm.isOneWay()) && wasConnected) {
        return CompletableFuture.completedFuture(compileResult(handshake, handleException(new AvroRuntimeException("Not both one-way: " + messageName), context, m), context));
      }

      REMOTE.set(remote);
    } catch (IOException e) {
      final CompletableFuture<List<ByteBuffer>> future = new CompletableFuture<List<ByteBuffer>>();
      future.completeExceptionally(e);
      return future;
    }

    return respondAsync(m, request)
        .thenApply(new Function<Object, List<ByteBuffer>>() {
          @Override
          public List<ByteBuffer> apply(Object response) {
            context.setResponse(response);
            if (m.isOneWay() && wasConnected)
              return null;
            ByteBufferOutputStream bbo = new ByteBufferOutputStream();
            BinaryEncoder out = EncoderFactory.get().binaryEncoder(bbo, null);
            try {
              out.writeBoolean(false);
              writeResponse(m.getResponse(), response, out);
              out.flush();
            } catch (IOException e) {
              Responder.<RuntimeException>throwException(e);
            }
            return bbo.getBufferList();
          }
        })
        .exceptionally(new Function<Throwable, List<ByteBuffer>>() {
          @Override
          public List<ByteBuffer> apply(Throwable throwable) {
            return futureExceptionally(throwable, context, m);
          }
        })
        .whenComplete(new BiConsumer<List<ByteBuffer>, Throwable>() {
          @Override
          public void accept(List<ByteBuffer> byteBufferList, Throwable throwable) {
            REMOTE.set(null);
            if (byteBufferList != null) {
              context.setResponsePayload(byteBufferList);
              for (RPCPlugin plugin : rpcMetaPlugins) {
                plugin.serverSendResponse(context);
              }
            }
          }
        })
        .thenApply(new Function<List<ByteBuffer>, List<ByteBuffer>>() {
          @Override
          public List<ByteBuffer> apply(List<ByteBuffer> byteBufferList) {
            try {
              return compileResult(handshake, byteBufferList, context);
            } catch (IOException e) {
              Responder.<RuntimeException>throwException(e);
              return null;
            }
          }
        });
  }

  private static List<ByteBuffer> compileResult(List<ByteBuffer> handshake, List<ByteBuffer> payload, RPCContext context) throws IOException {
    if (payload == null)
      return null;
    ByteBufferOutputStream bbo = new ByteBufferOutputStream();
    BinaryEncoder out = EncoderFactory.get().binaryEncoder(bbo, null);
    META_WRITER.write(context.responseCallMeta(), out);
    out.flush();
    bbo.prepend(handshake == null ? new ByteBufferOutputStream().getBufferList() : handshake);
    bbo.append(payload);
    return bbo.getBufferList();
  }


  private List<ByteBuffer> handleException(Exception exception, RPCContext context, Message m) throws IOException {
    ByteBufferOutputStream bbo = new ByteBufferOutputStream();
    BinaryEncoder out = EncoderFactory.get().binaryEncoder(bbo, null);
    out.writeBoolean(true);
    try {
      context.setError(exception);
      if (m != null)
        writeError(m.getErrors(), exception, out);
      else
        writeError(Protocol.SYSTEM_ERRORS, new Utf8(exception.toString()), out);
      out.flush();
      return bbo.getBufferList();
    } catch (Exception e) {
      LOG.warn("system error", e);
      context.setError(e);
      writeError(Protocol.SYSTEM_ERRORS, new Utf8(e.toString()), out);
      out.flush();
      return bbo.getBufferList();
    }
  }

  private List<ByteBuffer> futureExceptionally(Throwable throwable, RPCContext context, Message m) {
    try {
      LOG.warn("user error", throwable.getCause());
      return handleException((Exception) throwable.getCause(), context, m);
    } catch (IOException e) {
      Responder.<RuntimeException>throwException(e);
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> void throwException(Throwable exception) throws T {
    throw (T) exception;
  }

  private SpecificDatumWriter<HandshakeResponse> handshakeWriter =
      new SpecificDatumWriter<HandshakeResponse>(HandshakeResponse.class);
  private SpecificDatumReader<HandshakeRequest> handshakeReader =
      new SpecificDatumReader<HandshakeRequest>(HandshakeRequest.class);

  private Protocol handshake(Decoder in, Encoder out, Transceiver connection)
      throws IOException {
    if (connection != null && connection.isConnected())
      return connection.getRemote();
    HandshakeRequest request = (HandshakeRequest) handshakeReader.read(null, in);
    Protocol remote = protocols.get(request.clientHash);
    if (remote == null && request.clientProtocol != null) {
      remote = Protocol.parse(request.clientProtocol.toString());
      protocols.put(request.clientHash, remote);
    }
    HandshakeResponse response = new HandshakeResponse();
    if (localHash.equals(request.serverHash)) {
      response.match =
          remote == null ? HandshakeMatch.NONE : HandshakeMatch.BOTH;
    } else {
      response.match =
          remote == null ? HandshakeMatch.NONE : HandshakeMatch.CLIENT;
    }
    if (response.match != HandshakeMatch.BOTH) {
      response.serverProtocol = local.toString();
      response.serverHash = localHash;
    }

    RPCContext context = new RPCContext();
    context.setHandshakeRequest(request);
    context.setHandshakeResponse(response);
    for (RPCPlugin plugin : rpcMetaPlugins) {
      plugin.serverConnecting(context);
    }
    handshakeWriter.write(response, out);

    if (connection != null && response.match != HandshakeMatch.NONE)
      connection.setRemote(remote);

    return remote;
  }

  public CompletableFuture<Object> respondAsync(Message message, Object request) {
    CompletableFuture<Object> completableFuture = new CompletableFuture<Object>();
    try {
      completableFuture.complete(respond(message, request));
    } catch (Exception e) {
      completableFuture.completeExceptionally(e);
    }
    return completableFuture;
  }

  /** Reads a request message. */
  public abstract Object respond(Message message, Object request) throws Exception;

  /**
   * Reads a request message.
   */
  public abstract Object readRequest(Schema actual, Schema expected, Decoder in)
      throws IOException;

  /** Writes a response message. */
  public abstract void writeResponse(Schema schema, Object response,
                                     Encoder out) throws IOException;

  /** Writes an error message. */
  public abstract void writeError(Schema schema, Object error,
                                  Encoder out) throws IOException;

}

