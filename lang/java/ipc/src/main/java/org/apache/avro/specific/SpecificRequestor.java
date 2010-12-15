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

package org.apache.avro.specific;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.InvocationHandler;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.Requestor;

/** {@link org.apache.avro.ipc.Requestor Requestor} for generated interfaces. */
public class SpecificRequestor extends Requestor implements InvocationHandler {
  
  public SpecificRequestor(Class<?> iface, Transceiver transceiver)
    throws IOException {
    this(SpecificData.get().getProtocol(iface), transceiver);
  }

  protected SpecificRequestor(Protocol protocol, Transceiver transceiver)
    throws IOException {
    super(protocol, transceiver);
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args)
    throws Throwable {
    return request(method.getName(), args);
  }

  protected DatumWriter<Object> getDatumWriter(Schema schema) {
    return new SpecificDatumWriter<Object>(schema);
  }

  protected DatumReader<Object> getDatumReader(Schema schema) {
    return new SpecificDatumReader<Object>(schema);
  }

  @Override
  public void writeRequest(Schema schema, Object request, Encoder out)
    throws IOException {
    Object[] args = (Object[])request;
    int i = 0;
    for (Schema.Field param : schema.getFields())
      getDatumWriter(param.schema()).write(args[i++], out);
  }
    
  @Override
  public Object readResponse(Schema schema, Decoder in) throws IOException {
    return getDatumReader(schema).read(null, in);
  }

  @Override
  public Exception readError(Schema schema, Decoder in)
    throws IOException {
    Object value = getDatumReader(schema).read(null, in);
    if (value instanceof Exception)
      return (Exception)value;
    return new AvroRuntimeException(value.toString());
  }

  /** Create a proxy instance whose methods invoke RPCs. */
  public static  <T> T getClient(Class<T> iface, Transceiver transciever)
    throws IOException {
    return getClient(iface, transciever, SpecificData.get());
  }

  /** Create a proxy instance whose methods invoke RPCs. */
  @SuppressWarnings("unchecked")
  public static  <T> T getClient(Class<T> iface, Transceiver transciever,
                                 SpecificData specificData)
    throws IOException {
    Protocol protocol = specificData.getProtocol(iface);
    return (T)Proxy.newProxyInstance(iface.getClassLoader(),
                                  new Class[] { iface },
                                  new SpecificRequestor(protocol, transciever));
  }

  /** Create a proxy instance whose methods invoke RPCs. */
  @SuppressWarnings("unchecked")
  public static <T> T getClient(Class<T> iface, SpecificRequestor requestor)
    throws IOException {
    return (T)Proxy.newProxyInstance(iface.getClassLoader(),
                                  new Class[] { iface }, requestor);
  }

  /** Return the remote protocol for a proxy. */
  public static Protocol getRemote(Object proxy) throws IOException {
    return ((Requestor)Proxy.getInvocationHandler(proxy)).getRemote();
    
  }

}

