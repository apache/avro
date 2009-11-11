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
import java.util.Map;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.ipc.AvroRemoteException;
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
    return new SpecificDatumWriter(schema);
  }

  protected DatumReader<Object> getDatumReader(Schema schema) {
    return new SpecificDatumReader(schema);
  }

  @Override
  public void writeRequest(Schema schema, Object request, Encoder out)
    throws IOException {
    Object[] args = (Object[])request;
    int i = 0;
    for (Map.Entry<String, Schema> param : schema.getFieldSchemas())
      getDatumWriter(param.getValue()).write(args[i++], out);
  }
    
  @Override
  public Object readResponse(Schema schema, Decoder in) throws IOException {
    return getDatumReader(schema).read(null, in);
  }

  @Override
  public AvroRemoteException readError(Schema schema, Decoder in)
    throws IOException {
    return (AvroRemoteException)getDatumReader(schema).read(null, in);
  }

  /** Create a proxy instance whose methods invoke RPCs. */
  public static Object getClient(Class<?> iface, Transceiver transciever)
    throws IOException {
    return getClient(iface, transciever, SpecificData.get());
  }

  /** Create a proxy instance whose methods invoke RPCs. */
  public static Object getClient(Class<?> iface, Transceiver transciever,
                                 SpecificData specificData)
    throws IOException {
    Protocol protocol = specificData.getProtocol(iface);
    return Proxy.newProxyInstance(iface.getClassLoader(),
                                  new Class[] { iface },
                                  new SpecificRequestor(protocol, transciever));
  }
  
  /** Create a proxy instance whose methods invoke RPCs. */
  public static Object getClient(Class<?> iface, SpecificRequestor requestor)
    throws IOException {
    return Proxy.newProxyInstance(iface.getClassLoader(),
                                  new Class[] { iface }, requestor);
  }
}

