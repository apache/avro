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

package org.apache.avro.reflect;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.Requestor;
import org.apache.avro.ipc.Transceiver;

/** A {@link Requestor} for existing interfaces via Java reflection. */
public class ReflectRequestor extends Requestor implements InvocationHandler {
  protected String packageName;
  
  public ReflectRequestor(Class<?> iface, Transceiver transceiver)
    throws IOException {
    this(ReflectData.getProtocol(iface), transceiver);
  }

  protected ReflectRequestor(Protocol protocol, Transceiver transceiver)
    throws IOException {
    super(protocol, transceiver);
    this.packageName = protocol.getNamespace()+"."+protocol.getName()+"$";
  }

  public Object invoke(Object proxy, Method method, Object[] args)
    throws Throwable {
    return request(method.getName(), args);
  }
    
  protected DatumWriter<Object> getDatumWriter(Schema schema) {
    return new ReflectDatumWriter(schema);
  }

  protected DatumReader<Object> getDatumReader(Schema schema) {
    return new ReflectDatumReader(schema, packageName);
  }

  public void writeRequest(Schema schema, Object request, Encoder out)
    throws IOException {
    Object[] args = (Object[])request;
    int i = 0;
    for (Map.Entry<String, Schema> param : schema.getFieldSchemas())
      getDatumWriter(param.getValue()).write(args[i++], out);
  }
    
  public Object readResponse(Schema schema, Decoder in) throws IOException {
    return getDatumReader(schema).read(null, in);
  }

  public AvroRemoteException readError(Schema schema, Decoder in)
    throws IOException {
    return (AvroRemoteException)getDatumReader(schema).read(null, in);
  }

  /** Create a proxy instance whose methods invoke RPCs. */
  public static Object getClient(Class<?> iface, Transceiver transciever)
    throws IOException {
    Protocol protocol = ReflectData.getProtocol(iface);
    return Proxy.newProxyInstance(iface.getClassLoader(),
                                  new Class[] { iface },
                                  new ReflectRequestor(protocol, transciever));
  }
  
  /** Create a proxy instance whose methods invoke RPCs. */
  public static Object getClient(Class<?> iface, ReflectRequestor rreq)
    throws IOException {
    return Proxy.newProxyInstance(iface.getClassLoader(),
                                  new Class[] { iface }, rreq);
  }
}

