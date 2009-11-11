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
import java.lang.reflect.Proxy;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificRequestor;
import org.apache.avro.ipc.Transceiver;

/** A {@link org.apache.avro.ipc.Requestor} for existing interfaces. */
public class ReflectRequestor extends SpecificRequestor {
  
  public ReflectRequestor(Class<?> iface, Transceiver transceiver)
    throws IOException {
    this(ReflectData.get().getProtocol(iface), transceiver);
  }

  protected ReflectRequestor(Protocol protocol, Transceiver transceiver)
    throws IOException {
    super(protocol, transceiver);
  }
    
  @Override
  protected DatumWriter<Object> getDatumWriter(Schema schema) {
    return new ReflectDatumWriter(schema);
  }

  @Override
  protected DatumReader<Object> getDatumReader(Schema schema) {
    return new ReflectDatumReader(schema);
  }

  /** Create a proxy instance whose methods invoke RPCs. */
  public static Object getClient(Class<?> iface, Transceiver transciever)
    throws IOException {
    return getClient(iface, transciever, ReflectData.get());
  }

  /** Create a proxy instance whose methods invoke RPCs. */
  public static Object getClient(Class<?> iface, Transceiver transciever, ReflectData reflectData)
    throws IOException {
    Protocol protocol = reflectData.getProtocol(iface);
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

