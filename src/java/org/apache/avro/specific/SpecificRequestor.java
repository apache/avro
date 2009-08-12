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
import java.lang.reflect.Proxy;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectRequestor;

/** {@link org.apache.avro.ipc.Requestor Requestor} for generated interfaces. */
public class SpecificRequestor extends ReflectRequestor {
  
  public SpecificRequestor(Class<?> iface, Transceiver transceiver)
    throws IOException {
    this(ReflectData.getProtocol(iface), transceiver);
  }
  
  private SpecificRequestor(Protocol protocol, Transceiver transceiver)
    throws IOException {
    super(protocol, transceiver);
  }
    
  protected DatumWriter<Object> getDatumWriter(Schema schema) {
    return new SpecificDatumWriter(schema);
  }

  protected DatumReader<Object> getDatumReader(Schema schema) {
    return new SpecificDatumReader(schema, packageName);
  }

  /** Create a proxy instance whose methods invoke RPCs. */
  public static Object getClient(Class<?> iface, Transceiver transciever)
    throws IOException {
    Protocol protocol = ReflectData.getProtocol(iface);
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

