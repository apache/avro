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

import org.apache.avro.Schema;
import org.apache.avro.Protocol;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificResponder;
import org.apache.avro.util.Utf8;

/** {@link org.apache.avro.ipc.Responder} for existing interfaces.*/
public class ReflectResponder extends SpecificResponder {
  public ReflectResponder(Class iface, Object impl) {
    super(ReflectData.get().getProtocol(iface), impl, ReflectData.get());
  }
  
  public ReflectResponder(Protocol protocol, Object impl) {
    super(protocol, impl, ReflectData.get());
  }

  @Override
  protected DatumWriter<Object> getDatumWriter(Schema schema) {
    return new ReflectDatumWriter<Object>(schema);
  }

  @Override
  protected DatumReader<Object> getDatumReader(Schema schema) {
    return new ReflectDatumReader<Object>(schema);
  }

  @Override
  public void writeError(Schema schema, Object error,
                         Encoder out) throws IOException {
    if (error instanceof Utf8)
      error = error.toString();                   // system error: convert
    super.writeError(schema, error, out);
  }



}
