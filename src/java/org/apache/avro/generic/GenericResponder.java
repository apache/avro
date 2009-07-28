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

package org.apache.avro.generic;

import java.io.IOException;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.ipc.Responder;

/** {@link Responder} implementation for generic Java data. */
public abstract class GenericResponder extends Responder {

  public GenericResponder(Protocol local) {
    super(local);
  }

  /** Reads a request message. */
  public Object readRequest(Schema schema, Decoder in) throws IOException {
    return new GenericDatumReader<Object>(schema).read(null, in);
  }

  /** Writes a response message. */
  public void writeResponse(Schema schema, Object response, Encoder out)
    throws IOException {
    new GenericDatumWriter<Object>(schema).write(response, out);
  }

  /** Writes an error message. */
  public void writeError(Schema schema, AvroRemoteException error,
                         Encoder out) throws IOException {
    new GenericDatumWriter<Object>(schema).write(error.getValue(), out);
  }

}

