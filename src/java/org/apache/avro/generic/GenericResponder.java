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

import java.util.*;
import java.nio.ByteBuffer;
import java.io.*;

import org.apache.avro.*;
import org.apache.avro.io.*;
import org.apache.avro.ipc.*;

/** {@link Responder} implementation for generic Java data. */
public abstract class GenericResponder extends Responder {

  public GenericResponder(Protocol local) {
    super(local);
  }

  /** Reads a request message. */
  public Object readRequest(Schema schema, ValueReader in) throws IOException {
    return new GenericDatumReader(schema).read(null, in);
  }

  /** Writes a response message. */
  public void writeResponse(Schema schema, Object response, ValueWriter out)
    throws IOException {
    new GenericDatumWriter(schema).write(response, out);
  }

  /** Writes an error message. */
  public void writeError(Schema schema, AvroRemoteException error,
                         ValueWriter out) throws IOException {
    new GenericDatumWriter(schema).write(error.getValue(), out);
  }

}
