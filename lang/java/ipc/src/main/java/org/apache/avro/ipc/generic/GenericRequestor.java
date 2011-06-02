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

package org.apache.avro.ipc.generic;

import java.io.IOException;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.ipc.Requestor;
import org.apache.avro.ipc.Transceiver;

/** {@link Requestor} implementation for generic Java data. */
public class GenericRequestor extends Requestor {
  public GenericRequestor(Protocol protocol, Transceiver transceiver)
    throws IOException {
    super(protocol, transceiver);
  }

  @Override
  public Object request(String messageName, Object request)
    throws IOException {
    try {
      return super.request(messageName, request);
    } catch (Exception e) {
      if (e instanceof RuntimeException)
        throw (RuntimeException)e;
      if (e instanceof IOException)
        throw (IOException)e;
      throw new AvroRemoteException(e);
    }
  }

  @Override
  public void writeRequest(Schema schema, Object request, Encoder out)
    throws IOException {
    new GenericDatumWriter<Object>(schema).write(request, out);
  }

  @Override
  public Object readResponse(Schema writer, Schema reader, Decoder in)
    throws IOException {
    return new GenericDatumReader<Object>(writer, reader).read(null, in);
  }

  @Override
  public Exception readError(Schema writer, Schema reader, Decoder in)
    throws IOException {
    Object error = new GenericDatumReader<Object>(writer, reader).read(null,in);
    if (error instanceof CharSequence)
      return new AvroRuntimeException(error.toString()); // system error
    return new AvroRemoteException(error);
  }

}

