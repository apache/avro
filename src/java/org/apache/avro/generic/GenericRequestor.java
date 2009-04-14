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
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.*;
import org.apache.avro.io.*;
import org.apache.avro.util.*;
import org.apache.avro.ipc.*;

/** {@link Requestor} implementation for generic Java data. */
public class GenericRequestor extends Requestor {
  private static final Logger LOG
    = LoggerFactory.getLogger(GenericRequestor.class);

  public GenericRequestor(Protocol protocol, Transceiver transceiver)
    throws IOException {
    super(protocol, transceiver);
  }

  public void writeRequest(Schema schema, Object request, ValueWriter out)
    throws IOException {
    new GenericDatumWriter<Object>(schema).write(request, out);
  }

  public Object readResponse(Schema schema, ValueReader in) throws IOException {
    return new GenericDatumReader<Object>(schema).read(null, in);
  }

  public AvroRemoteException readError(Schema schema, ValueReader in)
    throws IOException {
    return new AvroRemoteException(new GenericDatumReader<Object>(schema).read(null,in));
  }

}
