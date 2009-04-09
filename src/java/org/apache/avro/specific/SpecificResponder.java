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

import java.util.*;
import java.nio.ByteBuffer;
import java.io.*;

import org.apache.avro.*;
import org.apache.avro.io.*;
import org.apache.avro.util.*;
import org.apache.avro.ipc.*;
import org.apache.avro.reflect.ReflectResponder;
import org.apache.avro.Protocol.Message;

/** {@link Responder} for generated interfaces.*/
public class SpecificResponder extends ReflectResponder {
  public SpecificResponder(Class iface, Object impl) {
    super(iface, impl);
  }
    
  protected DatumWriter<Object> getDatumWriter(Schema schema) {
    return new SpecificDatumWriter(schema);
  }

  protected DatumReader<Object> getDatumReader(Schema schema) {
    return new SpecificDatumReader(schema, packageName);
  }

}
