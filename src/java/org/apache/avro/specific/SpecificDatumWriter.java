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

import java.io.*;
import java.util.*;

import org.apache.avro.*;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectDatumWriter;

/** {@link DatumWriter} for generated Java classes. */
public class SpecificDatumWriter extends ReflectDatumWriter {
  public SpecificDatumWriter() {}

  public SpecificDatumWriter(Schema root) {
    super(root);
  }
  
  protected void writeRecord(Schema schema, Object datum, ValueWriter out)
    throws IOException {
    SpecificRecord record = (SpecificRecord)datum;
    int i = 0;
    for (Map.Entry<String,Schema> entry : schema.getFields().entrySet())
      write(entry.getValue(), record.get(i++), out);
  }
}

