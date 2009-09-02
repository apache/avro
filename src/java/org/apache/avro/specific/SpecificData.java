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

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

/** Utilities for generated Java classes and interfaces. */
public class SpecificData extends ReflectData {

  private static final SpecificData INSTANCE = new SpecificData();

  protected SpecificData() {}
  
  /** Return the singleton instance. */
  public static SpecificData get() { return INSTANCE; }

  @Override
  protected boolean isRecord(Object datum) {
    return datum instanceof SpecificRecord;
  }

  @Override
  protected Schema getRecordSchema(Object record) {
    return ((SpecificRecord)record).getSchema();
  }

  @Override
  public int compare(Object o1, Object o2, Schema s) {
    switch (s.getType()) {
    case RECORD:
      SpecificRecord r1 = (SpecificRecord)o1;
      SpecificRecord r2 = (SpecificRecord)o2;
      int i = 0;
      for (Map.Entry<String, Schema> e : s.getFieldSchemas()) {
        int compare = compare(r1.get(i), r2.get(i), e.getValue());
        if (compare != 0) return compare;
        i++;
      }
      return 0;
    case ENUM:
      return ((Enum)o1).ordinal() - ((Enum)o2).ordinal();
    default:
      return super.compare(o1, o2, s);
    }
  }


}

