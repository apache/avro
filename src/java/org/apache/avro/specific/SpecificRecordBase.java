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

import org.apache.avro.Schema;

/** Base class for generated record classes. */
public abstract class SpecificRecordBase
  implements SpecificRecord, Comparable<SpecificRecord> {

  public abstract Schema getSchema();
  public abstract Object get(int field);
  public abstract void set(int field, Object value);

  public boolean equals(Object o) {
    return SpecificRecordBase.equals(this, o);
  }
    
  static boolean equals(SpecificRecord r1, Object o) {
    if (r1 == o) return true;
    if (!(o instanceof SpecificRecord)) return false;

    SpecificRecord r2 = (SpecificRecord)o;
    if (!r1.getSchema().equals(r2.getSchema())) return false;

    int end = r1.getSchema().getFields().size();
    for (int i = 0; i < end; i++) {
      Object v1 = r1.get(i);
      Object v2 = r2.get(i);
      if (v1 == null) {
        if (v2 != null) return false;
      } else {
        if (!v1.equals(v2)) return false;
      }
    }
    return true;
  }

  public int hashCode() {
    return SpecificRecordBase.hashCode(this);
  }

  static int hashCode(SpecificRecord r) {
    int result = 0;
    int end = r.getSchema().getFields().size();
    for (int i = 0; i < end; i++)
      result += r.get(i).hashCode();
    return result;
  }

  @SuppressWarnings(value="unchecked")
  public int compareTo(SpecificRecord that) {
    return SpecificData.get().compare(this, that, this.getSchema());
  }

}

