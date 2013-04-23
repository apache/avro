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
import org.apache.avro.generic.GenericRecord;

/** Base class for generated record classes. */
public abstract class SpecificRecordBase
  implements SpecificRecord, Comparable<SpecificRecord>, GenericRecord {

  public abstract Schema getSchema();
  public abstract Object get(int field);
  public abstract void put(int field, Object value);

  @Override
  public void put(String fieldName, Object value) {
    put(getSchema().getField(fieldName).pos(), value);
  }

  @Override
  public Object get(String fieldName) {
    return get(getSchema().getField(fieldName).pos());
  }

  @Override
  public boolean equals(Object that) {
    if (that == this) return true;                        // identical object
    if (!(that instanceof SpecificRecord)) return false;  // not a record
    if (this.getClass() != that.getClass()) return false; // not same schema
    return SpecificData.get().compare(this, that, this.getSchema(), true) == 0;
  }
    
  @Override
  public int hashCode() {
    return SpecificData.get().hashCode(this, this.getSchema());
  }

  @Override
  public int compareTo(SpecificRecord that) {
    return SpecificData.get().compare(this, that, this.getSchema());
  }

  @Override
  public String toString() {
    return SpecificData.get().toString(this);
  }

}

