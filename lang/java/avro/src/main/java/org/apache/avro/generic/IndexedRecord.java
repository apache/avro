/*
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

/** A record implementation that permits field access by integer index.*/
public interface IndexedRecord extends GenericContainer {
  /** Set the value of a field given its position in the schema.
   * <p>This method is not meant to be called by user code, but only by {@link
   * org.apache.avro.io.DatumReader} implementations. */
  void put(int i, Object v);
  /** Return the value of a field given its position in the schema.
   * <p>This method is not meant to be called by user code, but only by {@link
   * org.apache.avro.io.DatumWriter} implementations. */
  Object get(int i);
}
