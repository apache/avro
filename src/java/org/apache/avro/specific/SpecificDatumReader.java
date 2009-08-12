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
import org.apache.avro.reflect.ReflectDatumReader;

/** {@link org.apache.avro.io.DatumReader DatumReader} for generated Java classes. */
public class SpecificDatumReader extends ReflectDatumReader {
  public SpecificDatumReader(String packageName) {
    super(packageName);
  }

  public SpecificDatumReader(Schema root, String packageName) {
    super(root, packageName);
  }

  public SpecificDatumReader(Schema root) {
    super(root, root.getNamespace()+".");
  }

  protected void addField(Object record, String name, int position, Object o) {
    ((SpecificRecord)record).set(position, o);
  }
  protected Object getField(Object record, String name, int position) {
    return ((SpecificRecord)record).get(position);
  }
  protected void removeField(Object record, String field, int position) {
    ((SpecificRecord)record).set(position, null);
  }

}

