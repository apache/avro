/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.reflect;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

import java.util.Collections;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.junit.Test;

public class TestReflectData {
  @Test
  @SuppressWarnings("unchecked")
  public void testWeakSchemaCaching() throws Exception {
    int numSchemas = 1000000;
    for (int i = 0; i < numSchemas; i++) {
      // Create schema
      Schema schema = Schema.createRecord("schema", null, null, false);
      schema.setFields(Collections.emptyList());

      ReflectData.get().getRecordState(new Object(), schema);
    }

    // Reflect the number of schemas currently in the cache
    ReflectData.ClassAccessorData classData = ReflectData.ACCESSOR_CACHE.get(Object.class);

    System.gc(); // Not guaranteed, but seems to be reliable enough

    assertThat("ReflectData cache should release references", classData.bySchema.size(), lessThan(numSchemas));
  }

  @Test
  public void testGenericProtocol() {
    Protocol protocol = ReflectData.get().getProtocol(FooBarProtocol.class);
    Schema recordSchema = ReflectData.get().getSchema(FooBarReflectiveRecord.class);

    assertThat(protocol.getTypes(), contains(recordSchema));

    assertThat(protocol.getMessages().keySet(), containsInAnyOrder("store", "findById", "exists"));

    Schema.Field storeArgument = protocol.getMessages().get("store").getRequest().getFields().get(0);
    assertThat(storeArgument.schema(), equalTo(recordSchema));

    Schema.Field findByIdArgument = protocol.getMessages().get("findById").getRequest().getFields().get(0);
    assertThat(findByIdArgument.schema(), equalTo(Schema.create(Schema.Type.STRING)));

    Schema findByIdResponse = protocol.getMessages().get("findById").getResponse();
    assertThat(findByIdResponse, equalTo(recordSchema));

    Schema.Field existsArgument = protocol.getMessages().get("exists").getRequest().getFields().get(0);
    assertThat(existsArgument.schema(), equalTo(Schema.create(Schema.Type.STRING)));
  }

  private interface CrudProtocol<R, I> extends OtherProtocol<I> {
    void store(R record);

    R findById(I id);
  }

  private interface OtherProtocol<G> {
    boolean exists(G id);
  }

  private interface FooBarProtocol extends OtherProtocol<String>, CrudProtocol<FooBarReflectiveRecord, String> {
  }

  private static class FooBarReflectiveRecord {
    private String bar;
    private int baz;
  }
}
