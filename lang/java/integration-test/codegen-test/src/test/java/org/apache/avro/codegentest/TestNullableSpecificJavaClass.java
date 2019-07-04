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

package org.apache.avro.codegentest;

import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;

import java.util.Date;
import org.apache.avro.codegentest.testdata.NullableSpecificJavaClass;
import org.junit.Test;

public class TestNullableSpecificJavaClass extends AbstractSpecificRecordTest {

  @Test
  public void testSpecificRecordHashCode() {
    // hashCode shouldn't generate an exception wither the second field is null or
    // set to a Date.
    NullableSpecificJavaClass instanceOfGeneratedClass = NullableSpecificJavaClass.newBuilder()
        .setDate(new Date(-12801286800000L)).setNullableDate(null).build();
    assertThat(instanceOfGeneratedClass.hashCode(), not(0L));
    instanceOfGeneratedClass.setNullableDate(new Date(-11161414800000L));
    assertThat(instanceOfGeneratedClass.hashCode(), not(0L));
    instanceOfGeneratedClass.setDate(null);
    assertThat(instanceOfGeneratedClass.hashCode(), not(0L));
  }
}
