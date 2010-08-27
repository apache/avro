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
package org.apache.avro.ipc.trace;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class TestTracingUtil {
  @SuppressWarnings("unchecked")
  @Test
  public void testListSampling() {
    List<Long> in1 = (List<Long>) 
      Arrays.asList(new Long[] {0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L});
    List<Long> out1 = Util.sampledList(in1, 4);
    assertTrue(Arrays.equals(out1.toArray(), new Long[] {0L, 3L, 6L }));
    
    List<Long> in2 = new ArrayList<Long>(50000);
    for (int i = 0; i < 50000; i++) {
      in2.add((long)i);
    }
    
    List<Long> out2 = Util.sampledList(in2, 1000);
    assertEquals(1000, out2.size());
  }
}
