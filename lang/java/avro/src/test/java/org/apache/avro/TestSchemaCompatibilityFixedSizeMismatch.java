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
package org.apache.avro;

import static org.apache.avro.TestSchemaCompatibility.validateIncompatibleSchemas;
import static org.apache.avro.TestSchemas.A_DINT_B_DFIXED_4_BYTES_RECORD1;
import static org.apache.avro.TestSchemas.A_DINT_B_DFIXED_8_BYTES_RECORD1;
import static org.apache.avro.TestSchemas.FIXED_4_BYTES;
import static org.apache.avro.TestSchemas.FIXED_8_BYTES;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.SchemaCompatibility.SchemaIncompatibilityType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestSchemaCompatibilityFixedSizeMismatch {

  @Parameters(name = "r: {0} | w: {1}")
  public static Iterable<Object[]> data() {
    Object[][] fields = { //
        { FIXED_4_BYTES, FIXED_8_BYTES, "expected: 8, found: 4", "/size" },
        { FIXED_8_BYTES, FIXED_4_BYTES, "expected: 4, found: 8", "/size" },
        { A_DINT_B_DFIXED_8_BYTES_RECORD1, A_DINT_B_DFIXED_4_BYTES_RECORD1, "expected: 4, found: 8", "/fields/1/type/size" },
        { A_DINT_B_DFIXED_4_BYTES_RECORD1, A_DINT_B_DFIXED_8_BYTES_RECORD1, "expected: 8, found: 4", "/fields/1/type/size" }, };
    List<Object[]> list = new ArrayList<>(fields.length);
    for (Object[] schemas : fields) {
      list.add(schemas);
    }
    return list;
  }

  @Parameter(0)
  public Schema reader;
  @Parameter(1)
  public Schema writer;
  @Parameter(2)
  public String details;
  @Parameter(3)
  public String location;

  @Test
  public void testFixedSizeMismatchSchemas() throws Exception {
    validateIncompatibleSchemas(reader, writer, SchemaIncompatibilityType.FIXED_SIZE_MISMATCH,
        details, location);
  }
}
