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
package org.apache.avro;

import org.apache.avro.SchemaCompatibility.SchemaIncompatibilityType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.List;

import static org.apache.avro.TestSchemaCompatibility.validateIncompatibleSchemas;
import static org.apache.avro.TestSchemas.BYTES_DECIMAL_3_2;
import static org.apache.avro.TestSchemas.BYTES_DECIMAL_3_3;
import static org.apache.avro.TestSchemas.BYTES_DECIMAL_4_3;

@RunWith(Parameterized.class)
public class TestBytesSchemaCompatibilityDecimalMismatch {

  @Parameters(name = "r: {0} | w: {1}")
  public static Iterable<Object[]> data() {
    Object[][] fields = {
        { BYTES_DECIMAL_3_3 , BYTES_DECIMAL_3_2,
                "Decimal (precision,scale) doesn't match for reader (3,3) and writer (3,2) schemas" },
        { BYTES_DECIMAL_3_3 , BYTES_DECIMAL_4_3,
                "Decimal (precision,scale) doesn't match for reader (3,3) and writer (4,3) schemas" }
    };
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

  @Test
  public void testTypeMismatchSchemas() throws Exception {
    validateIncompatibleSchemas(reader, writer, SchemaIncompatibilityType.DECIMAL_SCALE_OR_PRECISION_MISMATCH, details);
  }
}
