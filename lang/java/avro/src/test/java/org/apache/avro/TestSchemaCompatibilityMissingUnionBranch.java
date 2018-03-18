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

import static java.util.Arrays.asList;
import static org.apache.avro.TestSchemaCompatibility.validateIncompatibleSchemas;
import static org.apache.avro.TestSchemas.A_DINT_B_DINT_STRING_UNION_RECORD1;
import static org.apache.avro.TestSchemas.A_DINT_B_DINT_UNION_RECORD1;
import static org.apache.avro.TestSchemas.BOOLEAN_SCHEMA;
import static org.apache.avro.TestSchemas.BYTES_UNION_SCHEMA;
import static org.apache.avro.TestSchemas.DOUBLE_UNION_SCHEMA;
import static org.apache.avro.TestSchemas.ENUM1_AB_SCHEMA;
import static org.apache.avro.TestSchemas.FIXED_4_BYTES;
import static org.apache.avro.TestSchemas.FLOAT_UNION_SCHEMA;
import static org.apache.avro.TestSchemas.INT_ARRAY_SCHEMA;
import static org.apache.avro.TestSchemas.INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA;
import static org.apache.avro.TestSchemas.INT_MAP_SCHEMA;
import static org.apache.avro.TestSchemas.INT_SCHEMA;
import static org.apache.avro.TestSchemas.INT_STRING_UNION_SCHEMA;
import static org.apache.avro.TestSchemas.INT_UNION_SCHEMA;
import static org.apache.avro.TestSchemas.LONG_UNION_SCHEMA;
import static org.apache.avro.TestSchemas.NULL_SCHEMA;
import static org.apache.avro.TestSchemas.STRING_UNION_SCHEMA;
import static org.apache.avro.TestSchemas.list;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.avro.SchemaCompatibility.SchemaIncompatibilityType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestSchemaCompatibilityMissingUnionBranch {

  private static final Schema RECORD1_WITH_INT = SchemaBuilder.record("Record1").fields() //
      .name("field1").type(INT_SCHEMA).noDefault() //
      .endRecord();
  private static final Schema RECORD2_WITH_INT = SchemaBuilder.record("Record2").fields() //
      .name("field1").type(INT_SCHEMA).noDefault() //
      .endRecord();
  private static final Schema UNION_INT_RECORD1 = Schema
      .createUnion(list(INT_SCHEMA, RECORD1_WITH_INT));
  private static final Schema UNION_INT_RECORD2 = Schema
      .createUnion(list(INT_SCHEMA, RECORD2_WITH_INT));
  private static final Schema UNION_INT_ENUM1_AB = Schema
      .createUnion(list(INT_SCHEMA, ENUM1_AB_SCHEMA));
  private static final Schema UNION_INT_FIXED_4_BYTES = Schema
      .createUnion(list(INT_SCHEMA, FIXED_4_BYTES));
  private static final Schema UNION_INT_BOOLEAN = Schema
      .createUnion(list(INT_SCHEMA, BOOLEAN_SCHEMA));
  private static final Schema UNION_INT_ARRAY_INT = Schema
      .createUnion(list(INT_SCHEMA, INT_ARRAY_SCHEMA));
  private static final Schema UNION_INT_MAP_INT = Schema
      .createUnion(list(INT_SCHEMA, INT_MAP_SCHEMA));
  private static final Schema UNION_INT_NULL = Schema.createUnion(list(INT_SCHEMA, NULL_SCHEMA));

  @Parameters(name = "r: {0} | w: {1}")
  public static Iterable<Object[]> data() {
    Object[][] fields = { //
        { INT_UNION_SCHEMA, INT_STRING_UNION_SCHEMA, asList("reader union lacking writer type: STRING"), asList("/1") },
        { STRING_UNION_SCHEMA, INT_STRING_UNION_SCHEMA, asList("reader union lacking writer type: INT"), asList("/0") },
        { INT_UNION_SCHEMA, UNION_INT_RECORD1, asList("reader union lacking writer type: RECORD"), asList("/1") },
        { INT_UNION_SCHEMA, UNION_INT_RECORD2, asList("reader union lacking writer type: RECORD"), asList("/1") },
        // more info in the subset schemas
        { UNION_INT_RECORD1, UNION_INT_RECORD2, asList("reader union lacking writer type: RECORD"), asList("/1") },
        { INT_UNION_SCHEMA, UNION_INT_ENUM1_AB, asList("reader union lacking writer type: ENUM"), asList("/1") },
        { INT_UNION_SCHEMA, UNION_INT_FIXED_4_BYTES, asList("reader union lacking writer type: FIXED"), asList("/1") },
        { INT_UNION_SCHEMA, UNION_INT_BOOLEAN, asList("reader union lacking writer type: BOOLEAN"), asList("/1") },
        { INT_UNION_SCHEMA, LONG_UNION_SCHEMA, asList("reader union lacking writer type: LONG"), asList("/0") },
        { INT_UNION_SCHEMA, FLOAT_UNION_SCHEMA, asList("reader union lacking writer type: FLOAT"), asList("/0") },
        { INT_UNION_SCHEMA, DOUBLE_UNION_SCHEMA, asList("reader union lacking writer type: DOUBLE"), asList("/0") },
        { INT_UNION_SCHEMA, BYTES_UNION_SCHEMA, asList("reader union lacking writer type: BYTES"), asList("/0") },
        { INT_UNION_SCHEMA, UNION_INT_ARRAY_INT, asList("reader union lacking writer type: ARRAY"), asList("/1") },
        { INT_UNION_SCHEMA, UNION_INT_MAP_INT, asList("reader union lacking writer type: MAP"), asList("/1") },
        { INT_UNION_SCHEMA, UNION_INT_NULL, asList("reader union lacking writer type: NULL"), asList("/1") },
        { INT_UNION_SCHEMA, INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA,
              asList("reader union lacking writer type: LONG", "reader union lacking writer type: FLOAT", "reader union lacking writer type: DOUBLE"),
              asList("/1", "/2", "/3") },
        { A_DINT_B_DINT_UNION_RECORD1, A_DINT_B_DINT_STRING_UNION_RECORD1,
              asList("reader union lacking writer type: STRING"), asList("/fields/1/type/1") } };
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
  public List<String> details;
  @Parameter(3)
  public List<String> location;

  @Test
  public void testMissingUnionBranch() throws Exception {
    List<SchemaIncompatibilityType> types = Collections.nCopies(details.size(), SchemaIncompatibilityType.MISSING_UNION_BRANCH);
    validateIncompatibleSchemas(reader, writer, types, details, location);
  }
}
