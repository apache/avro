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
package org.apache.avro.io.parsing;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.ValidatingEncoder;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestResolvingGrammarGenerator {
  private final Schema schema;
  private final JsonNode data;
  
  public TestResolvingGrammarGenerator(String jsonSchema, String jsonData)
    throws IOException {
    this.schema = Schema.parse(jsonSchema);
    JsonFactory factory = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper(factory);

    this.data = mapper.readTree(new StringReader(jsonData));
  }

  @Test
  public void test() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder e = new ValidatingEncoder(schema, new BinaryEncoder(baos));
    
    ResolvingGrammarGenerator.encode(e, schema, data);
  }
  
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> ret = Arrays.asList(
        new Object[][] {
            { "{ \"type\": \"record\", \"name\": \"r\", \"fields\": [ "
                + " { \"name\" : \"f1\", \"type\": \"int\" }, "
                + " { \"name\" : \"f2\", \"type\": \"float\" } "
                + "] } }",
              "{ \"f2\": 10.4, \"f1\": 10 } " },
            { "{ \"type\": \"enum\", \"name\": \"e\", \"symbols\": "
                + "[ \"s1\", \"s2\"] } }", " \"s1\" " },
            { "{ \"type\": \"enum\", \"name\": \"e\", \"symbols\": "
                + "[ \"s1\", \"s2\"] } }", " \"s2\" " },
            { "{ \"type\": \"fixed\", \"name\": \"f\", \"size\": 10 }",
              "\"hello\"" },
            { "{ \"type\": \"array\", \"items\": \"int\" }",
              "[ 10, 20, 30 ]" },
            { "{ \"type\": \"map\", \"values\": \"int\" }",
              "{ \"k1\": 10, \"k3\": 20, \"k3\": 30 }" },
            { "[ \"int\", \"long\" ]", "10" },
            { "\"string\"", "\"hello\"" },
            { "\"bytes\"", "\"hello\"" },
            { "\"int\"", "10" },
            { "\"long\"", "10" },
            { "\"float\"", "10.0" },
            { "\"double\"", "10.0" },
            { "\"boolean\"", "true" },
            { "\"boolean\"", "false" },
            { "\"null\"", "null" },
            }
        );
    return ret;
  }

}
