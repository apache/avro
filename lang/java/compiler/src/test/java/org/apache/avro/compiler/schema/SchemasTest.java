/*
 * Copyright 2017 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.compiler.schema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.junit.Assert;
import org.junit.Test;

public class SchemasTest {

  private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"SampleNode\",\"doc\":\"caca\","
      + "\"namespace\":\"org.spf4j.ssdump2.avro\",\n" +
      " \"fields\":[\n" +
      "    {\"name\":\"count\",\"type\":\"int\",\"default\":0,\"doc\":\"caca\"},\n" +
      "    {\"name\":\"subNodes\",\"type\":\n" +
      "       {\"type\":\"array\",\"items\":{\n" +
      "           \"type\":\"record\",\"name\":\"SamplePair\",\n" +
      "           \"fields\":[\n" +
      "              {\"name\":\"method\",\"type\":\n" +
      "                  {\"type\":\"record\",\"name\":\"Method\",\n" +
      "                  \"fields\":[\n" +
      "                     {\"name\":\"declaringClass\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},\n" +
      "                     {\"name\":\"methodName\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}\n" +
      "                  ]}},\n" +
      "              {\"name\":\"node\",\"type\":\"SampleNode\"}]}}}]}";

  private static class PrintingVisitor implements SchemaVisitor {


    @Override
    public SchemaVisitorAction visitTerminal(Schema terminal) {
      System.out.println("Terminal: " + terminal.getFullName());
      return SchemaVisitorAction.CONTINUE;
    }

    @Override
    public SchemaVisitorAction visitNonTerminal(Schema terminal) {
      System.out.println("NONTerminal start: " + terminal.getFullName());
      return SchemaVisitorAction.CONTINUE;
    }

    @Override
    public SchemaVisitorAction afterVisitNonTerminal(Schema terminal) {
      System.out.println("NONTerminal end: " + terminal.getFullName());
      return SchemaVisitorAction.CONTINUE;
    }

    @Override
    public Object get() {
      return null;
    }
  }



  @Test
  public void textCloning() {
    Schema recSchema = new Schema.Parser().parse(SCHEMA);
    Schemas.visit(recSchema, new PrintingVisitor());


    Schema trimmed = Schemas.visit(recSchema, new CloningVisitor(recSchema));
    Assert.assertNull(trimmed.getDoc());
    Assert.assertNotNull(recSchema.getDoc());

    SchemaCompatibility.SchemaCompatibilityType compat =
            SchemaCompatibility.checkReaderWriterCompatibility(trimmed, recSchema).getType();
    Assert.assertEquals(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE, compat);
    compat = SchemaCompatibility.checkReaderWriterCompatibility(recSchema, trimmed).getType();
    Assert.assertEquals(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE, compat);
  }

}
