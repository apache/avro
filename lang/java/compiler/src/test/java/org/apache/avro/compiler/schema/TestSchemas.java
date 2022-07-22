/*
 * Copyright 2017 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSchemas {

  private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"SampleNode\",\"doc\":\"caca\","
      + "\"namespace\":\"org.spf4j.ssdump2.avro\",\n" + " \"fields\":[\n"
      + "    {\"name\":\"count\",\"type\":\"int\",\"default\":0,\"doc\":\"caca\"},\n"
      + "    {\"name\":\"kind1\",\"type\":{\"type\":\"enum\", \"name\": \"Kind1\", \"symbols\": [\"A1\", \"B1\"]}},\n"
      + "    {\"name\":\"kind2\",\"type\":{\"type\":\"enum\", \"name\": \"Kind2\", \"symbols\": [\"A2\", \"B2\"], \"doc\": \"doc\"}},\n"
      + "    {\"name\":\"pat\",\"type\":{\"type\":\"fixed\", \"name\": \"FixedPattern\", \"size\": 10}},\n"
      + "    {\"name\":\"uni\",\"type\":[\"int\", \"double\"]},\n"
      + "    {\"name\":\"mp\",\"type\":{\"type\":\"map\", \"values\": \"int\"}},\n"
      + "    {\"name\":\"subNodes\",\"type\":\n" + "       {\"type\":\"array\",\"items\":{\n"
      + "           \"type\":\"record\",\"name\":\"SamplePair\",\n" + "           \"fields\":[\n"
      + "              {\"name\":\"method\",\"type\":\n"
      + "                  {\"type\":\"record\",\"name\":\"Method\",\n" + "                  \"fields\":[\n"
      + "                     {\"name\":\"declaringClass\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},\n"
      + "                     {\"name\":\"methodName\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}\n"
      + "                  ]}},\n" + "              {\"name\":\"node\",\"type\":\"SampleNode\"}]}}}" + "]}";

  @Test
  void textCloning() {
    Schema recSchema = new Schema.Parser().parse(SCHEMA);

    CloningVisitor cv = new CloningVisitor(recSchema);
    Schema trimmed = Schemas.visit(recSchema, cv);
    assertNull(trimmed.getDoc());
    assertNotNull(recSchema.getDoc());

    SchemaCompatibility.SchemaCompatibilityType compat = SchemaCompatibility
        .checkReaderWriterCompatibility(trimmed, recSchema).getType();
    assertEquals(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE, compat);
    compat = SchemaCompatibility.checkReaderWriterCompatibility(recSchema, trimmed).getType();
    assertEquals(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE, compat);
    assertNotNull(cv.toString());
  }

  @Test
  void textCloningCopyDocs() {
    Schema recSchema = new Schema.Parser().parse(SCHEMA);

    Schema trimmed = Schemas.visit(recSchema, new CloningVisitor(new CloningVisitor.PropertyCopier() {
      @Override
      public void copy(final Schema first, final Schema second) {
        Schemas.copyLogicalTypes(first, second);
        Schemas.copyAliases(first, second);
      }

      @Override
      public void copy(final Schema.Field first, final Schema.Field second) {
        Schemas.copyAliases(first, second);
      }
    }, true, recSchema));
    assertEquals("caca", trimmed.getDoc());
    assertNotNull(recSchema.getDoc());

    SchemaCompatibility.SchemaCompatibilityType compat = SchemaCompatibility
        .checkReaderWriterCompatibility(trimmed, recSchema).getType();
    assertEquals(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE, compat);
    compat = SchemaCompatibility.checkReaderWriterCompatibility(recSchema, trimmed).getType();
    assertEquals(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE, compat);
  }

  @Test
  void cloningError1() {
    assertThrows(IllegalStateException.class, () -> {
      // Visit Terminal with union
      Schema recordSchema = new Schema.Parser().parse(
          "{\"type\": \"record\", \"name\": \"R\", \"fields\":[{\"name\": \"f1\", \"type\": [\"int\", \"long\"]}]}");
      new CloningVisitor(recordSchema).visitTerminal(recordSchema.getField("f1").schema());
    });
  }

  @Test
  void cloningError2() {
    assertThrows(IllegalStateException.class, () -> {
      // After visit Non-terminal with int
      Schema recordSchema = new Schema.Parser()
          .parse("{\"type\": \"record\", \"name\": \"R\", \"fields\":[{\"name\": \"f1\", \"type\": \"int\"}]}");
      new CloningVisitor(recordSchema).afterVisitNonTerminal(recordSchema.getField("f1").schema());
    });
  }

  @Test
  void hasGeneratedJavaClass() {
    assertTrue(Schemas
        .hasGeneratedJavaClass(new Schema.Parser().parse("{\"type\": \"fixed\", \"name\": \"N\", \"size\": 10}")));
    assertFalse(Schemas.hasGeneratedJavaClass(new Schema.Parser().parse("{\"type\": \"int\"}")));
  }

  @Test
  void getJavaClassName() {
    assertEquals("N",
        Schemas.getJavaClassName(new Schema.Parser().parse("{\"type\": \"fixed\", \"name\": \"N\", \"size\": 10}")));
    assertEquals("N", Schemas.getJavaClassName(
        new Schema.Parser().parse("{\"type\": \"fixed\", \"name\": \"N\", \"size\": 10, \"namespace\": \"\"}")));
    assertEquals("com.example.N", Schemas.getJavaClassName(new Schema.Parser()
        .parse("{\"type\": \"fixed\", \"name\": \"N\", \"size\": 10, \"namespace\": \"com.example\"}")));
  }

  private static class TestVisitor implements SchemaVisitor<String> {
    StringBuilder sb = new StringBuilder();

    @Override
    public SchemaVisitorAction visitTerminal(Schema terminal) {
      sb.append(terminal);
      return SchemaVisitorAction.CONTINUE;
    }

    @Override
    public SchemaVisitorAction visitNonTerminal(Schema nonTerminal) {
      String n = nonTerminal.getName();
      sb.append(n).append('.');
      if (n.startsWith("t")) {
        return SchemaVisitorAction.TERMINATE;
      } else if (n.startsWith("ss")) {
        return SchemaVisitorAction.SKIP_SIBLINGS;
      } else if (n.startsWith("st")) {
        return SchemaVisitorAction.SKIP_SUBTREE;
      } else {
        return SchemaVisitorAction.CONTINUE;
      }
    }

    @Override
    public SchemaVisitorAction afterVisitNonTerminal(Schema nonTerminal) {
      sb.append("!");
      String n = nonTerminal.getName();
      if (n.startsWith("ct")) {
        return SchemaVisitorAction.TERMINATE;
      } else if (n.startsWith("css")) {
        return SchemaVisitorAction.SKIP_SIBLINGS;
      } else if (n.startsWith("cst")) {
        return SchemaVisitorAction.SKIP_SUBTREE;
      } else {
        return SchemaVisitorAction.CONTINUE;
      }
    }

    @Override
    public String get() {
      return sb.toString();
    }
  }

  @Test
  void visit1() {
    String s1 = "{\"type\": \"record\", \"name\": \"t1\", \"fields\": [" + "{\"name\": \"f1\", \"type\": \"int\"}"
        + "]}";
    assertEquals("t1.", Schemas.visit(new Schema.Parser().parse(s1), new TestVisitor()));
  }

  @Test
  void visit2() {
    String s2 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": [" + "{\"name\": \"f1\", \"type\": \"int\"}"
        + "]}";
    assertEquals("c1.\"int\"!", Schemas.visit(new Schema.Parser().parse(s2), new TestVisitor()));

  }

  @Test
  void visit3() {
    String s3 = "{\"type\": \"record\", \"name\": \"ss1\", \"fields\": [" + "{\"name\": \"f1\", \"type\": \"int\"}"
        + "]}";
    assertEquals("ss1.", Schemas.visit(new Schema.Parser().parse(s3), new TestVisitor()));

  }

  @Test
  void visit4() {
    String s4 = "{\"type\": \"record\", \"name\": \"st1\", \"fields\": [" + "{\"name\": \"f1\", \"type\": \"int\"}"
        + "]}";
    assertEquals("st1.!", Schemas.visit(new Schema.Parser().parse(s4), new TestVisitor()));

  }

  @Test
  void visit5() {
    String s5 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
        + "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"c2\", \"fields\": "
        + "[{\"name\": \"f11\", \"type\": \"int\"}]}}," + "{\"name\": \"f2\", \"type\": \"long\"}" + "]}";
    assertEquals("c1.c2.\"int\"!\"long\"!", Schemas.visit(new Schema.Parser().parse(s5), new TestVisitor()));

  }

  @Test
  void visit6() {
    String s6 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
        + "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"ss2\", \"fields\": "
        + "[{\"name\": \"f11\", \"type\": \"int\"}]}}," + "{\"name\": \"f2\", \"type\": \"long\"}" + "]}";
    assertEquals("c1.ss2.!", Schemas.visit(new Schema.Parser().parse(s6), new TestVisitor()));

  }

  @Test
  void visit7() {
    String s7 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
        + "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"css2\", \"fields\": "
        + "[{\"name\": \"f11\", \"type\": \"int\"}]}}," + "{\"name\": \"f2\", \"type\": \"long\"}" + "]}";
    assertEquals("c1.css2.\"int\"!!", Schemas.visit(new Schema.Parser().parse(s7), new TestVisitor()));
  }

  @Test
  void visit8() {
    assertThrows(UnsupportedOperationException.class, () -> {
      String s8 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
          + "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"cst2\", \"fields\": "
          + "[{\"name\": \"f11\", \"type\": \"int\"}]}}," + "{\"name\": \"f2\", \"type\": \"int\"}" + "]}";
      Schemas.visit(new Schema.Parser().parse(s8), new TestVisitor());
    });
  }

  @Test
  void visit9() {
    String s9 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
        + "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"ct2\", \"fields\": "
        + "[{\"name\": \"f11\", \"type\": \"int\"}]}}," + "{\"name\": \"f2\", \"type\": \"long\"}" + "]}";
    assertEquals("c1.ct2.\"int\"!", Schemas.visit(new Schema.Parser().parse(s9), new TestVisitor()));
  }

  @Test
  void visit10() {
    String s10 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
        + "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"ct2\", \"fields\": "
        + "[{\"name\": \"f11\", \"type\": \"int\"}]}}," + "{\"name\": \"f2\", \"type\": \"int\"}" + "]}";
    assertThrows(UnsupportedOperationException.class, () -> {
      Schemas.visit(new Schema.Parser().parse(s10), new TestVisitor() {
        @Override
        public SchemaVisitorAction visitTerminal(Schema terminal) {
          return SchemaVisitorAction.SKIP_SUBTREE;
        }
      });
    });
  }

  @Test
  void visit11() {
    String s11 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
        + "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"c2\", \"fields\": "
        + "[{\"name\": \"f11\", \"type\": \"int\"},{\"name\": \"f12\", \"type\": \"double\"}" + "]}},"
        + "{\"name\": \"f2\", \"type\": \"long\"}" + "]}";
    assertEquals("c1.c2.\"int\".!\"long\".!", Schemas.visit(new Schema.Parser().parse(s11), new TestVisitor() {
      public SchemaVisitorAction visitTerminal(Schema terminal) {
        sb.append(terminal).append('.');
        return SchemaVisitorAction.SKIP_SIBLINGS;
      }
    }));
  }

  @Test
  void visit12() {
    String s12 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
        + "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"ct2\", \"fields\": "
        + "[{\"name\": \"f11\", \"type\": \"int\"}]}}," + "{\"name\": \"f2\", \"type\": \"long\"}" + "]}";
    assertEquals("c1.ct2.\"int\".", Schemas.visit(new Schema.Parser().parse(s12), new TestVisitor() {
      public SchemaVisitorAction visitTerminal(Schema terminal) {
        sb.append(terminal).append('.');
        return SchemaVisitorAction.TERMINATE;
      }
    }));
  }

  @Test
  void visit13() {
    String s12 = "{\"type\": \"int\"}";
    assertEquals("\"int\".", Schemas.visit(new Schema.Parser().parse(s12), new TestVisitor() {
      public SchemaVisitorAction visitTerminal(Schema terminal) {
        sb.append(terminal).append('.');
        return SchemaVisitorAction.SKIP_SIBLINGS;
      }
    }));
  }
}
