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
package org.apache.avro.idl;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

public class TestSchemas {

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
  public void testVisit1() {
    String s1 = "{\"type\": \"record\", \"name\": \"t1\", \"fields\": [" + "{\"name\": \"f1\", \"type\": \"int\"}"
        + "]}";
    Assert.assertEquals("t1.", Schemas.visit(new Schema.Parser().parse(s1), new TestVisitor()));
  }

  @Test
  public void testVisit2() {
    String s2 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": [" + "{\"name\": \"f1\", \"type\": \"int\"}"
        + "]}";
    Assert.assertEquals("c1.\"int\"!", Schemas.visit(new Schema.Parser().parse(s2), new TestVisitor()));

  }

  @Test
  public void testVisit3() {
    String s3 = "{\"type\": \"record\", \"name\": \"ss1\", \"fields\": [" + "{\"name\": \"f1\", \"type\": \"int\"}"
        + "]}";
    Assert.assertEquals("ss1.", Schemas.visit(new Schema.Parser().parse(s3), new TestVisitor()));

  }

  @Test
  public void testVisit4() {
    String s4 = "{\"type\": \"record\", \"name\": \"st1\", \"fields\": [" + "{\"name\": \"f1\", \"type\": \"int\"}"
        + "]}";
    Assert.assertEquals("st1.!", Schemas.visit(new Schema.Parser().parse(s4), new TestVisitor()));

  }

  @Test
  public void testVisit5() {
    String s5 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
        + "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"c2\", \"fields\": "
        + "[{\"name\": \"f11\", \"type\": \"int\"}]}}," + "{\"name\": \"f2\", \"type\": \"long\"}" + "]}";
    Assert.assertEquals("c1.c2.\"int\"!\"long\"!", Schemas.visit(new Schema.Parser().parse(s5), new TestVisitor()));

  }

  @Test
  public void testVisit6() {
    String s6 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
        + "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"ss2\", \"fields\": "
        + "[{\"name\": \"f11\", \"type\": \"int\"}]}}," + "{\"name\": \"f2\", \"type\": \"long\"}" + "]}";
    Assert.assertEquals("c1.ss2.!", Schemas.visit(new Schema.Parser().parse(s6), new TestVisitor()));

  }

  @Test
  public void testVisit7() {
    String s7 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
        + "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"css2\", \"fields\": "
        + "[{\"name\": \"f11\", \"type\": \"int\"}]}}," + "{\"name\": \"f2\", \"type\": \"long\"}" + "]}";
    Assert.assertEquals("c1.css2.\"int\"!!", Schemas.visit(new Schema.Parser().parse(s7), new TestVisitor()));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testVisit8() {
    String s8 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
        + "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"cst2\", \"fields\": "
        + "[{\"name\": \"f11\", \"type\": \"int\"}]}}," + "{\"name\": \"f2\", \"type\": \"int\"}" + "]}";
    Schemas.visit(new Schema.Parser().parse(s8), new TestVisitor());
  }

  @Test
  public void testVisit9() {
    String s9 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
        + "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"ct2\", \"fields\": "
        + "[{\"name\": \"f11\", \"type\": \"int\"}]}}," + "{\"name\": \"f2\", \"type\": \"long\"}" + "]}";
    Assert.assertEquals("c1.ct2.\"int\"!", Schemas.visit(new Schema.Parser().parse(s9), new TestVisitor()));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testVisit10() {
    String s10 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
        + "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"ct2\", \"fields\": "
        + "[{\"name\": \"f11\", \"type\": \"int\"}]}}," + "{\"name\": \"f2\", \"type\": \"int\"}" + "]}";
    Schemas.visit(new Schema.Parser().parse(s10), new TestVisitor() {
      @Override
      public SchemaVisitorAction visitTerminal(Schema terminal) {
        return SchemaVisitorAction.SKIP_SUBTREE;
      }
    });
  }

  @Test
  public void testVisit11() {
    String s11 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
        + "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"c2\", \"fields\": "
        + "[{\"name\": \"f11\", \"type\": \"int\"},{\"name\": \"f12\", \"type\": \"double\"}" + "]}},"
        + "{\"name\": \"f2\", \"type\": \"long\"}" + "]}";
    Assert.assertEquals("c1.c2.\"int\".!\"long\".!", Schemas.visit(new Schema.Parser().parse(s11), new TestVisitor() {
      @Override
      public SchemaVisitorAction visitTerminal(Schema terminal) {
        sb.append(terminal).append('.');
        return SchemaVisitorAction.SKIP_SIBLINGS;
      }
    }));
  }

  @Test
  public void testVisit12() {
    String s12 = "{\"type\": \"record\", \"name\": \"c1\", \"fields\": ["
        + "{\"name\": \"f1\", \"type\": {\"type\": \"record\", \"name\": \"ct2\", \"fields\": "
        + "[{\"name\": \"f11\", \"type\": \"int\"}]}}," + "{\"name\": \"f2\", \"type\": \"long\"}" + "]}";
    Assert.assertEquals("c1.ct2.\"int\".", Schemas.visit(new Schema.Parser().parse(s12), new TestVisitor() {
      @Override
      public SchemaVisitorAction visitTerminal(Schema terminal) {
        sb.append(terminal).append('.');
        return SchemaVisitorAction.TERMINATE;
      }
    }));
  }

  @Test
  public void testVisit13() {
    String s12 = "{\"type\": \"int\"}";
    Assert.assertEquals("\"int\".", Schemas.visit(new Schema.Parser().parse(s12), new TestVisitor() {
      @Override
      public SchemaVisitorAction visitTerminal(Schema terminal) {
        sb.append(terminal).append('.');
        return SchemaVisitorAction.SKIP_SIBLINGS;
      }
    }));
  }
}
