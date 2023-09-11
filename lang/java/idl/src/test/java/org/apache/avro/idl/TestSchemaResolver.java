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

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestSchemaResolver {

  @Test
  public void testResolving() throws IOException {
    Path testIdl = Paths.get(".", "src", "test", "idl", "cycle.avdl").toAbsolutePath();
    IdlReader parser = new IdlReader();
    IdlFile idlFile = parser.parse(testIdl);
    Protocol protocol = idlFile.getProtocol();
    System.out.println(protocol);
    Assert.assertEquals(5, protocol.getTypes().size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIsUnresolvedSchemaError1() {
    // No "org.apache.avro.idl.unresolved.name" property
    Schema s = SchemaBuilder.record("R").fields().endRecord();
    SchemaResolver.getUnresolvedSchemaName(s);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIsUnresolvedSchemaError2() {
    // No "UnresolvedSchema" property
    Schema s = SchemaBuilder.record("R").prop("org.apache.avro.idl.unresolved.name", "x").fields().endRecord();
    SchemaResolver.getUnresolvedSchemaName(s);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIsUnresolvedSchemaError3() {
    // Namespace not "org.apache.avro.compiler".
    Schema s = SchemaBuilder.record("UnresolvedSchema").prop("org.apache.avro.idl.unresolved.name", "x").fields()
        .endRecord();
    SchemaResolver.getUnresolvedSchemaName(s);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetUnresolvedSchemaNameError() {
    Schema s = SchemaBuilder.fixed("a").size(10);
    SchemaResolver.getUnresolvedSchemaName(s);
  }
}
