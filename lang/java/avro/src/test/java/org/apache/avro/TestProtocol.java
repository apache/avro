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
package org.apache.avro;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;

import com.fasterxml.jackson.databind.JsonNode;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Collections;

import org.junit.jupiter.api.Test;

public class TestProtocol {

  @Test
  public void parse() throws IOException {
    File fic = new File("target/test-classes/share/test/schemas/namespace.avpr");
    Protocol protocol = Protocol.parse(fic);
    assertNotNull(protocol);
    assertEquals("TestNamespace", protocol.getName());
  }

  /**
   * record type 'User' contains a field of type 'Status', which contains a field
   * of type 'User'.
   */
  @Test
  public void crossProtocol() {
    String userStatus = "{ \"protocol\" : \"p1\", " + "\"types\": ["
        + "{\"name\": \"User\", \"type\": \"record\", \"fields\": [{\"name\": \"current_status\", \"type\": \"Status\"}]},\n"
        + "\n"
        + "{\"name\": \"Status\", \"type\": \"record\", \"fields\": [{\"name\": \"author\", \"type\": \"User\"}]}"
        + "]}";

    Protocol protocol = Protocol.parse(userStatus);
    Schema userSchema = protocol.getType("User");
    Schema statusSchema = protocol.getType("Status");
    assertSame(statusSchema, userSchema.getField("current_status").schema());
    assertSame(userSchema, statusSchema.getField("author").schema());

    String parsingFormUser = SchemaNormalization.toParsingForm(userSchema);
    assertEquals(
        "{\"name\":\"User\",\"type\":\"record\",\"fields\":[{\"name\":\"current_status\",\"type\":{\"name\":\"Status\",\"type\":\"record\",\"fields\":[{\"name\":\"author\",\"type\":\"User\"}]}}]}",
        parsingFormUser);

    String parsingFormStatus = SchemaNormalization.toParsingForm(statusSchema);
    assertEquals(
        "{\"name\":\"Status\",\"type\":\"record\",\"fields\":[{\"name\":\"author\",\"type\":{\"name\":\"User\",\"type\":\"record\",\"fields\":[{\"name\":\"current_status\",\"type\":\"Status\"}]}}]}",
        parsingFormStatus);
  }

  /**
   * When one schema with a type used before it is defined, test normalization
   * defined schema before it is used.
   */
  @Test
  void normalization() {
    final String schema = "{\n" + " \"type\":\"record\",  \"name\": \"Main\", " + " \"fields\":[\n"
        + "    {  \"name\":\"f1\", \"type\":\"Sub\"  },\n" // use Sub
        + "    {  \"name\":\"f2\", " + "       \"type\":{\n" + "         \"type\":\"enum\",  \"name\":\"Sub\",\n" // define
                                                                                                                  // Sub
        + "         \"symbols\":[\"OPEN\",\"CLOSE\"]\n" + "        }\n" + "    }\n" + " ]\n" + "}";
    Schema s = new Schema.Parser().parse(schema);
    assertNotNull(s);

    String parsingForm = SchemaNormalization.toParsingForm(s);
    assertEquals(
        "{\"name\":\"Main\",\"type\":\"record\",\"fields\":[{\"name\":\"f1\",\"type\":{\"name\":\"Sub\",\"type\":\"enum\",\"symbols\":[\"OPEN\",\"CLOSE\"]}},{\"name\":\"f2\",\"type\":\"Sub\"}]}",
        parsingForm);
  }

  @Test
  void namespaceAndNameRules() {
    Protocol p1 = new Protocol("P", null, "foo");
    Protocol p2 = new Protocol("foo.P", null, null);
    Protocol p3 = new Protocol("foo.P", null, "bar");
    assertEquals(p1.getName(), p2.getName());
    assertEquals(p1.getNamespace(), p2.getNamespace());
    assertEquals(p1.getName(), p3.getName());
    assertEquals(p1.getNamespace(), p3.getNamespace());

    // The following situation is allowed, even if confusing, because the
    // specification describes this algorithm without specifying that the resulting
    // namespace mst be non-empty.
    Protocol invalidName = new Protocol(".P", null, "ignored");
    assertNull(invalidName.getNamespace());
    assertEquals("P", invalidName.getName());
  }

  @Test
  void propEquals() {
    Protocol p1 = new Protocol("P", null, "foo");
    p1.addProp("a", "1");
    Protocol p2 = new Protocol("P", null, "foo");
    p2.addProp("a", "2");
    assertNotEquals(p1, p2);
  }

  @Test
  void splitProtocolBuild() {
    Protocol p = new Protocol("P", null, "foo");
    p.addProp("property", "some value");

    String protocolString = p.toString();
    final int mid = protocolString.length() / 2;

    Protocol parsedStringProtocol = org.apache.avro.Protocol.parse(protocolString);
    Protocol parsedArrayOfStringProtocol = org.apache.avro.Protocol.parse(protocolString.substring(0, mid),
        protocolString.substring(mid));

    assertNotNull(parsedStringProtocol);
    assertNotNull(parsedArrayOfStringProtocol);
    assertEquals(parsedStringProtocol.toString(), parsedArrayOfStringProtocol.toString());
  }

  @Test
  void copyMessage() {
    Protocol p = new Protocol("P", "protocol", "foo");
    Schema req1 = SchemaBuilder.record("foo.req1").fields().endRecord();
    Protocol.Message m1 = p.createMessage("M", "message", singletonMap("foo", "bar"), req1);
    Schema req2 = SchemaBuilder.record("foo.req2").fields().name("test").type().booleanType().noDefault().endRecord();

    Protocol.Message m2 = p.createMessage(m1, req2);
    assertEquals(m1.getName(), m2.getName());
    assertEquals(m1.getDoc(), m2.getDoc());
    assertEquals(m1.getProp("foo"), m2.getProp("foo"));
  }
}
