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
package org.apache.avro.specific;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.ArrayRecord;
import org.apache.avro.AvroTypeException;
import org.apache.avro.MapRecord;
import org.apache.avro.MapRecordEnum;
import org.apache.avro.RecordWithRequiredFields;
import org.apache.avro.Schema;
import org.apache.avro.UnionRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.test.Kind;
import org.apache.avro.test.MD5;
import org.apache.avro.test.TestRecordWithUnion;
import org.apache.avro.test.TestRecord;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestSpecificDatumWriter {
  @Test
  public void testResolveUnion() throws IOException {
    final SpecificDatumWriter<TestRecordWithUnion> writer = new SpecificDatumWriter<>();
    Schema schema = TestRecordWithUnion.SCHEMA$;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, out);

    writer.setSchema(schema);

    TestRecordWithUnion c = TestRecordWithUnion.newBuilder().setKind(Kind.BAR).setValue("rab").build();
    writer.write(c, encoder);
    encoder.flush();
    out.close();

    String expectedJson = String.format("{'kind':{'org.apache.avro.test.Kind':'%s'},'value':{'string':'%s'}}",
        c.getKind().toString(), c.getValue()).replace('\'', '"');

    assertEquals(expectedJson, out.toString("UTF-8"));
  }

  @Test
  public void testIncompleteRecord() throws IOException {
    final SpecificDatumWriter<TestRecord> writer = new SpecificDatumWriter<>();
    Schema schema = TestRecord.SCHEMA$;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, out);

    writer.setSchema(schema);

    TestRecord testRecord = new TestRecord();
    testRecord.setKind(Kind.BAR);
    testRecord.setHash(new MD5(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5 }));

    try {
      writer.write(testRecord, encoder);
      fail("Exception not thrown");
    } catch (NullPointerException e) {
      assertTrue(e.getMessage().contains("null value for (non-nullable) string at TestRecord.name"));
    } finally {
      out.close();
    }
  }

  @Test
  public void testNestedNPEErrorClarity() throws Exception {
    RecordWithRequiredFields topLevelRecord = buildComplexRecord();
    topLevelRecord.getUnionField().getArrayField().get(0).getMapField().get("a").setStrField(null);
    try {
      writeObject(topLevelRecord, false);
      Assert.fail("expected to throw");
    } catch (NullPointerException expected) {
      Assert.assertTrue("unexpected message " + expected.getMessage(), expected.getMessage()
          .contains("RecordWithRequiredFields.unionField[UnionRecord].arrayField[0].mapField[\"a\"].strField"));
    }
  }

  @Test
  public void testNestedNPEErrorClarityWithCustomCoders() throws Exception {
    RecordWithRequiredFields topLevelRecord = buildComplexRecord();
    topLevelRecord.getUnionField().getArrayField().get(0).getMapField().get("a").setEnumField(null);
    try {
      writeObject(topLevelRecord, true);
      Assert.fail("expected to throw");
    } catch (NullPointerException expected) {
      Assert.assertTrue("unexpected message " + expected.getMessage(),
          expected.getMessage().contains("custom coders were used"));
    }
  }

  @Test
  public void testNPEForMapKeyErrorClarity() throws Exception {
    RecordWithRequiredFields topLevelRecord = buildComplexRecord();
    Map<String, MapRecord> map = topLevelRecord.getUnionField().getArrayField().get(0).getMapField();
    map.put(null, map.get("a")); // value is valid, but key is null
    try {
      writeObject(topLevelRecord, false);
      Assert.fail("expected to throw");
    } catch (NullPointerException expected) {
      Assert.assertTrue("unexpected message " + expected.getMessage(), expected.getMessage()
          .contains("null key in map at RecordWithRequiredFields.unionField[UnionRecord].arrayField[0].mapField"));
    }
  }

  @Test
  public void testNPEForMapKeyErrorClarityWithCustomCoders() throws Exception {
    RecordWithRequiredFields topLevelRecord = buildComplexRecord();
    Map<String, MapRecord> map = topLevelRecord.getUnionField().getArrayField().get(0).getMapField();
    map.put(null, map.get("a")); // value is valid, but key is null
    try {
      writeObject(topLevelRecord, true);
      Assert.fail("expected to throw");
    } catch (NullPointerException expected) {
      Assert.assertTrue("unexpected message " + expected.getMessage(),
          expected.getMessage().contains("custom coders were used"));
    }
  }

  @Test
  public void testNestedATEErrorClarity() throws Exception {
    RecordWithRequiredFields topLevelRecord = buildComplexRecord();
    topLevelRecord.getUnionField().getArrayField().get(0).getMapField().get("a").setEnumField(null); // not an enum
    try {
      writeObject(topLevelRecord, false);
      Assert.fail("expected to throw");
    } catch (AvroTypeException expected) {
      Assert.assertTrue("unexpected message " + expected.getMessage(), expected.getMessage()
          .contains("RecordWithRequiredFields.unionField[UnionRecord].arrayField[0].mapField[\"a\"].enumField"));
    }
  }

  @Test
  public void testNestedATEErrorClarityWithCustomCoders() throws Exception {
    RecordWithRequiredFields topLevelRecord = buildComplexRecord();
    topLevelRecord.getUnionField().getArrayField().get(0).getMapField().get("a").setEnumField(null); // not an enum
    try {
      writeObject(topLevelRecord, true);
      Assert.fail("expected to throw");
    } catch (NullPointerException expected) {
      // with custom coders this gets us an NPE ...
      Assert.assertTrue("unexpected message " + expected.getMessage(),
          expected.getMessage().contains("custom coders were used"));
    }
  }

  private RecordWithRequiredFields buildComplexRecord() {
    RecordWithRequiredFields topLevelRecord = new RecordWithRequiredFields();
    UnionRecord unionRecord = new UnionRecord();
    ArrayRecord arrayRecord1 = new ArrayRecord();
    ArrayRecord arrayRecord2 = new ArrayRecord();
    MapRecord mapRecordA = new MapRecord();
    mapRecordA.setEnumField(MapRecordEnum.B);
    mapRecordA.setStrField("4");
    arrayRecord1.setStrField("2");
    Map<String, MapRecord> map1 = new HashMap<>();
    map1.put("a", mapRecordA);
    arrayRecord1.setMapField(map1);
    arrayRecord2.setStrField("2");
    Map<String, MapRecord> map2 = new HashMap<>();
    map2.put("a", mapRecordA);
    arrayRecord2.setMapField(map2);
    unionRecord.setStrField("1");
    unionRecord.setArrayField(Arrays.asList(arrayRecord1, arrayRecord2));
    topLevelRecord.setStrField("0");
    topLevelRecord.setUnionField(unionRecord);

    return topLevelRecord;
  }

  private void writeObject(IndexedRecord datum, boolean useCustomCoders) throws Exception {
    writeObject(datum.getSchema(), datum, useCustomCoders);
  }

  private void writeObject(Schema schema, Object datum, boolean useCustomCoders) throws Exception {
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(new ByteArrayOutputStream(), null);
    SpecificData specificData = new SpecificData();
    specificData.setCustomCoders(useCustomCoders);
    SpecificDatumWriter<Object> writer = new SpecificDatumWriter<>(schema, specificData);
    writer.write(datum, encoder);
    encoder.flush();
  }
}
