/*
 * Copyright 2015 The Apache Software Foundation.
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

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;

import static java.util.Objects.requireNonNull;

public class TestCycle {

  private static final Logger LOG = LoggerFactory.getLogger(TestCycle.class);

  @Test
  public void testCycleGeneration() throws IOException, URISyntaxException {
    final ClassLoader cl = Thread.currentThread().getContextClassLoader();
    IdlFile idlFile = new IdlReader().parse(requireNonNull(cl.getResource("input/cycle.avdl")).toURI());
    String json = idlFile.outputString();
    LOG.info(json);

    GenericRecordBuilder rb2 = new GenericRecordBuilder(idlFile.getNamedSchema("SampleNode"));
    rb2.set("count", 10);
    rb2.set("subNodes", Collections.EMPTY_LIST);
    GenericData.Record node = rb2.build();

    GenericRecordBuilder mb = new GenericRecordBuilder(idlFile.getNamedSchema("Method"));
    mb.set("declaringClass", "Test");
    mb.set("methodName", "test");
    GenericData.Record method = mb.build();

    GenericRecordBuilder spb = new GenericRecordBuilder(idlFile.getNamedSchema("SamplePair"));
    spb.set("method", method);
    spb.set("node", node);
    GenericData.Record sp = spb.build();

    GenericRecordBuilder rb = new GenericRecordBuilder(idlFile.getNamedSchema("SampleNode"));
    rb.set("count", 10);
    rb.set("subNodes", Collections.singletonList(sp));
    GenericData.Record record = rb.build();

    serDeserRecord(record);
  }

  private static void serDeserRecord(GenericData.Record data) throws IOException {
    ByteArrayOutputStream bab = new ByteArrayOutputStream();
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(data.getSchema());
    final BinaryEncoder directBinaryEncoder = EncoderFactory.get().directBinaryEncoder(bab, null);
    writer.write(data, directBinaryEncoder);
    directBinaryEncoder.flush();
    ByteArrayInputStream bis = new ByteArrayInputStream(bab.toByteArray(), 0, bab.size());
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(data.getSchema());
    BinaryDecoder directBinaryDecoder = DecoderFactory.get().directBinaryDecoder(bis, null);
    GenericData.Record read = (GenericData.Record) reader.read(null, directBinaryDecoder);
    Assert.assertEquals(data.toString(), read.toString());
  }

}
