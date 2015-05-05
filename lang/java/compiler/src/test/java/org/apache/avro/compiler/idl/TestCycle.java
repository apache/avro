/*
 * Copyright 2015 The Apache Software Foundation.
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
package org.apache.avro.compiler.idl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SchemaResolver;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.ExtendedGenericDatumReader;
import org.apache.avro.generic.ExtendedGenericDatumWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.ExtendedJsonDecoder;
import org.apache.avro.io.ExtendedJsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

/**
 *
 * @author zoly
 */
public class TestCycle {

    @Test
    public void testCycleGeneration() throws ParseException, IOException {
        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Idl idl = new Idl(cl.getResourceAsStream("cycle/cycle.avdl"),
                "UTF-8");
        Protocol protocol = idl.CompilationUnit();
        String json = protocol.toString();
        System.out.println(json);

        SpecificCompiler compiler = new SpecificCompiler(protocol);
        compiler.setStringType(GenericData.StringType.String);
        File output = new File("./target");
        SpecificCompiler.SCHEMA_OUTPUT_DIR.set(output);
        compiler.compileToDestination(null, output);

        Map<String, Schema> schemas = new HashMap<String, Schema>();
        for (Schema schema : protocol.getTypes()) {
            final String name = schema.getName();
            schemas.put(name, schema);
        }

        GenericRecordBuilder rb2 = new GenericRecordBuilder(schemas.get("SampleNode"));
        rb2.set("count", 10);
        rb2.set("subNodes", Collections.EMPTY_LIST);
        GenericData.Record node = rb2.build();

        GenericRecordBuilder mb = new GenericRecordBuilder(schemas.get("Method"));
        mb.set("declaringClass", "Test");
        mb.set("methodName", "test");
        GenericData.Record method = mb.build();

        GenericRecordBuilder spb = new GenericRecordBuilder(schemas.get("SamplePair"));
        spb.set("method", method);
        spb.set("node", node);
        GenericData.Record sp = spb.build();


        GenericRecordBuilder rb = new GenericRecordBuilder(schemas.get("SampleNode"));
        rb.set("count", 10);
        rb.set("subNodes", Arrays.asList(sp));
        GenericData.Record record = rb.build();

        serDeserRecord(record);
        serDeserRecordJson(record);

    }

    private static void serDeserRecord(GenericData.Record data) throws IOException {
        ByteArrayOutputStream bab = new ByteArrayOutputStream();
        GenericDatumWriter writer = new GenericDatumWriter(data.getSchema());
        final BinaryEncoder directBinaryEncoder = EncoderFactory.get().directBinaryEncoder(bab, null);
        writer.write(data, directBinaryEncoder);
        directBinaryEncoder.flush();
        ByteArrayInputStream bis = new ByteArrayInputStream(bab.toByteArray(), 0, bab.size());
        GenericDatumReader reader = new GenericDatumReader(data.getSchema());
        BinaryDecoder directBinaryDecoder = DecoderFactory.get().directBinaryDecoder(bis, null);
        GenericData.Record read = (GenericData.Record) reader.read(null, directBinaryDecoder);
        Assert.assertEquals(data.toString(), read.toString());
    }

    private static void serDeserRecordJson(GenericData.Record data) throws IOException {
        ByteArrayOutputStream bab = new ByteArrayOutputStream();
        GenericDatumWriter writer = new ExtendedGenericDatumWriter(data.getSchema());
        final Encoder encoder = new ExtendedJsonEncoder(data.getSchema(), bab);
        writer.write(data, encoder);
        encoder.flush();
        ByteArrayInputStream bis = new ByteArrayInputStream(bab.toByteArray(), 0, bab.size());
        ExtendedGenericDatumReader reader = new ExtendedGenericDatumReader(data.getSchema());
        ExtendedJsonDecoder directBinaryDecoder = new ExtendedJsonDecoder(data.getSchema(), bis, reader);
        GenericData.Record read = (GenericData.Record) reader.read(null, directBinaryDecoder);
        Assert.assertEquals(data.toString(), read.toString());
    }

}
