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
package org.apache.avro.specific;

import static org.junit.Assert.*;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class TestSpecificFromJson
{
    @Test
    public void testSchemaResolution() throws IOException
    {
        // http://avro.apache.org/docs/current/spec.html#Schema+Resolution
        final String json = "{"
                + "\"b\":true,"
                + "\"i32\":32,"
                + "\"i64\":64,"
                + "\"f32\":3.2,"
                + "\"f64\":6.4,"
                // field "s" was intentionally left out, since it is optional
                + "\"d\":123456,"
                + "\"t\":123456,"
                + "\"ts\":123456"
                + "}";

        final Schema schema = TestRecordWithLogicalTypes.SCHEMA$;

        final GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        final GenericData.Record datum = new GenericData.Record(schema);
        final Decoder decoder = DecoderFactory.get().jsonDecoder(schema, new ByteArrayInputStream(json.getBytes()));
        reader.read(datum, decoder);

        assertThat(datum.get("s"), Matchers.nullValue());
    }
}
