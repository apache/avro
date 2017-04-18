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

        final Schema writerSchema = TestRecordWithLogicalTypes.SCHEMA$;
        final Schema readerSchema = new org.apache.avro.Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"TestRecordWithLogicalTypes\",\"namespace\":\"org.apache.avro.specific\",\"fields\":["
                        + "{\"name\":\"b\",\"type\":\"boolean\"},"
                        + "{\"name\":\"i32\",\"type\":\"int\"},"
                        + "{\"name\":\"i64\",\"type\":\"long\"},"
                        + "{\"name\":\"f32\",\"type\":\"float\"},"
                        + "{\"name\":\"f64\",\"type\":\"double\"},"
                        // field "s" was intentionally left out, since doesn't "exist" on previous version
                        // + "{\"name\":\"s\",\"type\":[\"null\",\"string\"],\"default\":null},"
                        + "{\"name\":\"d\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},"
                        + "{\"name\":\"t\",\"type\":{\"type\":\"int\",\"logicalType\":\"time-millis\"}},"
                        + "{\"name\":\"ts\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},"
                        + "{\"name\":\"dec\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":9,\"scale\":2}}]}");

        final GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(writerSchema,
                readerSchema);
        final GenericData.Record datum = new GenericData.Record(writerSchema);
        final Decoder decoder = DecoderFactory.get().jsonDecoder(writerSchema, new ByteArrayInputStream(json.getBytes()));
        reader.read(datum, decoder);

        assertThat(datum.get("s"), Matchers.nullValue());
    }
}
