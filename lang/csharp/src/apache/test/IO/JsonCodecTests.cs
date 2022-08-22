/**
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

using System;
using NUnit.Framework;
using System.IO;
using System.Linq;
using System.Text;
using Avro.Generic;
using Avro.IO;
using Newtonsoft.Json.Linq;

namespace Avro.Test
{
    using Decoder = Avro.IO.Decoder;
    using Encoder = Avro.IO.Encoder;

    /// <summary>
    /// Tests the JsonEncoder and JsonDecoder.
    /// </summary>
    [TestFixture]
    public class JsonCodecTests
    {
        [TestCase]
        public void TestJsonEncoderWhenIncludeNamespaceOptionIsFalse()
        {
            string value = "{\"b\": {\"string\":\"myVal\"}, \"a\": 1}";
            string schemaStr = "{\"type\": \"record\", \"name\": \"ab\", \"fields\": [" +
                               "{\"name\": \"a\", \"type\": \"int\"}, {\"name\": \"b\", \"type\": [\"null\", \"string\"]}" +
                               "]}";
            Schema schema = Schema.Parse(schemaStr);
            byte[] avroBytes = fromJsonToAvro(value, schema);

            Assert.IsTrue(JToken.DeepEquals(JObject.Parse("{\"b\":\"myVal\",\"a\":1}"),
                JObject.Parse(fromAvroToJson(avroBytes, schema, false))));
        }

        [TestCase]
        public void TestJsonEncoderWhenIncludeNamespaceOptionIsTrue()
        {
            string value = "{\"b\": {\"string\":\"myVal\"}, \"a\": 1}";
            string schemaStr = "{\"type\": \"record\", \"name\": \"ab\", \"fields\": [" +
                               "{\"name\": \"a\", \"type\": \"int\"}, {\"name\": \"b\", \"type\": [\"null\", \"string\"]}" +
                               "]}";
            Schema schema = Schema.Parse(schemaStr);
            byte[] avroBytes = fromJsonToAvro(value, schema);

            Assert.IsTrue(JToken.DeepEquals(JObject.Parse("{\"b\":{\"string\":\"myVal\"},\"a\":1}"),
                JObject.Parse(fromAvroToJson(avroBytes, schema, true))));
        }

        [TestCase]
        public void TestJsonRecordOrdering()
        {
            string value = "{\"b\": 2, \"a\": 1}";
            Schema schema = Schema.Parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [" +
                                         "{\"name\": \"a\", \"type\": \"int\"}, {\"name\": \"b\", \"type\": \"int\"}" +
                                         "]}");
            GenericDatumReader<object> reader = new GenericDatumReader<object>(schema, schema);
            Decoder decoder = new JsonDecoder(schema, value);
            object o = reader.Read(null, decoder);

            Assert.AreEqual("{\"a\":1,\"b\":2}", fromDatumToJson(o, schema, false));
        }

        [TestCase]
        public void TestJsonRecordOrdering2()
        {
            string value = "{\"b\": { \"b3\": 1.4, \"b2\": 3.14, \"b1\": \"h\"}, \"a\": {\"a2\":true, \"a1\": null}}";
            Schema schema = Schema.Parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [\n" +
                                         "{\"name\": \"a\", \"type\": {\"type\":\"record\",\"name\":\"A\",\"fields\":\n" +
                                         "[{\"name\":\"a1\", \"type\":\"null\"}, {\"name\":\"a2\", \"type\":\"boolean\"}]}},\n" +
                                         "{\"name\": \"b\", \"type\": {\"type\":\"record\",\"name\":\"B\",\"fields\":\n" +
                                         "[{\"name\":\"b1\", \"type\":\"string\"}, {\"name\":\"b2\", \"type\":\"float\"}, {\"name\":\"b3\", \"type\":\"double\"}]}}\n" +
                                         "]}");
            GenericDatumReader<object> reader = new GenericDatumReader<object>(schema, schema);
            Decoder decoder = new JsonDecoder(schema, value);
            object o = reader.Read(null, decoder);

            Assert.AreEqual("{\"a\":{\"a1\":null,\"a2\":true},\"b\":{\"b1\":\"h\",\"b2\":3.14,\"b3\":1.4}}",
                fromDatumToJson(o, schema, false));
        }

        [TestCase]
        public void TestJsonRecordOrderingWithProjection()
        {
            String value = "{\"b\": { \"b3\": 1.4, \"b2\": 3.14, \"b1\": \"h\"}, \"a\": {\"a2\":true, \"a1\": null}}";
            Schema writerSchema = Schema.Parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [\n"
                                               + "{\"name\": \"a\", \"type\": {\"type\":\"record\",\"name\":\"A\",\"fields\":\n"
                                               + "[{\"name\":\"a1\", \"type\":\"null\"}, {\"name\":\"a2\", \"type\":\"boolean\"}]}},\n"
                                               + "{\"name\": \"b\", \"type\": {\"type\":\"record\",\"name\":\"B\",\"fields\":\n"
                                               + "[{\"name\":\"b1\", \"type\":\"string\"}, {\"name\":\"b2\", \"type\":\"float\"}, {\"name\":\"b3\", \"type\":\"double\"}]}}\n"
                                               + "]}");
            Schema readerSchema = Schema.Parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [\n"
                                               + "{\"name\": \"a\", \"type\": {\"type\":\"record\",\"name\":\"A\",\"fields\":\n"
                                               + "[{\"name\":\"a1\", \"type\":\"null\"}, {\"name\":\"a2\", \"type\":\"boolean\"}]}}\n" +
                                               "]}");
            GenericDatumReader<object> reader = new GenericDatumReader<object>(writerSchema, readerSchema);
            Decoder decoder = new JsonDecoder(writerSchema, value);
            Object o = reader.Read(null, decoder);

            Assert.AreEqual("{\"a\":{\"a1\":null,\"a2\":true}}",
                fromDatumToJson(o, readerSchema, false));
        }


        [TestCase]
        public void TestJsonRecordOrderingWithProjection2()
        {
            String value =
                "{\"b\": { \"b1\": \"h\", \"b2\": [3.14, 3.56], \"b3\": 1.4}, \"a\": {\"a2\":true, \"a1\": null}}";
            Schema writerSchema = Schema.Parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [\n"
                                               + "{\"name\": \"a\", \"type\": {\"type\":\"record\",\"name\":\"A\",\"fields\":\n"
                                               + "[{\"name\":\"a1\", \"type\":\"null\"}, {\"name\":\"a2\", \"type\":\"boolean\"}]}},\n"
                                               + "{\"name\": \"b\", \"type\": {\"type\":\"record\",\"name\":\"B\",\"fields\":\n"
                                               + "[{\"name\":\"b1\", \"type\":\"string\"}, {\"name\":\"b2\", \"type\":{\"type\":\"array\", \"items\":\"float\"}}, {\"name\":\"b3\", \"type\":\"double\"}]}}\n"
                                               + "]}");

            Schema readerSchema = Schema.Parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [\n"
                                               + "{\"name\": \"a\", \"type\": {\"type\":\"record\",\"name\":\"A\",\"fields\":\n"
                                               + "[{\"name\":\"a1\", \"type\":\"null\"}, {\"name\":\"a2\", \"type\":\"boolean\"}]}}\n" +
                                               "]}");

            GenericDatumReader<object> reader = new GenericDatumReader<object>(writerSchema, readerSchema);
            Decoder decoder = new JsonDecoder(writerSchema, value);
            object o = reader.Read(null, decoder);

            Assert.AreEqual("{\"a\":{\"a1\":null,\"a2\":true}}",
                fromDatumToJson(o, readerSchema, false));
        }

        [TestCase("{\"int\":123}")]
        [TestCase("{\"long\":456}")]
        [TestCase("null")]
        public void TestJsonUnionWithLogicalTypes(String value)
        {
            Schema schema = Schema.Parse(
                "[\"null\",\n" +
                "    { \"type\": \"int\", \"logicalType\": \"date\" },\n" +
                "    { \"type\": \"long\" }\n" +
                // The following does not work due to https://issues.apache.org/jira/browse/AVRO-3613
                //"    { \"type\": \"long\", \"logicalType\": \"timestamp-millis\" }\n" +
                "]");
            GenericDatumReader<object> reader = new GenericDatumReader<object>(schema, schema);
            Decoder decoder = new JsonDecoder(schema, value);
            object o = reader.Read(null, decoder);

            Assert.AreEqual(value, fromDatumToJson(o, schema, true));
        }

        [TestCase("int", 1)]
        [TestCase("long", 1L)]
        [TestCase("float", 1.0F)]
        [TestCase("double", 1.0)]
        public void TestJsonDecoderNumeric(string type, object value)
        {
            string def = "{\"type\":\"record\",\"name\":\"X\",\"fields\":" + "[{\"type\":\"" + type +
                         "\",\"name\":\"n\"}]}";
            Schema schema = Schema.Parse(def);
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema, schema);

            string[] records = { "{\"n\":1}", "{\"n\":1.0}" };

            foreach (GenericRecord g in records.Select(r => reader.Read(null, new JsonDecoder(schema, r))))
            {
                Assert.AreEqual(value, g["n"]);
            }
        }

        // Ensure that even if the order of fields in JSON is different from the order in schema, it works.
        [TestCase]
        public void TestJsonDecoderReorderFields()
        {
            String w = "{\"type\":\"record\",\"name\":\"R\",\"fields\":" + "[{\"type\":\"long\",\"name\":\"l\"},"
                                                                         + "{\"type\":{\"type\":\"array\",\"items\":\"int\"},\"name\":\"a\"}" +
                                                                         "]}";
            Schema ws = Schema.Parse(w);
            String data = "{\"a\":[1,2],\"l\":100}";
            JsonDecoder decoder = new JsonDecoder(ws, data);
            Assert.AreEqual(100, decoder.ReadLong());
            decoder.SkipArray();
            data = "{\"l\": 200, \"a\":[1,2]}";
            decoder = new JsonDecoder(ws, data);
            Assert.AreEqual(200, decoder.ReadLong());
            decoder.SkipArray();
        }

        private byte[] fromJsonToAvro(string json, Schema schema)
        {
            DatumReader<object> reader = new GenericDatumReader<object>(schema, schema);
            GenericDatumWriter<object> writer = new GenericDatumWriter<object>(schema);
            MemoryStream output = new MemoryStream();

            Decoder decoder = new JsonDecoder(schema, json);
            Encoder encoder = new BinaryEncoder(output);

            object datum = reader.Read(null, decoder);

            writer.Write(datum, encoder);
            encoder.Flush();
            output.Flush();

            return output.ToArray();
        }

        private string fromAvroToJson(byte[] avroBytes, Schema schema, bool includeNamespace)
        {
            GenericDatumReader<object> reader = new GenericDatumReader<object>(schema, schema);

            Decoder decoder = new BinaryDecoder(new MemoryStream(avroBytes));
            object datum = reader.Read(null, decoder);
            return fromDatumToJson(datum, schema, includeNamespace);
        }

        private string fromDatumToJson(object datum, Schema schema, bool includeNamespace)
        {
            DatumWriter<object> writer = new GenericDatumWriter<object>(schema);
            MemoryStream output = new MemoryStream();

            JsonEncoder encoder = new JsonEncoder(schema, output);
            encoder.IncludeNamespace = includeNamespace;
            writer.Write(datum, encoder);
            encoder.Flush();
            output.Flush();

            return Encoding.UTF8.GetString(output.ToArray());
        }
    }
}
