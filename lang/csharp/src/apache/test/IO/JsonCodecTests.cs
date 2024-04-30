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
using System.Collections.Generic;
using NUnit.Framework;
using System.IO;
using System.Linq;
using System.Text;
using Avro.Generic;
using Avro.IO;
using Avro.Specific;
using Newtonsoft.Json;
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
        [TestCase("{ \"type\": \"record\", \"name\": \"r\", \"fields\": [ " +
                  " { \"name\" : \"f1\", \"type\": \"int\" }, " +
                  " { \"name\" : \"f2\", \"type\": \"float\" } " +
                  "] }",
            "{ \"f2\": 10.4, \"f1\": 10 } ")]
        [TestCase("{ \"type\": \"enum\", \"name\": \"e\", \"symbols\": [ \"s1\", \"s2\"] }", " \"s1\" ")]
        [TestCase("{ \"type\": \"enum\", \"name\": \"e\", \"symbols\": [ \"s1\", \"s2\"] }", " \"s2\" ")]
        [TestCase("{ \"type\": \"array\", \"items\": \"int\" }", "[ 10, 20, 30 ]")]
        [TestCase("{ \"type\": \"map\", \"values\": \"int\" }", "{ \"k1\": 10, \"k2\": 20, \"k3\": 30 }")]
        [TestCase("\"string\"", "\"hello\"")]
        [TestCase("\"int\"", "10")]
        [TestCase("\"long\"", "10")]
        [TestCase("\"float\"", "10.0")]
        [TestCase("\"double\"", "10.0")]
        [TestCase("\"boolean\"", "true")]
        [TestCase("\"boolean\"", "false")]
        [TestCase("\"null\"", "null")]
        public void TestJsonAllTypesValidValues(String schemaStr, String value)
        {
            foreach (JsonMode mode in new[] { JsonMode.AvroJson, JsonMode.PlainJson })
            {
                Schema schema = Schema.Parse(schemaStr);
                byte[] avroBytes = fromJsonToAvro(value, schema, mode);

                Assert.IsTrue(JToken.DeepEquals(JToken.Parse(value),
                    JToken.Parse(fromAvroToJson(avroBytes, schema, true, mode))));
            }
        }

        [TestCase("[ \"int\", \"long\" ]", "{ \"int\": 10 }")]
        [TestCase("[ \"int\", \"long\" ]", "{ \"long\": 10 }")]
        [TestCase("[ \"int\", \"null\" ]", "null")]
        [TestCase("\"bytes\"", "\"\\u0068\\u0065\\u006C\\u006C\\u006F\"")]
        [TestCase("{ \"type\": \"fixed\", \"name\": \"f\", \"size\": 5 }", "\"\\u0068\\u0065\\u006C\\u006C\\u006F\"")]
        public void TestJsonAllTypesValidValuesAvroJson(String schemaStr, String value)
        {
            Schema schema = Schema.Parse(schemaStr);
            byte[] avroBytes = fromJsonToAvro(value, schema, JsonMode.AvroJson);

            Assert.IsTrue(JToken.DeepEquals(JToken.Parse(value),
                JToken.Parse(fromAvroToJson(avroBytes, schema, true, JsonMode.AvroJson))));
        }

        [TestCase("[ \"int\", \"long\" ]", "10")]
        [TestCase("[ \"int\", \"null\" ]", "10")]
        [TestCase("[ \"int\", \"null\" ]", "null")]
        [TestCase("\"bytes\"", "\"aGVsbG8=\"")]
        [TestCase("{ \"type\": \"fixed\", \"name\": \"f\", \"size\": 5 }", "\"aGVsbG8=\"")]

        public void TestJsonAllTypesValidValuesPlainJson(String schemaStr, String value)
        {
            Schema schema = Schema.Parse(schemaStr);
            byte[] avroBytes = fromJsonToAvro(value, schema, JsonMode.PlainJson);

            Assert.IsTrue(JToken.DeepEquals(JToken.Parse(value),
                JToken.Parse(fromAvroToJson(avroBytes, schema, true, JsonMode.PlainJson))));
        }



        [TestCase("{ \"type\": \"record\", \"name\": \"r\", \"fields\": [ " +
                  " { \"name\" : \"f1\", \"type\": \"int\" }, " +
                  " { \"name\" : \"f2\", \"type\": \"float\" } " +
                  "] }",
            "{ \"f4\": 10.4, \"f3\": 10 } ")]
        [TestCase("{ \"type\": \"enum\", \"name\": \"e\", \"symbols\": [ \"s1\", \"s2\"] }", " \"s3\" ")]
        [TestCase("{ \"type\": \"array\", \"items\": \"int\" }", "[ \"10\", \"20\", \"30\" ]")]
        [TestCase("{ \"type\": \"map\", \"values\": \"int\" }", "{ \"k1\": \"10\", \"k2\": \"20\"}")]
        [TestCase("\"string\"", "10")]
        [TestCase("\"int\"", "\"hi\"")]
        [TestCase("\"long\"", "\"hi\"")]
        [TestCase("\"float\"", "\"hi\"")]
        [TestCase("\"double\"", "\"hi\"")]
        [TestCase("\"boolean\"", "\"hi\"")]
        [TestCase("\"boolean\"", "\"hi\"")]
        [TestCase("\"null\"", "\"hi\"")]
        public void TestJsonAllTypesInvalidValues(String schemaStr, String value)
        {
            foreach (JsonMode mode in new[] { JsonMode.AvroJson, JsonMode.PlainJson })
            {
                Schema schema = Schema.Parse(schemaStr);
                Assert.Throws<AvroTypeException>(() => fromJsonToAvro(value, schema, mode));
            }
        }

        [TestCase("[ \"int\", \"long\" ]", "10")]
        [TestCase("\"bytes\"", "10")]
        [TestCase("{ \"type\": \"fixed\", \"name\": \"f\", \"size\": 10 }", "\"hello\"")]
        public void TestJsonAllTypesInvalidValuesAvroJson(String schemaStr, String value)
        {
            Schema schema = Schema.Parse(schemaStr);
            Assert.Throws<AvroTypeException>(() => fromJsonToAvro(value, schema, JsonMode.AvroJson));
        }

        [TestCase("[ \"int\", \"long\" ]", "{ \"int\": 10}")]
        [TestCase("\"bytes\"", "10")]
        [TestCase("\"bytes\"", "\"&10\"")]
        [TestCase("{ \"type\": \"fixed\", \"name\": \"f\", \"size\": 10 }", "\"129837\"")]
        [TestCase("{ \"type\": \"fixed\", \"name\": \"f\", \"size\": 10 }", "\"abc&\"")]
        public void TestJsonAllTypesInvalidValuesPlainJson(String schemaStr, String value)
        {
            Schema schema = Schema.Parse(schemaStr);
            Assert.Throws<AvroTypeException>(() => fromJsonToAvro(value, schema, JsonMode.PlainJson));
        }

        [TestCase("{ \"type\": \"record\", \"name\": \"r\", \"fields\": [ " +
                  " { \"name\" : \"f1\", \"type\": \"int\" }, " +
                  " { \"name\" : \"f2\", \"type\": \"float\" } " +
                  "] }",
            "{ \"f2\": 10.4, \"f1")]
        [TestCase("{ \"type\": \"enum\", \"name\": \"e\", \"symbols\": [ \"s1\", \"s2\"] }", "s1")]
        [TestCase("\"string\"", "\"hi")]
        public void TestJsonMalformed(String schemaStr, String value)
        {
            foreach (JsonMode mode in new[] { JsonMode.AvroJson, JsonMode.PlainJson })
            {
                Schema schema = Schema.Parse(schemaStr);
                Assert.Throws<JsonReaderException>(() => fromJsonToAvro(value, schema, mode));
            }
        }

        [Test]
        public void TestJsonEncoderWhenIncludeNamespaceOptionIsFalse()
        {
            foreach (JsonMode mode in new[] { JsonMode.AvroJson, JsonMode.PlainJson })
            {
                string value = (mode == JsonMode.AvroJson)
                    ? "{\"b\": {\"string\":\"myVal\"}, \"a\": 1}"
                    : "{\"b\": \"myVal\", \"a\": 1}";

                string schemaStr = "{\"type\": \"record\", \"name\": \"ab\", \"fields\": [" +
                                   "{\"name\": \"a\", \"type\": \"int\"}, {\"name\": \"b\", \"type\": [\"null\", \"string\"]}" +
                                   "]}";
                Schema schema = Schema.Parse(schemaStr);
                byte[] avroBytes = fromJsonToAvro(value, schema, mode);

                Assert.IsTrue(JToken.DeepEquals(JObject.Parse("{\"b\":\"myVal\",\"a\":1}"),
                    JObject.Parse(fromAvroToJson(avroBytes, schema, false, mode))));
            }
        }

        [Test]
        public void TestJsonEncoderWhenIncludeNamespaceOptionIsTrue()
        {
            foreach (JsonMode mode in new[] { JsonMode.AvroJson, JsonMode.PlainJson })
            {
                string value = (mode == JsonMode.AvroJson)
                    ? "{\"b\": {\"string\":\"myVal\"}, \"a\": 1}"
                    : "{\"b\": \"myVal\", \"a\": 1}";

                string schemaStr = "{\"type\": \"record\", \"name\": \"ab\", \"fields\": [" +
                                   "{\"name\": \"a\", \"type\": \"int\"}, {\"name\": \"b\", \"type\": [\"null\", \"string\"]}" +
                                   "]}";
                Schema schema = Schema.Parse(schemaStr);
                byte[] avroBytes = fromJsonToAvro(value, schema, mode);

                Assert.IsTrue(JToken.DeepEquals(JObject.Parse(value),
                    JObject.Parse(fromAvroToJson(avroBytes, schema, true, mode))));
            }
        }

        [Test]
        public void TestJsonRecordOrdering()
        {
            foreach (JsonMode mode in new[] { JsonMode.AvroJson, JsonMode.PlainJson })
            {
                string value = "{\"b\": 2, \"a\": 1}";
                Schema schema = Schema.Parse("{\"type\": \"record\", \"name\": \"ab\", \"fields\": [" +
                                             "{\"name\": \"a\", \"type\": \"int\"}, {\"name\": \"b\", \"type\": \"int\"}" +
                                             "]}");
                GenericDatumReader<object> reader = new GenericDatumReader<object>(schema, schema);
                Decoder decoder = new JsonDecoder(schema, value);
                object o = reader.Read(null, decoder);

                Assert.AreEqual("{\"a\":1,\"b\":2}", fromDatumToJson(o, schema, false, mode));
            }
        }

        [Test]
        public void TestJsonRecordOrdering2()
        {
            foreach (JsonMode mode in new[] { JsonMode.AvroJson, JsonMode.PlainJson })
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
                    fromDatumToJson(o, schema, false, mode));
            }
        }

        [Test]
        public void TestJsonRecordOrderingWithProjection()
        {
            foreach (JsonMode mode in new[] { JsonMode.AvroJson, JsonMode.PlainJson })
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
                    fromDatumToJson(o, readerSchema, false, mode));
            }
        }


        [Test]
        public void TestJsonRecordOrderingWithProjection2()
        {
            foreach (JsonMode mode in new[] { JsonMode.AvroJson, JsonMode.PlainJson })
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
                    fromDatumToJson(o, readerSchema, false, mode));
            }
        }

        [TestCase("{\"int\":123}")]
        [TestCase("{\"string\":\"12345678-1234-5678-1234-123456789012\"}")]
        [TestCase("null")]
        public void TestJsonUnionWithLogicalTypes(String value)
        {
            Schema schema = Schema.Parse(
                "[\"null\",\n" +
                "    { \"type\": \"int\", \"logicalType\": \"date\" },\n" +
                "    { \"type\": \"string\", \"logicalType\": \"uuid\" }\n" +
                "]");
            GenericDatumReader<object> reader = new GenericDatumReader<object>(schema, schema);
            Decoder decoder = new JsonDecoder(schema, value);
            object o = reader.Read(null, decoder);

            Assert.AreEqual(value, fromDatumToJson(o, schema, true, JsonMode.AvroJson));
        }

        [TestCase("\"2024-05-01\"")]
        [TestCase("\"12345678-1234-5678-1234-123456789012\"")]
        [TestCase("null")]
        public void TestJsonUnionWithLogicalTypesPlainJson(String value)
        {
            Schema schema = Schema.Parse(
                "[\"null\",\n" +
                "    { \"type\": \"int\", \"logicalType\": \"date\" },\n" +
                "    { \"type\": \"string\", \"logicalType\": \"uuid\" }\n" +
                "]");
            GenericDatumReader<object> reader = new GenericDatumReader<object>(schema, schema);
            Decoder decoder = new JsonDecoder(schema, value, JsonMode.PlainJson);
            object o = reader.Read(null, decoder);

            Assert.AreEqual(value, fromDatumToJson(o, schema, true, JsonMode.PlainJson));
        }

        [TestCase("{\"int\":123}")]
        [TestCase("{\"com.myrecord\":{\"f1\":123}}")]
        [TestCase("null")]
        public void TestJsonUnionWithRecord(String value)
        {
            Schema schema = Schema.Parse(
                "[\"null\",\n" +
                "    { \"type\": \"int\", \"logicalType\": \"date\" },\n" +
                "    {\"type\":\"record\",\"name\":\"myrecord\", \"namespace\":\"com\"," +
                "        \"fields\":[{\"name\":\"f1\",\"type\": \"int\"}]}" +
                "]");
            GenericDatumReader<object> reader = new GenericDatumReader<object>(schema, schema);
            Decoder decoder = new JsonDecoder(schema, value);
            object o = reader.Read(null, decoder);

            Assert.AreEqual(value, fromDatumToJson(o, schema, true, JsonMode.AvroJson));
        }

        [TestCase("\"2024-05-01\"")]
        [TestCase("{\"f1\":123}")]
        [TestCase("null")]
        public void TestJsonUnionWithRecordPlainJson(String value)
        {
            Schema schema = Schema.Parse(
                "[\"null\",\n" +
                "    { \"type\": \"int\", \"logicalType\": \"date\" },\n" +
                "    {\"type\":\"record\",\"name\":\"myrecord\", \"namespace\":\"com\"," +
                "        \"fields\":[{\"name\":\"f1\",\"type\": \"int\"}]}" +
                "]");
            GenericDatumReader<object> reader = new GenericDatumReader<object>(schema, schema);
            Decoder decoder = new JsonDecoder(schema, value, JsonMode.PlainJson);
            object o = reader.Read(null, decoder);

            Assert.AreEqual(value, fromDatumToJson(o, schema, true, JsonMode.PlainJson));
        }

        
        [TestCase("{\"f1\":123}")]
        [TestCase("{\"f1\":\"abc\"}")]
        public void TestJsonRecordUnionPlainJson(String value)
        {
            Schema schema = Schema.Parse(
                "[" +
                "    {\"type\":\"record\",\"name\":\"myrecord1\", \"namespace\":\"com\"," +
                "        \"fields\":[{\"name\":\"f1\",\"type\": \"int\"}]}," +
                "    {\"type\":\"record\",\"name\":\"myrecord2\", \"namespace\":\"com\"," +
                "        \"fields\":[{\"name\":\"f1\",\"type\": \"string\"}]}" +
                "]");
            GenericDatumReader<object> reader = new GenericDatumReader<object>(schema, schema);
            Decoder decoder = new JsonDecoder(schema, value, JsonMode.PlainJson);
            object o = reader.Read(null, decoder);

            Assert.AreEqual(value, fromDatumToJson(o, schema, true, JsonMode.PlainJson));
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

        [Test]
        public void TestJsonDecoderDecimalPlainJson()
        {
            decimal value = 1.0M;
            string def = "{\"type\":\"record\",\"name\":\"X\",\"fields\": [{\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":16},\"name\":\"n\"}]}";
            Schema schema = Schema.Parse(def);
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema, schema);

            string[] records = { "{\"n\":1}", "{\"n\":1.0}" };

            foreach (GenericRecord g in records.Select(r => reader.Read(null, new JsonDecoder(schema, r, JsonMode.PlainJson))))
            {
                decimal d = (decimal)(AvroDecimal)g["n"];
                Assert.AreEqual(value, d);
            }
        }

        // Ensure that even if the order of fields in JSON is different from the order in schema, it works.
        [Test]
        public void TestJsonDecoderReorderFields()
        {
            foreach (JsonMode mode in new[] { JsonMode.AvroJson, JsonMode.PlainJson })
            {
                String w = "{\"type\":\"record\",\"name\":\"R\",\"fields\":" + "[{\"type\":\"long\",\"name\":\"l\"},"
                                                                         + "{\"type\":{\"type\":\"array\",\"items\":\"int\"},\"name\":\"a\"}" +
                                                                         "]}";
                Schema ws = Schema.Parse(w);
                String data = "{\"a\":[1,2],\"l\":100}";
                JsonDecoder decoder = new JsonDecoder(ws, data, mode);
                Assert.AreEqual(100, decoder.ReadLong());
                decoder.SkipArray();
                data = "{\"l\": 200, \"a\":[1,2]}";
                decoder = new JsonDecoder(ws, data, mode);
                Assert.AreEqual(200, decoder.ReadLong());
                decoder.SkipArray();
            }
        }

        [Test]
        public void TestJsonDecoderSpecificDatumWriterWithArrayAndMap()
        {
            foreach (JsonMode mode in new[] { JsonMode.AvroJson, JsonMode.PlainJson })
            {
                Root data = new Root();
                Item item = new Item { id = 123456 };
                data.myarray = new List<Item> { item };
                data.mymap = new Dictionary<string, int> { { "1", 1 }, { "2", 2 }, { "3", 3 }, { "4", 4 } };

                DatumWriter<Root> writer = new SpecificDatumWriter<Root>(data.Schema);

                ByteBufferOutputStream bbos = new ByteBufferOutputStream();

                Encoder encoder = new JsonEncoder(data.Schema, bbos, mode);
                writer.Write(data, encoder);
                encoder.Flush();

                List<MemoryStream> listStreams = bbos.GetBufferList();

                using (StreamReader reader = new StreamReader(listStreams[0]))
                {
                    String output = reader.ReadToEnd();
                    if ( mode == JsonMode.AvroJson)
                    {
                        Assert.AreEqual("{\"myarray\":[{\"id\":123456}],\"mymap\":{\"map\":{\"1\":1,\"2\":2,\"3\":3,\"4\":4}}}", output);
                    }
                    else
                    {
                        Assert.AreEqual("{\"myarray\":[{\"id\":123456}],\"mymap\":{\"1\":1,\"2\":2,\"3\":3,\"4\":4}}", output);
                    }   
                }
            }
        }

        [Test]
        public void TestJsonDecoderSpecificDefaultWriterWithArrayAndMap()
        {
            foreach (JsonMode mode in new[] { JsonMode.AvroJson, JsonMode.PlainJson })
            {
                Root data = new Root();
                Item item = new Item { id = 123456 };
                data.myarray = new List<Item> { item };
                data.mymap = new Dictionary<string, int> { { "1", 1 }, { "2", 2 }, { "3", 3 }, { "4", 4 } };

                SpecificDefaultWriter writer = new SpecificDefaultWriter(data.Schema);

                ByteBufferOutputStream bbos = new ByteBufferOutputStream();

                Encoder encoder = new JsonEncoder(data.Schema, bbos, mode);
                writer.Write(data, encoder);
                encoder.Flush();

                List<MemoryStream> listStreams = bbos.GetBufferList();

                using (StreamReader reader = new StreamReader(listStreams[0]))
                {
                    String output = reader.ReadToEnd();
                    if (mode == JsonMode.AvroJson)
                    {
                        Assert.AreEqual("{\"myarray\":[{\"id\":123456}],\"mymap\":{\"map\":{\"1\":1,\"2\":2,\"3\":3,\"4\":4}}}", output);
                    }
                    else
                    {
                        Assert.AreEqual("{\"myarray\":[{\"id\":123456}],\"mymap\":{\"1\":1,\"2\":2,\"3\":3,\"4\":4}}", output);
                    }
                }
            }
        }

        private byte[] fromJsonToAvro(string json, Schema schema, JsonMode mode)
        {
            DatumReader<object> reader = new GenericDatumReader<object>(schema, schema);
            GenericDatumWriter<object> writer = new GenericDatumWriter<object>(schema);
            MemoryStream output = new MemoryStream();

            Decoder decoder = new JsonDecoder(schema, json, mode);
            Encoder encoder = new BinaryEncoder(output);

            object datum = reader.Read(null, decoder);

            writer.Write(datum, encoder);
            encoder.Flush();
            output.Flush();

            return output.ToArray();
        }

        private string fromAvroToJson(byte[] avroBytes, Schema schema, bool includeNamespace, JsonMode mode)
        {
            GenericDatumReader<object> reader = new GenericDatumReader<object>(schema, schema);

            Decoder decoder = new BinaryDecoder(new MemoryStream(avroBytes));
            object datum = reader.Read(null, decoder);
            return fromDatumToJson(datum, schema, includeNamespace, mode);
        }

        private string fromDatumToJson(object datum, Schema schema, bool includeNamespace, JsonMode mode)
        {
            DatumWriter<object> writer = new GenericDatumWriter<object>(schema);
            MemoryStream output = new MemoryStream();

            JsonEncoder encoder = new JsonEncoder(schema, output, mode);
            encoder.IncludeNamespace = includeNamespace;
            writer.Write(datum, encoder);
            encoder.Flush();
            output.Flush();

            return Encoding.UTF8.GetString(output.ToArray());
        }
    }

    public partial class Root : global::Avro.Specific.ISpecificRecord
    {
        public static global::Avro.Schema _SCHEMA = global::Avro.Schema.Parse(
            "{\"type\":\"record\",\"name\":\"Root\",\"namespace\":\"Avro.Test\",\"fields\":[{\"name\":\"myarray" +
            "\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"Item\",\"namespace\":\"Avr" +
            "o.Test\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}}},{\"name\":\"mymap\",\"default\":null," +
            "\"type\":[\"null\",{\"type\":\"map\",\"values\":\"int\"}]}]}");
        private IList<Avro.Test.Item> _myarray;
        private IDictionary<string,System.Int32> _mymap;

        public virtual global::Avro.Schema Schema
        {
            get { return Root._SCHEMA; }
        }

        public IList<Avro.Test.Item> myarray
        {
            get { return this._myarray; }
            set { this._myarray = value; }
        }

        public IDictionary<string,System.Int32> mymap
        {
            get { return this._mymap; }
            set { this._mymap = value; }
        }

        public virtual object Get(int fieldPos)
        {
            switch (fieldPos)
            {
                case 0: return this.myarray;
                case 1: return this.mymap;
                default: throw new global::Avro.AvroRuntimeException("Bad index " + fieldPos + " in Get()");
            }
        }

        public virtual void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0:
                    this.myarray = (IList<Avro.Test.Item>)fieldValue;
                    break;
                case 1:
                    this.mymap = (IDictionary<string,System.Int32>)fieldValue;
                    break;
                default: throw new global::Avro.AvroRuntimeException("Bad index " + fieldPos + " in Put()");
            }
        }
    }

    public partial class Item : global::Avro.Specific.ISpecificRecord
    {
        public static global::Avro.Schema _SCHEMA = global::Avro.Schema.Parse(
            "{\"type\":\"record\",\"name\":\"Item\",\"namespace\":\"Avro.Test\",\"fields\":[{\"name\":\"id\",\"ty" +
            "pe\":\"long\"}]}");

        private long _id;

        public virtual global::Avro.Schema Schema
        {
            get { return Item._SCHEMA; }
        }

        public long id
        {
            get { return this._id; }
            set { this._id = value; }
        }

        public virtual object Get(int fieldPos)
        {
            switch (fieldPos)
            {
                case 0: return this.id;
                default: throw new global::Avro.AvroRuntimeException("Bad index " + fieldPos + " in Get()");
            }
        }

        public virtual void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0:
                    this.id = (System.Int64)fieldValue;
                    break;
                default: throw new global::Avro.AvroRuntimeException("Bad index " + fieldPos + " in Put()");
            }
        }
    }
}
