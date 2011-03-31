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
using System;
using System.IO;
using Avro.IO;
using System.Collections.Generic;
using Avro.Generic;
using NUnit.Framework;

namespace Avro.Test.Generic
{
    class GenericTests
    {
        private static void test<T>(string s, T value)
        {
            Stream ms;
            Schema ws;
            serialize(s, value, out ms, out ws);
            Schema rs = Schema.Parse(s);
            T output = deserialize<T>(ms, ws, rs);
            Assert.AreEqual(value, output);
        }

        [TestCase("{\"type\": \"boolean\"}", true)]
        [TestCase("{\"type\": \"boolean\"}", false)]

        // Union
        [TestCase("[\"boolean\", \"null\"]", null)]
        [TestCase("[\"boolean\", \"null\"]", true)]
        [TestCase("[\"int\", \"long\"]", 100)]
        [TestCase("[\"int\", \"long\"]", 100L)]
        [TestCase("[\"float\", \"double\"]", 100.75)]
        [TestCase("[\"float\", \"double\"]", 23.67f)]
        [TestCase("[{\"type\": \"array\", \"items\": \"float\"}, \"double\"]", new float[] { 23.67f, 22.78f })]
        [TestCase("[{\"type\": \"array\", \"items\": \"float\"}, \"double\"]", 100.89)]
        [TestCase("[{\"type\": \"array\", \"items\": \"string\"}, \"string\"]", "a")]
        [TestCase("[{\"type\": \"array\", \"items\": \"string\"}, \"string\"]", new string[] { "a", "b" })]
        [TestCase("[{\"type\": \"array\", \"items\": \"bytes\"}, \"bytes\"]", new byte[] { 1, 2, 3 })]
        [TestCase("[{\"type\": \"array\", \"items\": \"bytes\"}, \"bytes\"]",
            new object[] { new byte[] { 1, 2 }, new byte[] { 3, 4 } })]
        [TestCase("[{\"type\": \"enum\", \"symbols\": [\"s1\", \"s2\"], \"name\": \"e\"}, \"string\"]", "h1")]
        public void TestPrimitive(string schema, object value)
        {
            test(schema, value);
        }

        [TestCase("{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"null\"}]}",
            new object[] { "f1", null })]
        [TestCase("{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"boolean\"}]}",
            new object[] { "f1", true })]
        [TestCase("{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"boolean\"}]}",
            new object[] { "f1", false })]
        [TestCase("{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"int\"}]}",
            new object[] { "f1", 101 })]
        [TestCase("{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"long\"}]}",
            new object[] { "f1", 101L })]
        [TestCase("{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"float\"}]}",
            new object[] { "f1", 101.78f })]
        [TestCase("{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"double\"}]}",
            new object[] { "f1", 101.78 })]
        [TestCase("{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"string\"}]}",
            new object[] { "f1", "A" })]
        [TestCase("{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"bytes\"}]}",
            new object[] { "f1", new byte[] { 0, 1 } })]
        [TestCase("{\"type\":\"record\", \"name\":\"n\", \"fields\":" +
            "[{\"name\":\"f1\", \"type\":{\"type\": \"enum\", \"name\": \"e\", \"symbols\":[\"s1\", \"s2\"]}}]}",
            new object[] { "f1", "s2" })]
        [TestCase("{\"type\":\"record\", \"name\":\"n\", \"fields\":" +
            "[{\"name\":\"f1\", \"type\":{\"type\": \"array\", \"items\": \"int\"}}]}",
            new object[] { "f1", new object[] { 0, 1, 101 } })]
        [TestCase("{\"type\":\"record\", \"name\":\"n\", \"fields\":" +
            "[{\"name\":\"f1\", \"type\":{\"type\": \"array\", \"items\": \"int\"}}]}",
            new object[] { "f1", new int[] { 0, 1, 101 } })]
        [TestCase("{\"type\":\"record\", \"name\":\"n\", \"fields\":" +
            "[{\"name\":\"f1\", \"type\":[\"int\", \"long\"]}]}",
            new object[] { "f1", 100 })]
        [TestCase("{\"type\":\"record\", \"name\":\"n\", \"fields\":" +
            "[{\"name\":\"f1\", \"type\":[\"int\", \"long\"]}]}",
            new object[] { "f1", 100L })]
        [TestCase("{\"type\":\"record\", \"name\":\"n\", \"fields\":" +
            "[{\"name\":\"f1\", \"type\":{\"type\": \"fixed\", \"name\": \"f\", \"size\": 2}}]}",
            new object[] { "f1", new byte[] { 1, 2 } })]
        public void TestRecord(string schema, object[] kv)
        {
            test(schema, mkRecord(kv, Schema.Parse(schema) as RecordSchema));
        }

        [TestCase("{\"type\": \"map\", \"values\": \"string\"}",
            new object[] { "a", "0", "b", "1", "c", "101" })]
        public void TestMap(string schema, object[] values)
        {
            test(schema, mkMap(values));
        }

        [TestCase("[{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"string\"}]}, \"string\"]",
            "{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"string\"}]}",
            new object[] { "f1", "v1" })]
        public void TestUnion_record(string unionSchema, string recordSchema, object[] value)
        {
            test(unionSchema, mkRecord(value, Schema.Parse(recordSchema) as RecordSchema));
        }

        [TestCase("[{\"type\": \"enum\", \"symbols\": [\"s1\", \"s2\"], \"name\": \"e\"}, \"string\"]",
            "{\"type\": \"enum\", \"symbols\": [\"s1\", \"s2\"], \"name\": \"e\"}", "s1")]
        [TestCase("[{\"type\": \"enum\", \"symbols\": [\"s1\", \"s2\"], \"name\": \"e\"}, \"string\"]",
            "{\"type\": \"enum\", \"symbols\": [\"s1\", \"s2\"], \"name\": \"e\"}", "s2")]
        [TestCase("[{\"type\": \"enum\", \"symbols\": [\"s1\", \"s2\"], \"name\": \"e\"}, \"string\"]",
            "{\"type\": \"enum\", \"symbols\": [\"s1\", \"s2\"], \"name\": \"e\"}", "s3",
            ExpectedException = typeof(AvroException))]
        public void TestUnion_enum(string unionSchema, string enumSchema, string value)
        {
            test(unionSchema, mkEnum(enumSchema, value));
        }


        [TestCase("[{\"type\": \"map\", \"values\": \"int\"}, \"string\"]",
            "{\"type\": \"map\", \"values\": \"int\"}", new object[] { "a", 1, "b", 2 })]
        public void TestUnion_map(string unionSchema, string mapSchema, object[] value)
        {
            test(unionSchema, mkMap(value));
        }

        [TestCase("[{\"type\": \"fixed\", \"size\": 2, \"name\": \"f\"}, \"string\"]",
            "{\"type\": \"fixed\", \"size\": 2, \"name\": \"f\"}", new byte[] { 1, 2 })]
        [TestCase("[{\"type\": \"fixed\", \"size\": 2, \"name\": \"f\"}, \"string\"]",
            "{\"type\": \"fixed\", \"size\": 2, \"name\": \"f\"}", new byte[] { 1, 2, 3 },
            ExpectedException = typeof(AvroException))]
        [TestCase("[{\"type\": \"fixed\", \"size\": 2, \"name\": \"f\"}, \"string\"]",
            "{\"type\": \"fixed\", \"size\": 3, \"name\": \"f\"}", new byte[] { 1, 2, 3 },
            ExpectedException = typeof(AvroException))]
        public void TestUnion_fixed(string unionSchema, string fixedSchema, byte[] value)
        {
            test(unionSchema, mkFixed(fixedSchema, value));
        }

        public void TestResolution<T, S>(string writerSchema, T actual, string readerSchema, S expected)
        {
            Stream ms;
            Schema ws;
            serialize<T>(writerSchema, actual, out ms, out ws);
            Schema rs = Schema.Parse(readerSchema);
            S output = deserialize<S>(ms, ws, rs);
            Assert.AreEqual(expected, output);
        }

        [TestCase("int", 10, "long", 10L)]
        [TestCase("int", 10, "float", 10.0f)]
        [TestCase("int", 10, "double", 10.0)]
        [TestCase("long", 10L, "float", 10.0f)]
        [TestCase("long", 10L, "double", 10.0)]
        [TestCase("float", 10.0f, "double", 10.0)]
        [TestCase("{\"type\":\"array\", \"items\":\"int\"}", new int[] { 10, 20 },
            "{\"type\":\"array\", \"items\":\"long\"}", new object[] { 10L, 20L })]
        [TestCase("[\"int\", \"boolean\"]", true, "[\"boolean\", \"double\"]", true)]
        [TestCase("[\"int\", \"boolean\"]", 10, "[\"boolean\", \"double\"]", 10.0)]
        [TestCase("[\"int\", \"boolean\"]", 10, "\"int\"", 10)]
        [TestCase("[\"int\", \"boolean\"]", 10, "\"double\"", 10.0)]
        [TestCase("\"int\"", 10, "[\"int\", \"boolean\"]", 10)]
        [TestCase("\"int\"", 10, "[\"long\", \"boolean\"]", 10L)]
        public void TestResolution_simple(string writerSchema, object actual, string readerSchema, object expected)
        {
            TestResolution(writerSchema, actual, readerSchema, expected);
        }

        [Test]
        public void TestResolution_intMapToLongMap()
        {
            TestResolution("{\"type\":\"map\", \"values\":\"int\"}", mkMap(new object[] { "a", 10, "b", 20 }),
                "{\"type\":\"map\", \"values\":\"long\"}", mkMap(new object[] { "a", 10L, "b", 20L }));
        }

        [Test]
        public void TestResolution_enum()
        {
            string ws = "{\"type\":\"enum\", \"symbols\":[\"a\", \"b\"], \"name\":\"e\"}";
            string rs = "{\"type\":\"enum\", \"symbols\":[\"a\", \"b\"], \"name\":\"e\"}";
            TestResolution(ws, mkEnum(ws, "a"), rs, mkEnum(rs, "a"));
        }

        [TestCase("{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f1\",\"type\":\"boolean\"},{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f1", true, "f2", 100 },
            "{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f2\",\"type\":\"int\"},{\"name\":\"f1\",\"type\":\"boolean\"}]}",
            new object[] { "f1", true, "f2", 100 }, Description = "Out of order fields")]
        [TestCase("{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f1\",\"type\":\"boolean\"},{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f1", true, "f2", 100 },
            "{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f1\",\"type\":\"boolean\"},{\"name\":\"f2\",\"type\":\"long\"}]}",
            new object[] { "f1", true, "f2", 100L }, Description = "Field promotion")]
        [TestCase("{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f1\",\"type\":\"boolean\"},{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f1", true, "f2", 100 },
            "{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f1\",\"type\":\"boolean\"}]}",
            new object[] { "f1", true }, Description = "Missing fields - 1")]
        [TestCase("{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f1\",\"type\":\"null\"},{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f1", null, "f2", 100 },
            "{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f2", 100 }, Description = "Missing fields - null")]
        [TestCase("{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f1\",\"type\":\"boolean\"},{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f1", true, "f2", 100 },
            "{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f2", 100 }, Description = "Missing fields - boolean")]
        [TestCase("{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f1\",\"type\":\"int\"},{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f1", 1, "f2", 100 },
            "{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f2", 100 }, Description = "Missing fields - int")]
        [TestCase("{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f1\",\"type\":\"long\"},{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f1", 1L, "f2", 100 },
            "{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f2", 100 }, Description = "Missing fields - long")]
        [TestCase("{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f1\",\"type\":\"float\"},{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f1", 1.0f, "f2", 100 },
            "{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f2", 100 }, Description = "Missing fields - float")]
        [TestCase("{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f1\",\"type\":\"double\"},{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f1", 1.0, "f2", 100 },
            "{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f2", 100 }, Description = "Missing fields - double")]
        [TestCase("{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f1\",\"type\":\"bytes\"},{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f1", new byte[] { 1 , 0 }, "f2", 100 },
            "{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f2", 100 }, Description = "Missing fields - bytes")]
        [TestCase("{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f1\",\"type\":\"string\"},{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f1", "h", "f2", 100 },
            "{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f2", 100 }, Description = "Missing fields - string")]
        [TestCase("{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f1\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f1", new int[] { 100, 101 }, "f2", 100 },
            "{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f2", 100 }, Description = "Missing fields - array")]
        [TestCase("{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f1\",\"type\":[\"int\", \"null\"]},{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f1", 101, "f2", 100 },
            "{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f2", 100 }, Description = "Missing fields - union")]
        // TODO: Missing fields - record, enum, map, fixed
        /*  FIXME: Resolution using defaults don't work yet.
        [TestCase("{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f1\",\"type\":\"boolean\"}]}",
            new object[] { "f1", true },
            "{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f1\",\"type\":\"boolean\"},{\"name\":\"f2\",\"type\":\"string\",\"default\":\"d\"}]}",
            new object[] { "f1", true, "f2", "d" }, Description = "Default field")]
         */
        public void TestResolution_record(string ws, object[] actual, string rs, object[] expected)
        {
            TestResolution(ws, mkRecord(actual, Schema.Parse(ws) as RecordSchema), rs,
                mkRecord(expected, Schema.Parse(rs) as RecordSchema));
        }

        [TestCase("{\"type\":\"map\",\"values\":\"int\"}", new object[] { "a", 100, "b", -202 },
            "{\"type\":\"map\",\"values\":\"long\"}", new object[] { "a", 100L, "b", -202L })]
        public void TestResolution_intMapToLongMap(string ws, object[] value, string rs, object[] expected)
        {
            TestResolution(ws, mkMap(value), rs, mkMap(expected));
        }


        private static void testResolutionMismatch<T>(string writerSchema, T value, string readerSchema)
        {
            Stream ms;
            Schema ws;
            serialize(writerSchema, value, out ms, out ws);
            deserialize<object>(ms, ws, Schema.Parse(readerSchema));
        }

        [TestCase("boolean", true, "null", ExpectedException = typeof(AvroException))]
        [TestCase("int", 10, "boolean", ExpectedException = typeof(AvroException))]
        [TestCase("int", 10, "string", ExpectedException = typeof(AvroException))]
        [TestCase("int", 10, "bytes", ExpectedException = typeof(AvroException))]
        [TestCase("int", 10, "{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"f\", \"type\":\"int\"}]}",
            ExpectedException = typeof(AvroException))]
        [TestCase("int", 10, "{\"type\":\"enum\",\"name\":\"e\",\"symbols\":[\"s\", \"t\"]}", ExpectedException = typeof(AvroException))]
        [TestCase("int", 10, "{\"type\":\"array\",\"items\":\"int\"}", ExpectedException = typeof(AvroException))]
        [TestCase("int", 10, "{\"type\":\"map\",\"values\":\"int\"}", ExpectedException = typeof(AvroException))]
        [TestCase("int", 10, "[\"string\", \"bytes\"]", ExpectedException = typeof(AvroException))]
        [TestCase("int", 10, "{\"type\":\"fixed\",\"name\":\"f\",\"size\":2}", ExpectedException = typeof(AvroException))]
        [TestCase("{\"type\":\"array\",\"items\":\"int\"}", new int[] { 10 },
            "\"boolean\"", ExpectedException = typeof(AvroException))]
        [TestCase("{\"type\":\"array\",\"items\":\"int\"}", new int[] { 10 },
            "{\"type\":\"array\",\"items\":\"string\"}", ExpectedException = typeof(AvroException))]
        [TestCase("[\"int\", \"boolean\"]", 10, "[\"string\", \"bytes\"]", ExpectedException = typeof(AvroException))]
        [TestCase("[\"int\", \"boolean\"]", 10, "\"string\"", ExpectedException = typeof(AvroException))]
        public void TestResolutionMismatch_simple(string writerSchema, object value, string readerSchema)
        {
            testResolutionMismatch(writerSchema, value, readerSchema);
        }

        [TestCase("{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f1\",\"type\":[\"int\", \"null\"]},{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f1", 101, "f2", 100 }, "int",
            ExpectedException = typeof(AvroException), Description = "Non-record schema")]
        [TestCase("{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f1\",\"type\":[\"int\", \"null\"]},{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f1", 101, "f2", 100 },
            "{\"type\":\"record\",\"name\":\"s\",\"fields\":" +
            "[{\"name\":\"f2\",\"type\":\"int\"}]}",
            ExpectedException = typeof(AvroException), Description = "Name mismatch")]
        [TestCase("{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f1\",\"type\":[\"int\", \"null\"]},{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f1", 101, "f2", 100 },
            "{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f2\",\"type\":\"string\"}]}",
            ExpectedException = typeof(AvroException), Description = "incompatible field")]
        [TestCase("{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f1\",\"type\":[\"int\", \"null\"]},{\"name\":\"f2\",\"type\":\"int\"}]}",
            new object[] { "f1", 101, "f2", 100 },
            "{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f3\",\"type\":\"string\"}]}",
            ExpectedException = typeof(AvroException), Description = "new field without default")]
        public void TestResolutionMismatch_record(string ws, object[] actual, string rs)
        {
            testResolutionMismatch(ws, mkRecord(actual, Schema.Parse(ws) as RecordSchema), rs);
        }

        [TestCase("{\"type\":\"enum\",\"name\":\"e\",\"symbols\":[\"s\", \"t\"]}", "s", "int",
            ExpectedException = typeof(AvroException), Description = "Non-enum schema")]
        [TestCase("{\"type\":\"enum\",\"name\":\"e\",\"symbols\":[\"s\", \"t\"]}",
            "s", "{\"type\":\"enum\",\"name\":\"f\",\"symbols\":[\"s\", \"t\"]}",
            ExpectedException = typeof(AvroException), Description = "Name mismatch")]
        [TestCase("{\"type\":\"enum\",\"name\":\"e\",\"symbols\":[\"s\", \"t\"]}",
            "s", "{\"type\":\"enum\",\"name\":\"f\",\"symbols\":[\"t\", \"u\"]}",
            ExpectedException = typeof(AvroException), Description = "Incompatible symbols")]
        public void TestResolutionMismatch_enum(string ws, string value, string rs)
        {
            testResolutionMismatch(ws, mkEnum(ws, value), rs);
        }

        [TestCase("{\"type\":\"map\",\"values\":\"int\"}", new object[] { "a", 0 }, "int",
            ExpectedException = typeof(AvroException), Description = "Non-map schema")]
        [TestCase("{\"type\":\"map\",\"values\":\"int\"}",
            new object[] { "a", 0 }, "{\"type\":\"map\",\"values\":\"string\"}",
            ExpectedException = typeof(AvroException), Description = "Name mismatch")]
        public void TestResolutionMismatch_map(string ws, object[] value, string rs)
        {
            testResolutionMismatch(ws, mkMap(value), rs);
        }

        [TestCase("{\"type\":\"fixed\",\"name\":\"f\",\"size\":2}", new byte[] { 1, 1 }, "int",
            ExpectedException = typeof(AvroException), Description = "Non-fixed schema")]
        [TestCase("{\"type\":\"fixed\",\"name\":\"f\",\"size\":2}",
            new byte[] { 1, 1 }, "{\"type\":\"fixed\",\"name\":\"g\",\"size\":2}",
            ExpectedException = typeof(AvroException), Description = "Name mismatch")]
        [TestCase("{\"type\":\"fixed\",\"name\":\"f\",\"size\":2}",
            new byte[] { 1, 1 }, "{\"type\":\"fixed\",\"name\":\"f\",\"size\":1}",
            ExpectedException = typeof(AvroException), Description = "Size mismatch")]
        public void TestResolutionMismatch_fixed(string ws, byte[] value, string rs)
        {
            testResolutionMismatch(ws, mkFixed(ws, value), rs);
        }

        private static GenericRecord mkRecord(object[] kv, RecordSchema s)
        {
            GenericRecord input = new GenericRecord(s);
            for (int i = 0; i < kv.Length; i += 2)
            {
                string fieldName = (string)kv[i];
                object fieldValue = kv[i + 1];
                Schema inner = s[fieldName].Schema;
                if (inner is EnumSchema)
                {
                    GenericEnum ge = new GenericEnum(inner as EnumSchema, (string)fieldValue);
                    fieldValue = ge;
                }
                else if (inner is FixedSchema)
                {
                    GenericFixed gf = new GenericFixed(inner as FixedSchema);
                    gf.Value = (byte[])fieldValue;
                    fieldValue = gf;
                }
                input.Add(fieldName, fieldValue);
            }
            return input;
        }

        private static IDictionary<string, object> mkMap(object[] vv)
        {
            IDictionary<string, object> d = new Dictionary<string, object>();
            for (int j = 0; j < vv.Length; j += 2)
            {
                d[(string)vv[j]] = vv[j + 1];
            }
            return d;
        }

        private static object mkEnum(string enumSchema, string value)
        {
            return new GenericEnum(Schema.Parse(enumSchema) as EnumSchema, value);
        }

        private static object mkFixed(string fixedSchema, byte[] value)
        {
            return new GenericFixed(Schema.Parse(fixedSchema) as FixedSchema, value);
        }

        private static S deserialize<S>(Stream ms, Schema ws, Schema rs)
        {
            GenericReader<S> r = new GenericReader<S>(ws, rs);
            Decoder d = new BinaryDecoder(ms);
            S n = default(S);
            S output = r.Read(n, d);
            Assert.AreEqual(ms.Length, ms.Position); // Ensure we have read everything.
            return output;
        }

        private static void serialize<T>(string writerSchema, T actual, out Stream ms, out Schema ws)
        {
            ms = new MemoryStream();
            Encoder e = new BinaryEncoder(ms);
            ws = Schema.Parse(writerSchema);
            GenericWriter<T> w = new GenericWriter<T>(ws);
            w.Write(actual, e);
            ms.Flush();
            ms.Position = 0;
        }

    }
}
