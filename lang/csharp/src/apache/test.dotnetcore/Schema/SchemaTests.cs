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
using System.Collections.Generic;
using System.Text;
using NUnit.Framework;
using Avro;

namespace Avro.Test
{
    [TestFixture]
    public class SchemaTests
    {
        // Primitive types - shorthand
        [TestCase("null")]
        [TestCase("boolean")]
        [TestCase("int")]
        [TestCase("long")]
        [TestCase("float")]
        [TestCase("double")]
        [TestCase("bytes")]
        [TestCase("string")]

        [TestCase("\"null\"")]
        [TestCase("\"boolean\"")]
        [TestCase("\"int\"")]
        [TestCase("\"long\"")]
        [TestCase("\"float\"")]
        [TestCase("\"double\"")]
        [TestCase("\"bytes\"")]
        [TestCase("\"string\"")]

        // Primitive types - longer
        [TestCase("{ \"type\": \"null\" }")]
        [TestCase("{ \"type\": \"boolean\" }")]
        [TestCase("{ \"type\": \"int\" }")]
        [TestCase("{ \"type\": \"long\" }")]
        [TestCase("{ \"type\": \"float\" }")]
        [TestCase("{ \"type\": \"double\" }")]
        [TestCase("{ \"type\": \"bytes\" }")]
        [TestCase("{ \"type\": \"string\" }")]
        // Record
        [TestCase("{\"type\": \"record\",\"name\": \"Test\",\"fields\": [{\"name\": \"f\",\"type\": \"long\"}]}")]
        [TestCase("{\"type\": \"record\",\"name\": \"Test\",\"fields\": " +
            "[{\"name\": \"f1\",\"type\": \"long\"},{\"name\": \"f2\", \"type\": \"int\"}]}")]
        [TestCase("{\"type\": \"error\",\"name\": \"Test\",\"fields\": " +
            "[{\"name\": \"f1\",\"type\": \"long\"},{\"name\": \"f2\", \"type\": \"int\"}]}")]
        [TestCase("{\"type\":\"record\",\"name\":\"LongList\"," +
            "\"fields\":[{\"name\":\"value\",\"type\":\"long\"},{\"name\":\"next\",\"type\":[\"LongList\",\"null\"]}]}")] // Recursive.
        [TestCase("{\"type\":\"record\",\"name\":\"LongList\"," +
            "\"fields\":[{\"name\":\"value\",\"type\":\"long\"},{\"name\":\"next\",\"type\":[\"LongListA\",\"null\"]}]}",
            Description = "Unknown name", ExpectedException = typeof(SchemaParseException))]
        [TestCase("{\"type\":\"record\",\"name\":\"LongList\"}",
            Description = "No fields", ExpectedException = typeof(SchemaParseException))]
        [TestCase("{\"type\":\"record\",\"name\":\"LongList\", \"fields\": \"hi\"}",
            Description = "Fields not an array", ExpectedException = typeof(SchemaParseException))]

        // Enum
        [TestCase("{\"type\": \"enum\", \"name\": \"Test\", \"symbols\": [\"A\", \"B\"]}")]
        [TestCase("{\"type\": \"enum\", \"name\": \"Status\", \"symbols\": \"Normal Caution Critical\"}",
            Description = "Symbols not an array", ExpectedException = typeof(SchemaParseException))]
        [TestCase("{\"type\": \"enum\", \"name\": [ 0, 1, 1, 2, 3, 5, 8 ], \"symbols\": [\"Golden\", \"Mean\"]}",
            Description = "Name not a string", ExpectedException = typeof(SchemaParseException))]
        [TestCase("{\"type\": \"enum\", \"symbols\" : [\"I\", \"will\", \"fail\", \"no\", \"name\"]}",
            Description = "No name", ExpectedException = typeof(SchemaParseException))]
        [TestCase("{\"type\": \"enum\", \"name\": \"Test\", \"symbols\" : [\"AA\", \"AA\"]}",
            Description = "Duplicate symbol", ExpectedException = typeof(SchemaParseException))]

        // Array
        [TestCase("{\"type\": \"array\", \"items\": \"long\"}")]
        [TestCase("{\"type\": \"array\",\"items\": {\"type\": \"enum\", \"name\": \"Test\", \"symbols\": [\"A\", \"B\"]}}")]

        // Map
        [TestCase("{\"type\": \"map\", \"values\": \"long\"}")]
        [TestCase("{\"type\": \"map\",\"values\": {\"type\": \"enum\", \"name\": \"Test\", \"symbols\": [\"A\", \"B\"]}}")]

        // Union
        [TestCase("[\"string\", \"null\", \"long\"]")]
        [TestCase("[\"string\", \"long\", \"long\"]",
            Description = "Duplicate type", ExpectedException = typeof(SchemaParseException))]
        [TestCase("[{\"type\": \"array\", \"items\": \"long\"}, {\"type\": \"array\", \"items\": \"string\"}]",
            Description = "Duplicate type", ExpectedException = typeof(SchemaParseException))]
        [TestCase("{\"type\":[\"string\", \"null\", \"long\"]}")]
        
        // Fixed
        [TestCase("{ \"type\": \"fixed\", \"name\": \"Test\", \"size\": 1}")]
        [TestCase("{\"type\": \"fixed\", \"name\": \"MyFixed\", \"namespace\": \"org.apache.hadoop.avro\", \"size\": 1}")]
        [TestCase("{ \"type\": \"fixed\", \"name\": \"Test\", \"size\": 1}")]
        [TestCase("{ \"type\": \"fixed\", \"name\": \"Test\", \"size\": 1}")]
        [TestCase("{\"type\": \"fixed\", \"name\": \"Missing size\"}", ExpectedException = typeof(SchemaParseException))]
        [TestCase("{\"type\": \"fixed\", \"size\": 314}",
            Description = "No name", ExpectedException = typeof(SchemaParseException))]
        public void TestBasic(string s)
        {
            Schema.Parse(s);
        }

        [TestCase("null", Schema.Type.Null)]
        [TestCase("boolean", Schema.Type.Boolean)]
        [TestCase("int", Schema.Type.Int)]
        [TestCase("long", Schema.Type.Long)]
        [TestCase("float", Schema.Type.Float)]
        [TestCase("double", Schema.Type.Double)]
        [TestCase("bytes", Schema.Type.Bytes)]
        [TestCase("string", Schema.Type.String)]
        
        [TestCase("{ \"type\": \"null\" }", Schema.Type.Null)]
        [TestCase("{ \"type\": \"boolean\" }", Schema.Type.Boolean)]
        [TestCase("{ \"type\": \"int\" }", Schema.Type.Int)]
        [TestCase("{ \"type\": \"long\" }", Schema.Type.Long)]
        [TestCase("{ \"type\": \"float\" }", Schema.Type.Float)]
        [TestCase("{ \"type\": \"double\" }", Schema.Type.Double)]
        [TestCase("{ \"type\": \"bytes\" }", Schema.Type.Bytes)]
        [TestCase("{ \"type\": \"string\" }", Schema.Type.String)]
        public void TestPrimitive(string s, Schema.Type type)
        {
            Schema sc = Schema.Parse(s);
            Assert.IsTrue(sc is PrimitiveSchema);
            Assert.AreEqual(type, sc.Tag);

            testEquality(s, sc);
            testToString(sc);
        }

        private static void testEquality(string s, Schema sc)
        {
            Assert.IsTrue(sc.Equals(sc));
            Schema sc2 = Schema.Parse(s);
            Assert.IsTrue(sc.Equals(sc2));
            Assert.AreEqual(sc.GetHashCode(), sc2.GetHashCode());
        }

        private static void testToString(Schema sc)
        {
            try
            {
                Assert.AreEqual(sc, Schema.Parse(sc.ToString()));
            }
            catch (Exception e)
            {
                throw new AvroException(e.ToString() + ": " + sc.ToString());
            }
        }

        [TestCase("{\"type\":\"record\",\"name\":\"LongList\"," +
            "\"fields\":[{\"name\":\"f1\",\"type\":\"long\"}," +
            "{\"name\":\"f2\",\"type\": \"int\"}]}",
            new string[] { "f1", "long", "100", "f2", "int", "10" })]
        [TestCase("{\"type\":\"record\",\"name\":\"LongList\"," +
            "\"fields\":[{\"name\":\"f1\",\"type\":\"long\", \"default\": \"100\"}," +
            "{\"name\":\"f2\",\"type\": \"int\"}]}",
            new string[] { "f1", "long", "100", "f2", "int", "10" })]
        [TestCase("{\"type\":\"record\",\"name\":\"LongList\"," +
            "\"fields\":[{\"name\":\"value\",\"type\":\"long\", \"default\": \"100\"}," +
            "{\"name\":\"next\",\"type\":[\"LongList\",\"null\"]}]}",
            new string[] { "value", "long", "100", "next", "union", null })]
        public void TestRecord(string s, string[] kv)
        {
            Schema sc = Schema.Parse(s);
            Assert.AreEqual(Schema.Type.Record, sc.Tag);
            RecordSchema rs = sc as RecordSchema;
            Assert.AreEqual(kv.Length / 3, rs.Count);
            for (int i = 0; i < kv.Length; i += 3)
            {
                Field f = rs[kv[i]];
                Assert.AreEqual(kv[i + 1], f.Schema.Name);
                /*
                if (kv[i + 2] != null)
                {
                    Assert.IsNotNull(f.DefaultValue);
                    Assert.AreEqual(kv[i + 2], f.DefaultValue);
                }
                else
                {
                    Assert.IsNull(f.DefaultValue);
                }
                 */
            }
            testEquality(s, sc);
            testToString(sc);
        }

        [TestCase("{\"type\": \"enum\", \"name\": \"Test\", \"symbols\": [\"A\", \"B\"]}",
            new string[] { "A", "B" })]
        public void TestEnum(string s, string[] symbols)
        {
            Schema sc = Schema.Parse(s);
            Assert.AreEqual(Schema.Type.Enumeration, sc.Tag);
            EnumSchema es = sc as EnumSchema;
            Assert.AreEqual(symbols.Length, es.Count);

            int i = 0;
            foreach (String str in es)
            {
                Assert.AreEqual(symbols[i++], str);
            }

            testEquality(s, sc);
            testToString(sc);
        }

        [TestCase("{\"type\": \"array\", \"items\": \"long\"}", "long")]
        public void TestArray(string s, string item)
        {
            Schema sc = Schema.Parse(s);
            Assert.AreEqual(Schema.Type.Array, sc.Tag);
            ArraySchema ars = sc as ArraySchema;
            Assert.AreEqual(item, ars.ItemSchema.Name);
            
            testEquality(s, sc);
            testToString(sc);
        }

        [TestCase("{\"type\": \"map\", \"values\": \"long\"}", "long")]
        public void TestMap(string s, string value)
        {
            Schema sc = Schema.Parse(s);
            Assert.AreEqual(Schema.Type.Map, sc.Tag);
            MapSchema ms = sc as MapSchema;
            Assert.AreEqual(value, ms.ValueSchema.Name);

            testEquality(s, sc);
            testToString(sc);
        }

        [TestCase("[\"string\", \"null\", \"long\"]", new string[] { "string", "null", "long" })]
        public void TestUnion(string s, string[] types)
        {
            Schema sc = Schema.Parse(s);
            Assert.AreEqual(Schema.Type.Union, sc.Tag);
            UnionSchema us = sc as UnionSchema;
            Assert.AreEqual(types.Length, us.Count);

            for (int i = 0; i < us.Count; i++)
            {
                Assert.AreEqual(types[i], us[i].Name);
            }
            testEquality(s, sc);
            testToString(sc);
        }

        [TestCase("{ \"type\": \"fixed\", \"name\": \"Test\", \"size\": 1}", 1)]
        public void TestFixed(string s, int size)
        {
            Schema sc = Schema.Parse(s);
            Assert.AreEqual(Schema.Type.Fixed, sc.Tag);
            FixedSchema fs = sc as FixedSchema;
            Assert.AreEqual(size, fs.Size);
            testEquality(s, sc);
            testToString(sc);
        }

        [TestCase("a", "o.a.h", Result = "o.a.h.a")]
        public string testFullname(string s1, string s2)
        {
            var name = new SchemaName(s1, s2, null);
            return name.Fullname;
        }

    }
}
