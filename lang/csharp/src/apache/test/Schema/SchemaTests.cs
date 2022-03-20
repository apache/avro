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
using System.Linq;

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
            typeof(SchemaParseException), Description = "Unknown name")]
        [TestCase("{\"type\":\"record\",\"name\":\"LongList\"}",
            typeof(SchemaParseException), Description = "No fields")]
        [TestCase("{\"type\":\"record\",\"name\":\"LongList\", \"fields\": \"hi\"}",
            typeof(SchemaParseException), Description = "Fields not an array")]
        [TestCase("[{\"type\": \"record\",\"name\": \"Test\",\"namespace\":\"ns1\",\"fields\": [{\"name\": \"f\",\"type\": \"long\"}]}," +
                   "{\"type\": \"record\",\"name\": \"Test\",\"namespace\":\"ns2\",\"fields\": [{\"name\": \"f\",\"type\": \"long\"}]}]")]

        // Doc
        [TestCase("{\"type\": \"record\",\"name\": \"Test\",\"doc\": \"Test Doc\",\"fields\": [{\"name\": \"f\",\"type\": \"long\"}]}")]

        // Enum
        [TestCase("{\"type\": \"enum\", \"name\": \"Test\", \"symbols\": [\"A\", \"B\"]}")]
        [TestCase("{\"type\": \"enum\", \"name\": \"Status\", \"symbols\": \"Normal Caution Critical\"}",
            typeof(SchemaParseException), Description = "Symbols not an array")]
        [TestCase("{\"type\": \"enum\", \"name\": [ 0, 1, 1, 2, 3, 5, 8 ], \"symbols\": [\"Golden\", \"Mean\"]}",
            typeof(SchemaParseException), Description = "Name not a string")]
        [TestCase("{\"type\": \"enum\", \"symbols\" : [\"I\", \"will\", \"fail\", \"no\", \"name\"]}",
            typeof(SchemaParseException), Description = "No name")]
        [TestCase("{\"type\": \"enum\", \"name\": \"Test\", \"symbols\" : [\"AA\", \"AA\"]}",
            typeof(SchemaParseException), Description = "Duplicate symbol")]

        // Array
        [TestCase("{\"type\": \"array\", \"items\": \"long\"}")]
        [TestCase("{\"type\": \"array\",\"items\": {\"type\": \"enum\", \"name\": \"Test\", \"symbols\": [\"A\", \"B\"]}}")]

        // Map
        [TestCase("{\"type\": \"map\", \"values\": \"long\"}")]
        [TestCase("{\"type\": \"map\",\"values\": {\"type\": \"enum\", \"name\": \"Test\", \"symbols\": [\"A\", \"B\"]}}")]

        // Union
        [TestCase("[\"string\", \"null\", \"long\"]")]
        [TestCase("[\"string\", \"long\", \"long\"]",
            typeof(SchemaParseException), Description = "Duplicate type")]
        [TestCase("[{\"type\": \"array\", \"items\": \"long\"}, {\"type\": \"array\", \"items\": \"string\"}]",
            typeof(SchemaParseException), Description = "Duplicate type")]
        [TestCase("{\"type\":[\"string\", \"null\", \"long\"]}")]

        // Fixed
        [TestCase("{ \"type\": \"fixed\", \"name\": \"Test\", \"size\": 1}")]
        [TestCase("{\"type\": \"fixed\", \"name\": \"MyFixed\", \"namespace\": \"org.apache.hadoop.avro\", \"size\": 1}")]
        [TestCase("{\"type\": \"fixed\", \"name\": \"Missing size\"}", typeof(SchemaParseException))]
        [TestCase("{\"type\": \"fixed\", \"size\": 314}",
            typeof(SchemaParseException), Description = "No name")]
        public void TestBasic(string s, Type expectedExceptionType = null)
        {
            if (expectedExceptionType != null)
            {
                Assert.Throws(expectedExceptionType, () => { Schema.Parse(s); });
            }
            else
            {
                Schema.Parse(s);
            }
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
            Schema schema = PrimitiveSchema.Create(type, null);

            Assert.AreEqual(sc, schema);

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
                throw new AvroException(e.ToString() + ": " + sc.ToString(), e);
            }
        }

        private static void testToString(Schema sc, string schema)
        {
            try
            {
                //remove any excess spaces in the JSON to normalize the match with toString 
                schema = schema.Replace("{ ", "{")
                    .Replace("} ", "}")
                    .Replace("\" ", "\"")
                    .Replace(", ", ",")
                    .Replace(": ", ":");
                Assert.AreEqual(sc.ToString(), schema);
            }
            catch (Exception e)
            {
                throw new AvroException($"{e} : {sc}", e);
            }
        }

        [TestCase("{ \"type\": \"null\", \"metafield\": \"abc\" }", Schema.Type.Null)]
        [TestCase("{ \"type\": \"boolean\", \"metafield\": \"abc\" }", Schema.Type.Boolean)]
        [TestCase("{ \"type\": \"int\", \"metafield\": \"abc\" }", Schema.Type.Int)]
        [TestCase("{ \"type\": \"long\", \"metafield\": \"abc\" }", Schema.Type.Long)]
        [TestCase("{ \"type\": \"float\", \"metafield\": \"abc\" }", Schema.Type.Float)]
        [TestCase("{ \"type\": \"double\", \"metafield\": \"abc\" }", Schema.Type.Double)]
        [TestCase("{ \"type\": \"bytes\", \"metafield\": \"abc\" }", Schema.Type.Bytes)]
        [TestCase("{ \"type\": \"string\", \"metafield\": \"abc\" }", Schema.Type.String)]
        public void TestPrimitiveWithMetadata(string rawSchema, Schema.Type type)
        {
            Schema definedSchema = Schema.Parse(rawSchema);
            Assert.IsTrue(definedSchema is PrimitiveSchema);
            Assert.AreEqual(type.ToString().ToLower(), definedSchema.Name);
            Assert.AreEqual(type, definedSchema.Tag);

            testEquality(rawSchema, definedSchema);
            testToString(definedSchema);

            Assert.True(definedSchema.ToString().Contains("metafield"));

            var rawRecordSchema = "{\"type\":\"record\",\"name\":\"Foo\"," +
                "\"fields\":[{\"name\":\"f1\",\"type\":" + rawSchema +
                "}]}";
            Schema baseRecordSchema = Schema.Parse(rawRecordSchema);
            Assert.AreEqual(Schema.Type.Record, baseRecordSchema.Tag);
            RecordSchema recordSchema = baseRecordSchema as RecordSchema;
            Assert.IsNotNull(recordSchema);
            Assert.AreEqual(1, recordSchema.Count);

            Assert.IsTrue(recordSchema["f1"].Schema is PrimitiveSchema);
            Assert.AreEqual(type.ToString().ToLower(), recordSchema["f1"].Schema.Name);
            Assert.AreEqual(type, recordSchema["f1"].Schema.Tag);

            testEquality(rawRecordSchema, baseRecordSchema);
            testToString(recordSchema["f1"].Schema);

            Assert.True(baseRecordSchema.ToString().Contains("metafield"));
            Assert.True(recordSchema["f1"].Schema.ToString().Contains("metafield"));

            Assert.True(definedSchema.Equals(recordSchema["f1"].Schema));
            Assert.AreEqual(definedSchema.GetHashCode(),recordSchema["f1"].Schema.GetHashCode());
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

        [TestCase("{\"type\":\"record\",\"name\":\"LongList\"," +
            "\"fields\":[{\"name\":\"f1\",\"type\":\"long\"}," +
            "{\"name\":\"f2\",\"type\": \"int\"}]}",
            null)]
        [TestCase("{\"type\":\"record\",\"name\":\"LongList\"," +
            "\"fields\":[{\"name\":\"f1\",\"type\":\"long\", \"default\": \"100\"}," +
            "{\"name\":\"f2\",\"type\": \"int\"}], \"doc\": \"\"}",
            "")]
        [TestCase("{\"type\":\"record\",\"name\":\"LongList\"," +
            "\"fields\":[{\"name\":\"f1\",\"type\":\"long\", \"default\": \"100\"}," +
            "{\"name\":\"f2\",\"type\": \"int\"}], \"doc\": \"this is a test\"}",
            "this is a test")]
        public void TestRecordDoc(string s, string expectedDoc)
        {
            var rs = Schema.Parse(s) as RecordSchema;
            Assert.IsNotNull(rs);
            Assert.AreEqual(expectedDoc, rs.Documentation);

            var roundTrip = Schema.Parse(rs.ToString()) as RecordSchema;

            Assert.IsNotNull(roundTrip);
            Assert.AreEqual(expectedDoc, roundTrip.Documentation);
        }

        [TestCase]
        public void TestRecordCreation()
        {
            string s = "{\"type\":\"record\",\"name\":\"Longs\",\"fields\":[{\"name\":\"value\",\"default\":\"100\",\"type\":\"long\",\"aliases\":[\"oldName\"]}]}";

            var recordField = new Field(PrimitiveSchema.Create(Schema.Type.Long),
                "value",
                new List<string> { "oldName" },
                0,
                null,
                "100",
                Field.SortOrder.ignore,
                null);

            RecordSchema recordSchema = RecordSchema.Create("Longs",
                new List<Field>
                {
                    recordField
                });

            var fieldByName = recordSchema["value"];
            recordSchema.TryGetFieldAlias("oldName", out Field fieldByAlias);

            Assert.AreEqual(s, recordSchema.ToString());
            Assert.AreEqual(recordField, fieldByName);
            Assert.AreEqual(recordField, fieldByAlias);
        }

        [TestCase]
        public void TestRecordCreationWithDuplicateFields()
        {
            var recordField = new Field(PrimitiveSchema.Create(Schema.Type.Long),
                "value",
                new List<string> { "oldName" },
                0,
                null,
                "100",
                Field.SortOrder.ignore,
                null);

            Assert.Throws<AvroException>(() => RecordSchema.Create("Longs",
                new List<Field>
                {
                    recordField,
                    recordField
                }));
        }

        [TestCase]
        public void TestRecordCreationWithRecursiveRecord()
        {
            string schema = "{\"type\":\"record\",\"name\":\"LongList\",\"aliases\":[\"LinkedLongs\"],\"fields\":[{\"name\":\"value\",\"type\":\"long\"},{\"name\":\"next\",\"type\":[\"null\",\"LongList\"]}]}";

            var recordSchema = RecordSchema.Create("LongList", new List<Field>(), null, new[] { "LinkedLongs" });

            recordSchema.Fields = new List<Field>
                {
                    new Field(PrimitiveSchema.Create(Schema.Type.Long),
                        "value",
                        null,
                        0,
                        null,
                        null,
                        Field.SortOrder.ignore,
                        null),
                    new Field(UnionSchema.Create(
                        new List<Schema>
                        {
                            PrimitiveSchema.Create(Schema.Type.Null), recordSchema
                        }),
                        "next",
                        1)
                };

            Assert.AreEqual(schema, recordSchema.ToString());
        }

        [TestCase("{\"type\":\"enum\",\"name\":\"Test\",\"symbols\":[\"A\",\"B\"]}",
                new string[] { "A", "B" })]

        [TestCase("{\"type\":\"enum\",\"name\":\"Test\",\"symbols\":[\"A\",\"B\"]}",
            new string[] { "A", "B" })]
        [TestCase("{\"type\":\"enum\",\"name\":\"Test\",\"symbols\":[\"UNKNOWN\",\"A\",\"B\"],\"default\":\"UNKNOWN\"}",
            new string[] { "UNKNOWN", "A", "B" }, "UNKNOWN")]
        public void TestEnum(string s, string[] symbols, string defaultSymbol = null)
        {
            Schema sc = Schema.Parse(s);

            Schema schema = EnumSchema.Create("Test", symbols, defaultSymbol: defaultSymbol);

            Assert.AreEqual(sc, schema);
            Assert.AreEqual(s, schema.ToString());

            Assert.AreEqual(Schema.Type.Enumeration, sc.Tag);
            EnumSchema es = sc as EnumSchema;
            Assert.AreEqual(symbols.Length, es.Count);

            int i = 0;
            foreach (string str in es)
            {
                Assert.AreEqual(symbols[i++], str);
            }

            testEquality(s, sc);
            testToString(sc, s);
        }

        [TestCase]
        public void TestEnumCreation()
        {
            string expected = "{\"type\":\"enum\",\"name\":\"Test\",\"namespace\":\"namespace\",\"aliases\":[\"namespace.alias\"],\"symbols\":[\"A\",\"B\"]}";

            Schema schema = EnumSchema.Create("Test", new[] { "A", "B" }, "namespace", new[] { "alias" }, null);

            Assert.AreEqual(expected, schema.ToString());
        }

        [TestCase("{\"type\": \"enum\", \"name\": \"Test\", \"symbols\": [\"A\", \"B\"]}", null)]
        [TestCase("{\"type\": \"enum\", \"name\": \"Test\", \"symbols\": [\"A\", \"B\"], \"doc\": \"\"}", "")]
        [TestCase("{\"type\": \"enum\", \"name\": \"Test\", \"symbols\": [\"A\", \"B\"], \"doc\": \"this is a test\"}", "this is a test")]
        public void TestEnumDoc(string s, string expectedDoc)
        {
            var es = Schema.Parse(s) as EnumSchema;
            Assert.IsNotNull(es);
            Assert.AreEqual(expectedDoc, es.Documentation);
        }

        [TestCase("{\"type\": \"enum\", \"name\": \"Test\", \"symbols\": [\"Unknown\", \"A\", \"B\"], \"default\": \"Unknown\" }", "Unknown")]
        public void TestEnumDefault(string s, string expectedToken)
        {
            var es = Schema.Parse(s) as EnumSchema;
            Assert.IsNotNull(es);
            Assert.AreEqual(es.Default, expectedToken);
        }

        [TestCase("{\"type\": \"enum\", \"name\": \"Test\", \"symbols\": [\"Unknown\", \"A\", \"B\"], \"default\": \"Something\" }")]
        public void TestEnumDefaultSymbolDoesntExist(string s)
        {
            Assert.Throws<SchemaParseException>(() => Schema.Parse(s));
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

        [TestCase("{\"type\": \"int\", \"logicalType\": \"date\"}", "int", "date")]
        public void TestLogicalPrimitive(string s, string baseType, string logicalType)
        {
            Schema sc = Schema.Parse(s);
            Assert.AreEqual(Schema.Type.Logical, sc.Tag);
            LogicalSchema logsc = sc as LogicalSchema;
            Assert.AreEqual(baseType, logsc.BaseSchema.Name);
            Assert.AreEqual(logicalType, logsc.LogicalType.Name);

            testEquality(s, sc);
            testToString(sc);
        }

        [TestCase("{\"type\": \"int\", \"logicalType\": \"unknown\"}", "unknown")]
        public void TestUnknownLogical(string s, string unknownType)
        {
            var err = Assert.Throws<AvroTypeException>(() => Schema.Parse(s));

            Assert.AreEqual("Logical type '" + unknownType + "' is not supported.", err.Message);
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

        [TestCase("[\"string\", \"null\", \"long\"]",
            new Schema.Type[] { Schema.Type.String, Schema.Type.Null, Schema.Type.Long })]
        public void TestUnion(string s, Schema.Type[] types)
        {
            Schema sc = Schema.Parse(s);

            UnionSchema schema = UnionSchema.Create(types.Select(t => (Schema)PrimitiveSchema.Create(t)).ToList());

            Assert.AreEqual(Schema.Type.Union, sc.Tag);
            UnionSchema us = sc as UnionSchema;
            Assert.AreEqual(types.Length, us.Count);

            for (int i = 0; i < us.Count; i++)
            {
                Assert.AreEqual(types[i].ToString().ToLower(), us[i].Name);
            }
            testEquality(s, sc);
            testToString(sc);
        }

        [TestCase]
        public void TestUnionCreation()
        {
            UnionSchema unionSchema = UnionSchema.Create(new List<Schema> { PrimitiveSchema.Create(Schema.Type.Null), PrimitiveSchema.Create(Schema.Type.String) });

            CollectionAssert.AreEqual(new List<Schema> { PrimitiveSchema.Create(Schema.Type.Null), PrimitiveSchema.Create(Schema.Type.String) },
                unionSchema.Schemas);
        }

        [TestCase]
        public void TestUnionCreationWithDuplicateSchemas()
        {
            Assert.Throws<ArgumentException>(() => UnionSchema.Create(new List<Schema> { PrimitiveSchema.Create(Schema.Type.String), PrimitiveSchema.Create(Schema.Type.String) }));
        }

        [TestCase]
        public void TestUnionNestedUnionCreation()
        {
            Assert.Throws<ArgumentException>(() => UnionSchema.Create(new List<Schema> { UnionSchema.Create(new List<Schema>()), PrimitiveSchema.Create(Schema.Type.String) }));
        }

        [TestCase("{\"type\":\"fixed\",\"name\":\"Test\",\"size\":1}", 1)]
        public void TestFixed(string s, int size)
        {
            Schema sc = Schema.Parse(s);
            FixedSchema schema = FixedSchema.Create("Test", 1);

            Assert.AreEqual(sc, schema);
            Assert.AreEqual(s, schema.ToString());

            Assert.AreEqual(Schema.Type.Fixed, sc.Tag);
            FixedSchema fs = sc as FixedSchema;
            Assert.AreEqual(size, fs.Size);
            testEquality(s, sc);
            testToString(sc);
        }

        [TestCase("{ \"type\": \"fixed\", \"name\": \"Test\", \"size\": 1}", null)]
        [TestCase("{ \"type\": \"fixed\", \"name\": \"Test\", \"size\": 1, \"doc\": \"\"}", "")]
        [TestCase("{ \"type\": \"fixed\", \"name\": \"Test\", \"size\": 1, \"doc\": \"this is a test\"}", "this is a test")]
        public void TestFixedDoc(string s, string expectedDoc)
        {
            var fs = Schema.Parse(s) as FixedSchema;
            Assert.IsNotNull(fs);
            Assert.AreEqual(expectedDoc, fs.Documentation);
        }

        [TestCase]
        public void TestFixedCreation()
        {
            string s = @"{""type"":""fixed"",""name"":""fixedName"",""namespace"":""space"",""aliases"":[""space.fixedOldName""],""size"":10}";

            FixedSchema fixedSchema = FixedSchema.Create("fixedName", 10, "space", new[] { "fixedOldName" }, null);

            Assert.AreEqual("fixedName", fixedSchema.Name);
            Assert.AreEqual("space.fixedName", fixedSchema.Fullname);
            Assert.AreEqual(10, fixedSchema.Size);
            Assert.AreEqual(s, fixedSchema.ToString());
        }

        [TestCase("a", "o.a.h", ExpectedResult = "o.a.h.a")]
        public string testFullname(string s1, string s2)
        {
            var name = new SchemaName(s1, s2, null, null);
            return name.Fullname;
        }

        [TestCase("{ \"type\": \"int\" }", "int")]
        [SetCulture("tr-TR")]
        public void TestSchemaNameInTurkishCulture(string schemaJson, string expectedName)
        {
            var schema = Schema.Parse(schemaJson);

            Assert.AreEqual(expectedName, schema.Name);
        }

        [TestCase("[\"null\",\"string\"]", "[\"null\",\"string\"]")]
        public void TestUnionSchemaWithoutTypeProperty(string schemaJson, string expectedSchemaJson)
        {
            var schema = Schema.Parse(schemaJson);
            Assert.AreEqual(schema.ToString(), expectedSchemaJson);
        }
    }
}
