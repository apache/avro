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
        [TestCase("{\"type\": \"array\"}", typeof(AvroTypeException), Description = "No Items")]

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
            Assert.AreEqual(definedSchema.GetHashCode(), recordSchema["f1"].Schema.GetHashCode());
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

        [TestCase("{\"type\":\"record\",\"name\":\"Longs\",\"fields\":[{\"name\":\"value\",\"default\":\"100\",\"type\":\"long\",\"aliases\":[\"oldName\"]}]}",
            "Longs", null, null, null,
            new[] { "value" }, new[] { Schema.Type.Long }, new[] { "100" }, new[] { "oldName" }, new string[] { null })]
        [TestCase("{\"type\":\"record\",\"name\":\"Longs\",\"fields\":[{\"name\":\"value\",\"doc\":\"Field With Documentation\",\"default\":\"100\",\"type\":\"long\",\"aliases\":[\"oldName\"]}]}",
            "Longs", null, null, null,
            new[] { "value" }, new[] { Schema.Type.Long }, new[] { "100" }, new[] { "oldName" }, new string[] { "Field With Documentation" })]
        [TestCase("{\"type\":\"record\",\"name\":\"Longs\",\"namespace\":\"space\",\"fields\":[{\"name\":\"value\",\"default\":\"100\",\"type\":\"long\",\"aliases\":[\"oldName\"]}]}",
            "Longs", "space", null, null,
            new[] { "value" }, new[] { Schema.Type.Long }, new[] { "100" }, new[] { "oldName" }, new string[] { null })]
        [TestCase("{\"type\":\"record\",\"name\":\"Longs\",\"doc\":\"Record with alias\",\"namespace\":\"space\",\"aliases\":[\"space.RecordAlias\"],\"fields\":[{\"name\":\"value\",\"default\":\"100\",\"type\":\"long\",\"aliases\":[\"oldName\"]}]}",
            "Longs", "space", "RecordAlias", "Record with alias",
            new[] { "value" }, new[] { Schema.Type.Long }, new[] { "100" }, new[] { "oldName" }, new string[] { null })]
        [TestCase("{\"type\":\"record\",\"name\":\"Longs\",\"doc\":\"Record with two fields\",\"namespace\":\"space\",\"aliases\":[\"space.RecordAlias\"],\"fields\":[{\"name\":\"value\",\"doc\":\"first field\",\"default\":\"100\",\"type\":\"long\",\"aliases\":[\"oldName\"]},{\"name\":\"field2\",\"default\":\"true\",\"type\":\"boolean\"}]}",
            "Longs", "space", "RecordAlias", "Record with two fields",
            new[] { "value", "field2" }, new[] { Schema.Type.Long, Schema.Type.Boolean }, new[] { "100", "true" }, new[] { "oldName", null }, new string[] { "first field", null })]
        public void TestRecordCreation(string expectedSchema, string name, string space, string alias, string documentation, string[] fieldsNames, Schema.Type[] fieldsTypes, object[] fieldsDefaultValues, string[] fieldsAliases, string[] fieldsDocs)
        {
            IEnumerable<Field> recordFields = fieldsNames.Select((fieldName, i) => new Field(PrimitiveSchema.Create(fieldsTypes[i]),
                fieldName,
                fieldsAliases[i] == null? null: new List<string> { fieldsAliases[i] },
                i,
                fieldsDocs[i],
                fieldsDefaultValues[i].ToString(),
                Field.SortOrder.ignore,
                null));

            string[] aliases = alias == null ? null : new[] { alias };

            RecordSchema recordSchema = RecordSchema.Create(name, recordFields.ToList(), space, aliases, null, documentation);

            for(int i = 0; i < fieldsNames.Length; i++)
            {
                var fieldByName = recordSchema[fieldsNames[i]];
                if (fieldsAliases[i] != null)
                {
                    recordSchema.TryGetFieldAlias(fieldsAliases[i], out Field fieldByAlias);

                    Assert.AreSame(fieldByAlias, fieldByName);
                }
                Assert.AreEqual(expectedSchema, recordSchema.ToString());
                Assert.AreEqual(fieldsNames[i], fieldByName.Name);
                Assert.AreEqual(i, fieldByName.Pos);
                Assert.AreEqual(fieldsTypes[i], fieldByName.Schema.Tag);
                Assert.AreEqual(fieldsDocs[i], fieldByName.Documentation);
                Assert.AreEqual(fieldsDefaultValues[i], fieldByName.DefaultValue.ToString());
                CollectionAssert.AreEqual(fieldsAliases[i] == null? null: new[] {fieldsAliases[i]}, fieldByName.Aliases);
            }
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
        public void TestRecordFieldNames() {
            var fields = new List<Field>
                {
                    new Field(PrimitiveSchema.Create(Schema.Type.Long),
                        "歳以上",
                        null,
                        0,
                        null,
                        null,
                        Field.SortOrder.ignore,
                        null)
                };
            var recordSchema = RecordSchema.Create("LongList", fields, null, new[] { "LinkedLongs" });

            Field f = recordSchema.Fields[0];
            Assert.AreEqual("歳以上", f.Name);
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
        [TestCase("{\"type\":\"enum\",\"name\":\"Test\",\"doc\":\"Some explanation\",\"namespace\":\"mynamespace\",\"aliases\":[\"mynamespace.Alias\"],\"symbols\":[\"UNKNOWN\",\"A\",\"B\"],\"default\":\"UNKNOWN\",\"propertyKey\":\"propertyValue\"}",
            new string[] { "UNKNOWN", "A", "B" }, "mynamespace", new string[] { "Alias" }, "Some explanation", true, "UNKNOWN")]
        [TestCase("{\"type\":\"enum\",\"name\":\"Test\",\"doc\":\"Some explanation\",\"namespace\":\"space\",\"aliases\":[\"internalNamespace.Alias\"],\"symbols\":[\"UNKNOWN\",\"A\",\"B\"]}",
            new string[] { "UNKNOWN", "A", "B" }, "space", new string[] { "internalNamespace.Alias" }, "Some explanation")]
        [TestCase("{\"type\":\"enum\",\"name\":\"Test\",\"doc\":\"Some explanation\",\"namespace\":\"space\",\"aliases\":[\"internalNamespace.Alias\"],\"symbols\":[]}",
            new string[] { }, "space", new string[] { "internalNamespace.Alias" }, "Some explanation")]

        public void TestEnum(string s, string[] symbols, string space = null, IEnumerable<string> aliases = null, string doc = null, bool? usePropertyMap = null, string defaultSymbol = null)
        {
            Schema sc = Schema.Parse(s);

            PropertyMap propertyMap = new PropertyMap();
            propertyMap.Add("propertyKey", "\"propertyValue\"");
            Schema schema = EnumSchema.Create("Test",
                symbols,
                space,
                aliases,
                usePropertyMap == true ? propertyMap : null,
                doc,
                defaultSymbol);

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

        [TestCase("name", new string[] { "A", "B" }, "s", new[] { "L1", "L2" }, "regular enum", null, "name", "s")]
        [TestCase("s.name", new string[] { "A", "B" }, null, new[] { "L1", "L2" }, "internal namespace", null, "name", "s")]
        [TestCase("name", new string[] { "A", "B" }, null, new[] { "L1", "L2" }, "no namespace", null, "name", null)]
        [TestCase("name", new string[] { "A", "B" }, null, new[] { "L1", "L2" }, "with default value", "A", "name", null)]
        [TestCase("name", new string[] { "A1B2", "B4324" }, null, new[] { "L1", "L2" }, "with longer symbols", "B4324", "name", null)]
        [TestCase("name", new string[] { "_A1B2_", "B4324" }, null, new[] { "L1", "L2" }, "underscore in symbols", "_A1B2_", "name", null)]
        public void TestEnumCreation(string name, string[] symbols, string space, string[] aliases, string doc, string defaultSymbol, string expectedName, string expectedNamespace)
        {
            EnumSchema enumSchema = EnumSchema.Create(name, symbols, space, aliases, null, doc, defaultSymbol);

            Assert.AreEqual(expectedName, enumSchema.Name);
            CollectionAssert.AreEqual(symbols, enumSchema.Symbols);
            Assert.AreEqual(expectedNamespace, enumSchema.Namespace);
            Assert.AreEqual(Schema.Type.Enumeration, enumSchema.Tag);
            Assert.AreEqual(doc, enumSchema.Documentation);
            Assert.AreEqual(defaultSymbol, enumSchema.Default);
        }

        [TestCase(new[] {"A", "B"}, "C")]
        [TestCase(new[] {null, "B"}, null)]
        [TestCase(new[] {"", "B" }, null)]
        [TestCase(new[] {"8", "B" }, null)]
        [TestCase(new[] {"8", "B" }, null)]
        [TestCase(new[] {"A", "A" }, null)]
        [TestCase(new[] {" ", "A" }, null)]
        [TestCase(new[] {"9A23", "A" }, null)]
        public void TestEnumInvalidSymbols(string[] symbols, string defaultSymbol)
        {
            Assert.Throws<AvroException>(() => EnumSchema.Create("name", symbols, defaultSymbol: defaultSymbol));
        }

        [TestCase("{\"type\": \"array\", \"items\": \"long\"}", "long")]
        public void TestArray(string s, string item)
        {
            Schema sc = Schema.Parse(s);
            Assert.AreEqual(Schema.Type.Array, sc.Tag);
            ArraySchema ars = (ArraySchema)sc;
            Assert.AreEqual(item, ars.ItemSchema.Name);

            testEquality(s, sc);
            testToString(sc);
        }

        [TestCase]
        public void TestArrayCreation()
        {
            PrimitiveSchema itemsSchema = PrimitiveSchema.Create(Schema.Type.String);
            ArraySchema arraySchema = ArraySchema.Create(itemsSchema);

            Assert.AreEqual("array", arraySchema.Name);
            Assert.AreEqual(Schema.Type.Array, arraySchema.Tag);
            Assert.AreEqual(itemsSchema, arraySchema.ItemSchema);
        }

        [TestCase]
        public void TestInvalidArrayCreation()
        {
            Assert.Throws<ArgumentNullException>(() => ArraySchema.Create(null));
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
            MapSchema ms = (MapSchema)sc;
            Assert.AreEqual(value, ms.ValueSchema.Name);

            testEquality(s, sc);
            testToString(sc);
        }

        [TestCase]
        public void TestMapCreation()
        {
            PrimitiveSchema mapType = PrimitiveSchema.Create(Schema.Type.Float);
            MapSchema mapSchema = MapSchema.CreateMap(mapType);

            Assert.AreEqual("map", mapSchema.Fullname);
            Assert.AreEqual("map", mapSchema.Name);
            Assert.AreEqual(Schema.Type.Map, mapSchema.Tag);
            Assert.AreEqual(mapType, mapSchema.ValueSchema);
        }

        [TestCase]
        public void TestInvalidMapCreation()
        {
            Assert.Throws<ArgumentNullException>(() => MapSchema.CreateMap(null));
        }

        [TestCase("[\"string\", \"null\", \"long\"]",
            new Schema.Type[] { Schema.Type.String, Schema.Type.Null, Schema.Type.Long })]
        public void TestUnion(string s, Schema.Type[] types)
        {
            Schema sc = Schema.Parse(s);

            UnionSchema schema = UnionSchema.Create(types.Select(t => (Schema)PrimitiveSchema.Create(t)).ToList());
            Assert.AreEqual(sc, schema);

            Assert.AreEqual(Schema.Type.Union, sc.Tag);
            UnionSchema us = (UnionSchema)sc;
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

        [TestFixture]
        public class SchemaTypeExtensionsTests
        {
            [TestCase("null", Schema.Type.Null)]
            [TestCase("boolean", Schema.Type.Boolean)]
            [TestCase("int", Schema.Type.Int)]
            [TestCase("long", Schema.Type.Long)]
            [TestCase("float", Schema.Type.Float)]
            [TestCase("double", Schema.Type.Double)]
            [TestCase("bytes", Schema.Type.Bytes)]
            [TestCase("string", Schema.Type.String)]
            [TestCase("record", Schema.Type.Record)]
            [TestCase("enumeration", Schema.Type.Enumeration)]
            [TestCase("array", Schema.Type.Array)]
            [TestCase("map", Schema.Type.Map)]
            [TestCase("union", Schema.Type.Union)]
            [TestCase("fixed", Schema.Type.Fixed)]
            [TestCase("error", Schema.Type.Error)]
            [TestCase("logical", Schema.Type.Logical)]
            [TestCase("Logical", null)]
            [TestCase("InvalidValue", null)]
            [TestCase("\"null\"", null)]
            [TestCase("", null)]
            [TestCase(null, null)]
            public void ParseTypeTest(string value, object expectedResult)
            {
                Assert.AreEqual(Schema.ParseType(value), expectedResult);
            }

            [TestCase("\"null\"", Schema.Type.Null)]
            [TestCase("\"nu\"ll\"", null)]
            [TestCase("\"\"", null)]
            public void ParseTypeRemoveQuotesTest(string value, object expectedResult)
            {
                Assert.AreEqual(Schema.ParseType(value, true), expectedResult);
            }
        }
    }
}
