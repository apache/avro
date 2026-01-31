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

        [TestCase]
        public void TestRecordWithNamedReference()
        {
            string nestedSchema = "{\"name\":\"NestedRecord\",\"type\":\"record\",\"fields\":[{\"name\":\"stringField\",\"type\":\"string\"}]}";
            // The root schema references the nested schema above by name only.
            // This mimics tools that allow schemas to have references to other schemas.
            string rootSchema = "{\"name\":\"RootRecord\",\"type\":\"record\",\"fields\":[{\"name\": \"nestedField\",\"type\":\"NestedRecord\"}]}";

            NamedSchema nestedRecord = (NamedSchema) Schema.Parse(nestedSchema);

            SchemaNames names = new SchemaNames();
            names.Add(nestedRecord.SchemaName, nestedRecord);

            // Pass the schema names when parsing the root schema and its reference.
            RecordSchema rootRecord = (RecordSchema) Schema.Parse(rootSchema, names);
            Assert.AreEqual("RootRecord", rootRecord.Name);
            Assert.AreEqual("NestedRecord", rootRecord.Fields[0].Schema.Name);
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

        [TestCase]
        public void Parse16DepthLevelSchemaTest()
        {
            var json = @"
{
  ""type"": ""record"",
  ""name"": ""Level1"",
  ""fields"": [
    {
      ""name"": ""level2"",
      ""type"": {
        ""type"": ""record"",
        ""name"": ""Level2"",
        ""fields"": [
          {
            ""name"": ""level3"",
            ""type"": {
              ""type"": ""record"",
              ""name"": ""Level3"",
              ""fields"": [
                {
                  ""name"": ""level4"",
                  ""type"": {
                    ""type"": ""record"",
                    ""name"": ""Level4"",
                    ""fields"": [
                      {
                        ""name"": ""level5"",
                        ""type"": {
                          ""type"": ""record"",
                          ""name"": ""Level5"",
                          ""fields"": [
                            {
                              ""name"": ""level6"",
                              ""type"": {
                                ""type"": ""record"",
                                ""name"": ""Level6"",
                                ""fields"": [
                                  {
                                    ""name"": ""level7"",
                                    ""type"": {
                                      ""type"": ""record"",
                                      ""name"": ""Level7"",
                                      ""fields"": [
                                        {
                                          ""name"": ""level8"",
                                          ""type"": {
                                            ""type"": ""record"",
                                            ""name"": ""Level8"",
                                            ""fields"": [
                                              {
                                                ""name"": ""level9"",
                                                ""type"": {
                                                  ""type"": ""record"",
                                                  ""name"": ""Level9"",
                                                  ""fields"": [
                                                    {
                                                      ""name"": ""level10"",
                                                      ""type"": {
                                                        ""type"": ""record"",
                                                        ""name"": ""Level10"",
                                                        ""fields"": [
                                                          {
                                                            ""name"": ""level11"",
                                                            ""type"": {
                                                              ""type"": ""record"",
                                                              ""name"": ""Level11"",
                                                              ""fields"": [
                                                                {
                                                                  ""name"": ""level12"",
                                                                  ""type"": {
                                                                    ""type"": ""record"",
                                                                    ""name"": ""Level12"",
                                                                    ""fields"": [
                                                                      {
                                                                        ""name"": ""level13"",
                                                                        ""type"": {
                                                                          ""type"": ""record"",
                                                                          ""name"": ""Level13"",
                                                                          ""fields"": [
                                                                            {
                                                                              ""name"": ""level14"",
                                                                              ""type"": {
                                                                                ""type"": ""record"",
                                                                                ""name"": ""Level14"",
                                                                                ""fields"": [
                                                                                  {
                                                                                    ""name"": ""level15"",
                                                                                    ""type"": {
                                                                                      ""type"": ""record"",
                                                                                      ""name"": ""Level15"",
                                                                                      ""fields"": [
                                                                                        {
                                                                                          ""name"": ""level16"",
                                                                                          ""type"": ""string""
                                                                                        }
                                                                                      ]
                                                                                    }
                                                                                  }
                                                                                ]
                                                                              }
                                                                            }
                                                                          ]
                                                                        }
                                                                      }
                                                                    ]
                                                                  }
                                                                }
                                                              ]
                                                            }
                                                          }
                                                        ]
                                                      }
                                                    }
                                                  ]
                                                }
                                              }
                                            ]
                                          }
                                        }
                                      ]
                                    }
                                  }
                                ]
                              }
                            }
                          ]
                        }
                      }
                    ]
                  }
                }
              ]
            }
          }
        ]
      }
    }
  ]
}
";

            var schema = Schema.Parse(json);

        }

        [TestCase]
        public void Parse32DepthLevelSchemaTest()
        {
            var json = @"
{
  ""type"": ""record"",
  ""name"": ""Level1"",
  ""fields"": [
    {
      ""name"": ""level2"",
      ""type"": {
        ""type"": ""record"",
        ""name"": ""Level2"",
        ""fields"": [
          {
            ""name"": ""level3"",
            ""type"": {
              ""type"": ""record"",
              ""name"": ""Level3"",
              ""fields"": [
                {
                  ""name"": ""level4"",
                  ""type"": {
                    ""type"": ""record"",
                    ""name"": ""Level4"",
                    ""fields"": [
                      {
                        ""name"": ""level5"",
                        ""type"": {
                          ""type"": ""record"",
                          ""name"": ""Level5"",
                          ""fields"": [
                            {
                              ""name"": ""level6"",
                              ""type"": {
                                ""type"": ""record"",
                                ""name"": ""Level6"",
                                ""fields"": [
                                  {
                                    ""name"": ""level7"",
                                    ""type"": {
                                      ""type"": ""record"",
                                      ""name"": ""Level7"",
                                      ""fields"": [
                                        {
                                          ""name"": ""level8"",
                                          ""type"": {
                                            ""type"": ""record"",
                                            ""name"": ""Level8"",
                                            ""fields"": [
                                              {
                                                ""name"": ""level9"",
                                                ""type"": {
                                                  ""type"": ""record"",
                                                  ""name"": ""Level9"",
                                                  ""fields"": [
                                                    {
                                                      ""name"": ""level10"",
                                                      ""type"": {
                                                        ""type"": ""record"",
                                                        ""name"": ""Level10"",
                                                        ""fields"": [
                                                          {
                                                            ""name"": ""level11"",
                                                            ""type"": {
                                                              ""type"": ""record"",
                                                              ""name"": ""Level11"",
                                                              ""fields"": [
                                                                {
                                                                  ""name"": ""level12"",
                                                                  ""type"": {
                                                                    ""type"": ""record"",
                                                                    ""name"": ""Level12"",
                                                                    ""fields"": [
                                                                      {
                                                                        ""name"": ""level13"",
                                                                        ""type"": {
                                                                          ""type"": ""record"",
                                                                          ""name"": ""Level13"",
                                                                          ""fields"": [
                                                                            {
                                                                              ""name"": ""level14"",
                                                                              ""type"": {
                                                                                ""type"": ""record"",
                                                                                ""name"": ""Level14"",
                                                                                ""fields"": [
                                                                                  {
                                                                                    ""name"": ""level15"",
                                                                                    ""type"": {
                                                                                      ""type"": ""record"",
                                                                                      ""name"": ""Level15"",
                                                                                      ""fields"": [
                                                                                        {
                                                                                          ""name"": ""level16"",
                                                                                          ""type"": {
                                                                                            ""type"": ""record"",
                                                                                            ""name"": ""Level16"",
                                                                                            ""fields"": [
                                                                                              {
                                                                                                ""name"": ""level17"",
                                                                                                ""type"": {
                                                                                                  ""type"": ""record"",
                                                                                                  ""name"": ""Level17"",
                                                                                                  ""fields"": [
                                                                                                    {
                                                                                                      ""name"": ""level18"",
                                                                                                      ""type"": {
                                                                                                        ""type"": ""record"",
                                                                                                        ""name"": ""Level18"",
                                                                                                        ""fields"": [
                                                                                                          {
                                                                                                            ""name"": ""level19"",
                                                                                                            ""type"": {
                                                                                                              ""type"": ""record"",
                                                                                                              ""name"": ""Level19"",
                                                                                                              ""fields"": [
                                                                                                                {
                                                                                                                  ""name"": ""level20"",
                                                                                                                  ""type"": {
                                                                                                                    ""type"": ""record"",
                                                                                                                    ""name"": ""Level20"",
                                                                                                                    ""fields"": [
                                                                                                                      {
                                                                                                                        ""name"": ""level21"",
                                                                                                                        ""type"": {
                                                                                                                          ""type"": ""record"",
                                                                                                                          ""name"": ""Level21"",
                                                                                                                          ""fields"": [
                                                                                                                            {
                                                                                                                              ""name"": ""level22"",
                                                                                                                              ""type"": {
                                                                                                                                ""type"": ""record"",
                                                                                                                                ""name"": ""Level22"",
                                                                                                                                ""fields"": [
                                                                                                                                  {
                                                                                                                                    ""name"": ""level23"",
                                                                                                                                    ""type"": {
                                                                                                                                      ""type"": ""record"",
                                                                                                                                      ""name"": ""Level23"",
                                                                                                                                      ""fields"": [
                                                                                                                                        {
                                                                                                                                          ""name"": ""level24"",
                                                                                                                                          ""type"": {
                                                                                                                                            ""type"": ""record"",
                                                                                                                                            ""name"": ""Level24"",
                                                                                                                                            ""fields"": [
                                                                                                                                              {
                                                                                                                                                ""name"": ""level25"",
                                                                                                                                                ""type"": {
                                                                                                                                                  ""type"": ""record"",
                                                                                                                                                  ""name"": ""Level25"",
                                                                                                                                                  ""fields"": [
                                                                                                                                                    {
                                                                                                                                                      ""name"": ""level26"",
                                                                                                                                                      ""type"": {
                                                                                                                                                        ""type"": ""record"",
                                                                                                                                                        ""name"": ""Level26"",
                                                                                                                                                        ""fields"": [
                                                                                                                                                          {
                                                                                                                                                            ""name"": ""level27"",
                                                                                                                                                            ""type"": {
                                                                                                                                                              ""type"": ""record"",
                                                                                                                                                              ""name"": ""Level27"",
                                                                                                                                                              ""fields"": [
                                                                                                                                                                {
                                                                                                                                                                  ""name"": ""level28"",
                                                                                                                                                                  ""type"": {
                                                                                                                                                                    ""type"": ""record"",
                                                                                                                                                                    ""name"": ""Level28"",
                                                                                                                                                                    ""fields"": [
                                                                                                                                                                      {
                                                                                                                                                                        ""name"": ""level29"",
                                                                                                                                                                        ""type"": {
                                                                                                                                                                          ""type"": ""record"",
                                                                                                                                                                          ""name"": ""Level29"",
                                                                                                                                                                          ""fields"": [
                                                                                                                                                                            {
                                                                                                                                                                              ""name"": ""level30"",
                                                                                                                                                                              ""type"": {
                                                                                                                                                                                ""type"": ""record"",
                                                                                                                                                                                ""name"": ""Level30"",
                                                                                                                                                                                ""fields"": [
                                                                                                                                                                                  {
                                                                                                                                                                                    ""name"": ""level31"",
                                                                                                                                                                                    ""type"": {
                                                                                                                                                                                      ""type"": ""record"",
                                                                                                                                                                                      ""name"": ""Level31"",
                                                                                                                                                                                      ""fields"": [
                                                                                                                                                                                        {
                                                                                                                                                                                          ""name"": ""level32"",
                                                                                                                                                                                          ""type"": ""string""
                                                                                                                                                                                        }
                                                                                                                                                                                      ]
                                                                                                                                                                                    }
                                                                                                                                                                                  }
                                                                                                                                                                                ]
                                                                                                                                                                              }
                                                                                                                                                                            }
                                                                                                                                                                          ]
                                                                                                                                                                        }
                                                                                                                                                                      }
                                                                                                                                                                    ]
                                                                                                                                                                  }
                                                                                                                                                                }
                                                                                                                                                              ]
                                                                                                                                                            }
                                                                                                                                                          }
                                                                                                                                                        ]
                                                                                                                                                      }
                                                                                                                                                    }
                                                                                                                                                  ]
                                                                                                                                                }
                                                                                                                                              }
                                                                                                                                            ]
                                                                                                                                          }
                                                                                                                                        }
                                                                                                                                      ]
                                                                                                                                    }
                                                                                                                                  }
                                                                                                                                ]
                                                                                                                              }
                                                                                                                            }
                                                                                                                          ]
                                                                                                                        }
                                                                                                                      }
                                                                                                                    ]
                                                                                                                  }
                                                                                                                }
                                                                                                              ]
                                                                                                            }
                                                                                                          }
                                                                                                        ]
                                                                                                      }
                                                                                                    }
                                                                                                  ]
                                                                                                }
                                                                                              }
                                                                                            ]
                                                                                          }
                                                                                        }
                                                                                      ]
                                                                                    }
                                                                                  }
                                                                                ]
                                                                              }
                                                                            }
                                                                          ]
                                                                        }
                                                                      }
                                                                    ]
                                                                  }
                                                                }
                                                              ]
                                                            }
                                                          }
                                                        ]
                                                      }
                                                    }
                                                  ]
                                                }
                                              }
                                            ]
                                          }
                                        }
                                      ]
                                    }
                                  }
                                ]
                              }
                            }
                          ]
                        }
                      }
                    ]
                  }
                }
              ]
            }
          }
        ]
      }
    }
  ]
}
";
            var schema = Schema.Parse(json);
        }

        [TestCase]
        public void Parse64DepthLevelSchemaTest()
        {
            var json = @"
{
  ""type"": ""record"",
  ""name"": ""Level1"",
  ""fields"": [
    {
      ""name"": ""level2"",
      ""type"": {
        ""type"": ""record"",
        ""name"": ""Level2"",
        ""fields"": [
          {
            ""name"": ""level3"",
            ""type"": {
              ""type"": ""record"",
              ""name"": ""Level3"",
              ""fields"": [
                {
                  ""name"": ""level4"",
                  ""type"": {
                    ""type"": ""record"",
                    ""name"": ""Level4"",
                    ""fields"": [
                      {
                        ""name"": ""level5"",
                        ""type"": {
                          ""type"": ""record"",
                          ""name"": ""Level5"",
                          ""fields"": [
                            {
                              ""name"": ""level6"",
                              ""type"": {
                                ""type"": ""record"",
                                ""name"": ""Level6"",
                                ""fields"": [
                                  {
                                    ""name"": ""level7"",
                                    ""type"": {
                                      ""type"": ""record"",
                                      ""name"": ""Level7"",
                                      ""fields"": [
                                        {
                                          ""name"": ""level8"",
                                          ""type"": {
                                            ""type"": ""record"",
                                            ""name"": ""Level8"",
                                            ""fields"": [
                                              {
                                                ""name"": ""level9"",
                                                ""type"": {
                                                  ""type"": ""record"",
                                                  ""name"": ""Level9"",
                                                  ""fields"": [
                                                    {
                                                      ""name"": ""level10"",
                                                      ""type"": {
                                                        ""type"": ""record"",
                                                        ""name"": ""Level10"",
                                                        ""fields"": [
                                                          {
                                                            ""name"": ""level11"",
                                                            ""type"": {
                                                              ""type"": ""record"",
                                                              ""name"": ""Level11"",
                                                              ""fields"": [
                                                                {
                                                                  ""name"": ""level12"",
                                                                  ""type"": {
                                                                    ""type"": ""record"",
                                                                    ""name"": ""Level12"",
                                                                    ""fields"": [
                                                                      {
                                                                        ""name"": ""level13"",
                                                                        ""type"": {
                                                                          ""type"": ""record"",
                                                                          ""name"": ""Level13"",
                                                                          ""fields"": [
                                                                            {
                                                                              ""name"": ""level14"",
                                                                              ""type"": {
                                                                                ""type"": ""record"",
                                                                                ""name"": ""Level14"",
                                                                                ""fields"": [
                                                                                  {
                                                                                    ""name"": ""level15"",
                                                                                    ""type"": {
                                                                                      ""type"": ""record"",
                                                                                      ""name"": ""Level15"",
                                                                                      ""fields"": [
                                                                                        {
                                                                                          ""name"": ""level16"",
                                                                                          ""type"": {
                                                                                            ""type"": ""record"",
                                                                                            ""name"": ""Level16"",
                                                                                            ""fields"": [
                                                                                              {
                                                                                                ""name"": ""level17"",
                                                                                                ""type"": {
                                                                                                  ""type"": ""record"",
                                                                                                  ""name"": ""Level17"",
                                                                                                  ""fields"": [
                                                                                                    {
                                                                                                      ""name"": ""level18"",
                                                                                                      ""type"": {
                                                                                                        ""type"": ""record"",
                                                                                                        ""name"": ""Level18"",
                                                                                                        ""fields"": [
                                                                                                          {
                                                                                                            ""name"": ""level19"",
                                                                                                            ""type"": {
                                                                                                              ""type"": ""record"",
                                                                                                              ""name"": ""Level19"",
                                                                                                              ""fields"": [
                                                                                                                {
                                                                                                                  ""name"": ""level20"",
                                                                                                                  ""type"": {
                                                                                                                    ""type"": ""record"",
                                                                                                                    ""name"": ""Level20"",
                                                                                                                    ""fields"": [
                                                                                                                      {
                                                                                                                        ""name"": ""level21"",
                                                                                                                        ""type"": {
                                                                                                                          ""type"": ""record"",
                                                                                                                          ""name"": ""Level21"",
                                                                                                                          ""fields"": [
                                                                                                                            {
                                                                                                                              ""name"": ""level22"",
                                                                                                                              ""type"": {
                                                                                                                                ""type"": ""record"",
                                                                                                                                ""name"": ""Level22"",
                                                                                                                                ""fields"": [
                                                                                                                                  {
                                                                                                                                    ""name"": ""level23"",
                                                                                                                                    ""type"": {
                                                                                                                                      ""type"": ""record"",
                                                                                                                                      ""name"": ""Level23"",
                                                                                                                                      ""fields"": [
                                                                                                                                        {
                                                                                                                                          ""name"": ""level24"",
                                                                                                                                          ""type"": {
                                                                                                                                            ""type"": ""record"",
                                                                                                                                            ""name"": ""Level24"",
                                                                                                                                            ""fields"": [
                                                                                                                                              {
                                                                                                                                                ""name"": ""level25"",
                                                                                                                                                ""type"": {
                                                                                                                                                  ""type"": ""record"",
                                                                                                                                                  ""name"": ""Level25"",
                                                                                                                                                  ""fields"": [
                                                                                                                                                    {
                                                                                                                                                      ""name"": ""level26"",
                                                                                                                                                      ""type"": {
                                                                                                                                                        ""type"": ""record"",
                                                                                                                                                        ""name"": ""Level26"",
                                                                                                                                                        ""fields"": [
                                                                                                                                                          {
                                                                                                                                                            ""name"": ""level27"",
                                                                                                                                                            ""type"": {
                                                                                                                                                              ""type"": ""record"",
                                                                                                                                                              ""name"": ""Level27"",
                                                                                                                                                              ""fields"": [
                                                                                                                                                                {
                                                                                                                                                                  ""name"": ""level28"",
                                                                                                                                                                  ""type"": {
                                                                                                                                                                    ""type"": ""record"",
                                                                                                                                                                    ""name"": ""Level28"",
                                                                                                                                                                    ""fields"": [
                                                                                                                                                                      {
                                                                                                                                                                        ""name"": ""level29"",
                                                                                                                                                                        ""type"": {
                                                                                                                                                                          ""type"": ""record"",
                                                                                                                                                                          ""name"": ""Level29"",
                                                                                                                                                                          ""fields"": [
                                                                                                                                                                            {
                                                                                                                                                                              ""name"": ""level30"",
                                                                                                                                                                              ""type"": {
                                                                                                                                                                                ""type"": ""record"",
                                                                                                                                                                                ""name"": ""Level30"",
                                                                                                                                                                                ""fields"": [
                                                                                                                                                                                  {
                                                                                                                                                                                    ""name"": ""level31"",
                                                                                                                                                                                    ""type"": {
                                                                                                                                                                                      ""type"": ""record"",
                                                                                                                                                                                      ""name"": ""Level31"",
                                                                                                                                                                                      ""fields"": [
                                                                                                                                                                                        {
                                                                                                                                                                                          ""name"": ""level32"",
                                                                                                                                                                                          ""type"": {
                                                                                                                                                                                            ""type"": ""record"",
                                                                                                                                                                                            ""name"": ""Level32"",
                                                                                                                                                                                            ""fields"": [
                                                                                                                                                                                              {
                                                                                                                                                                                                ""name"": ""level33"",
                                                                                                                                                                                                ""type"": {
                                                                                                                                                                                                  ""type"": ""record"",
                                                                                                                                                                                                  ""name"": ""Level33"",
                                                                                                                                                                                                  ""fields"": [
                                                                                                                                                                                                    {
                                                                                                                                                                                                      ""name"": ""level34"",
                                                                                                                                                                                                      ""type"": {
                                                                                                                                                                                                        ""type"": ""record"",
                                                                                                                                                                                                        ""name"": ""Level34"",
                                                                                                                                                                                                        ""fields"": [
                                                                                                                                                                                                          {
                                                                                                                                                                                                            ""name"": ""level35"",
                                                                                                                                                                                                            ""type"": {
                                                                                                                                                                                                              ""type"": ""record"",
                                                                                                                                                                                                              ""name"": ""Level35"",
                                                                                                                                                                                                              ""fields"": [
                                                                                                                                                                                                                {
                                                                                                                                                                                                                  ""name"": ""level36"",
                                                                                                                                                                                                                  ""type"": {
                                                                                                                                                                                                                    ""type"": ""record"",
                                                                                                                                                                                                                    ""name"": ""Level36"",
                                                                                                                                                                                                                    ""fields"": [
                                                                                                                                                                                                                      {
                                                                                                                                                                                                                        ""name"": ""level37"",
                                                                                                                                                                                                                        ""type"": {
                                                                                                                                                                                                                          ""type"": ""record"",
                                                                                                                                                                                                                          ""name"": ""Level37"",
                                                                                                                                                                                                                          ""fields"": [
                                                                                                                                                                                                                            {
                                                                                                                                                                                                                              ""name"": ""level38"",
                                                                                                                                                                                                                              ""type"": {
                                                                                                                                                                                                                                ""type"": ""record"",
                                                                                                                                                                                                                                ""name"": ""Level38"",
                                                                                                                                                                                                                                ""fields"": [
                                                                                                                                                                                                                                  {
                                                                                                                                                                                                                                    ""name"": ""level39"",
                                                                                                                                                                                                                                    ""type"": {
                                                                                                                                                                                                                                      ""type"": ""record"",
                                                                                                                                                                                                                                      ""name"": ""Level39"",
                                                                                                                                                                                                                                      ""fields"": [
                                                                                                                                                                                                                                        {
                                                                                                                                                                                                                                          ""name"": ""level40"",
                                                                                                                                                                                                                                          ""type"": {
                                                                                                                                                                                                                                            ""type"": ""record"",
                                                                                                                                                                                                                                            ""name"": ""Level40"",
                                                                                                                                                                                                                                            ""fields"": [
                                                                                                                                                                                                                                              {
                                                                                                                                                                                                                                                ""name"": ""level41"",
                                                                                                                                                                                                                                                ""type"": {
                                                                                                                                                                                                                                                  ""type"": ""record"",
                                                                                                                                                                                                                                                  ""name"": ""Level41"",
                                                                                                                                                                                                                                                  ""fields"": [
                                                                                                                                                                                                                                                    {
                                                                                                                                                                                                                                                      ""name"": ""level42"",
                                                                                                                                                                                                                                                      ""type"": {
                                                                                                                                                                                                                                                        ""type"": ""record"",
                                                                                                                                                                                                                                                        ""name"": ""Level42"",
                                                                                                                                                                                                                                                        ""fields"": [
                                                                                                                                                                                                                                                          {
                                                                                                                                                                                                                                                            ""name"": ""level43"",
                                                                                                                                                                                                                                                            ""type"": {
                                                                                                                                                                                                                                                              ""type"": ""record"",
                                                                                                                                                                                                                                                              ""name"": ""Level43"",
                                                                                                                                                                                                                                                              ""fields"": [
                                                                                                                                                                                                                                                                {
                                                                                                                                                                                                                                                                  ""name"": ""level44"",
                                                                                                                                                                                                                                                                  ""type"": {
                                                                                                                                                                                                                                                                    ""type"": ""record"",
                                                                                                                                                                                                                                                                    ""name"": ""Level44"",
                                                                                                                                                                                                                                                                    ""fields"": [
                                                                                                                                                                                                                                                                      {
                                                                                                                                                                                                                                                                        ""name"": ""level45"",
                                                                                                                                                                                                                                                                        ""type"": {
                                                                                                                                                                                                                                                                          ""type"": ""record"",
                                                                                                                                                                                                                                                                          ""name"": ""Level45"",
                                                                                                                                                                                                                                                                          ""fields"": [
                                                                                                                                                                                                                                                                            {
                                                                                                                                                                                                                                                                              ""name"": ""level46"",
                                                                                                                                                                                                                                                                              ""type"": {
                                                                                                                                                                                                                                                                                ""type"": ""record"",
                                                                                                                                                                                                                                                                                ""name"": ""Level46"",
                                                                                                                                                                                                                                                                                ""fields"": [
                                                                                                                                                                                                                                                                                  {
                                                                                                                                                                                                                                                                                    ""name"": ""level47"",
                                                                                                                                                                                                                                                                                    ""type"": {
                                                                                                                                                                                                                                                                                      ""type"": ""record"",
                                                                                                                                                                                                                                                                                      ""name"": ""Level47"",
                                                                                                                                                                                                                                                                                      ""fields"": [
                                                                                                                                                                                                                                                                                        {
                                                                                                                                                                                                                                                                                          ""name"": ""level48"",
                                                                                                                                                                                                                                                                                          ""type"": {
                                                                                                                                                                                                                                                                                            ""type"": ""record"",
                                                                                                                                                                                                                                                                                            ""name"": ""Level48"",
                                                                                                                                                                                                                                                                                            ""fields"": [
                                                                                                                                                                                                                                                                                              {
                                                                                                                                                                                                                                                                                                ""name"": ""level49"",
                                                                                                                                                                                                                                                                                                ""type"": {
                                                                                                                                                                                                                                                                                                  ""type"": ""record"",
                                                                                                                                                                                                                                                                                                  ""name"": ""Level49"",
                                                                                                                                                                                                                                                                                                  ""fields"": [
                                                                                                                                                                                                                                                                                                    {
                                                                                                                                                                                                                                                                                                      ""name"": ""level50"",
                                                                                                                                                                                                                                                                                                      ""type"": {
                                                                                                                                                                                                                                                                                                        ""type"": ""record"",
                                                                                                                                                                                                                                                                                                        ""name"": ""Level50"",
                                                                                                                                                                                                                                                                                                        ""fields"": [
                                                                                                                                                                                                                                                                                                          {
                                                                                                                                                                                                                                                                                                            ""name"": ""level51"",
                                                                                                                                                                                                                                                                                                            ""type"": {
                                                                                                                                                                                                                                                                                                              ""type"": ""record"",
                                                                                                                                                                                                                                                                                                              ""name"": ""Level51"",
                                                                                                                                                                                                                                                                                                              ""fields"": [
                                                                                                                                                                                                                                                                                                                {
                                                                                                                                                                                                                                                                                                                  ""name"": ""level52"",
                                                                                                                                                                                                                                                                                                                  ""type"": {
                                                                                                                                                                                                                                                                                                                    ""type"": ""record"",
                                                                                                                                                                                                                                                                                                                    ""name"": ""Level52"",
                                                                                                                                                                                                                                                                                                                    ""fields"": [
                                                                                                                                                                                                                                                                                                                      {
                                                                                                                                                                                                                                                                                                                        ""name"": ""level53"",
                                                                                                                                                                                                                                                                                                                        ""type"": {
                                                                                                                                                                                                                                                                                                                          ""type"": ""record"",
                                                                                                                                                                                                                                                                                                                          ""name"": ""Level53"",
                                                                                                                                                                                                                                                                                                                          ""fields"": [
                                                                                                                                                                                                                                                                                                                            {
                                                                                                                                                                                                                                                                                                                              ""name"": ""level54"",
                                                                                                                                                                                                                                                                                                                              ""type"": {
                                                                                                                                                                                                                                                                                                                                ""type"": ""record"",
                                                                                                                                                                                                                                                                                                                                ""name"": ""Level54"",
                                                                                                                                                                                                                                                                                                                                ""fields"": [
                                                                                                                                                                                                                                                                                                                                  {
                                                                                                                                                                                                                                                                                                                                    ""name"": ""level55"",
                                                                                                                                                                                                                                                                                                                                    ""type"": {
                                                                                                                                                                                                                                                                                                                                      ""type"": ""record"",
                                                                                                                                                                                                                                                                                                                                      ""name"": ""Level55"",
                                                                                                                                                                                                                                                                                                                                      ""fields"": [
                                                                                                                                                                                                                                                                                                                                        {
                                                                                                                                                                                                                                                                                                                                          ""name"": ""level56"",
                                                                                                                                                                                                                                                                                                                                          ""type"": {
                                                                                                                                                                                                                                                                                                                                            ""type"": ""record"",
                                                                                                                                                                                                                                                                                                                                            ""name"": ""Level56"",
                                                                                                                                                                                                                                                                                                                                            ""fields"": [
                                                                                                                                                                                                                                                                                                                                              {
                                                                                                                                                                                                                                                                                                                                                ""name"": ""level57"",
                                                                                                                                                                                                                                                                                                                                                ""type"": {
                                                                                                                                                                                                                                                                                                                                                  ""type"": ""record"",
                                                                                                                                                                                                                                                                                                                                                  ""name"": ""Level57"",
                                                                                                                                                                                                                                                                                                                                                  ""fields"": [
                                                                                                                                                                                                                                                                                                                                                    {
                                                                                                                                                                                                                                                                                                                                                      ""name"": ""level58"",
                                                                                                                                                                                                                                                                                                                                                      ""type"": {
                                                                                                                                                                                                                                                                                                                                                        ""type"": ""record"",
                                                                                                                                                                                                                                                                                                                                                        ""name"": ""Level58"",
                                                                                                                                                                                                                                                                                                                                                        ""fields"": [
                                                                                                                                                                                                                                                                                                                                                          {
                                                                                                                                                                                                                                                                                                                                                            ""name"": ""level59"",
                                                                                                                                                                                                                                                                                                                                                            ""type"": {
                                                                                                                                                                                                                                                                                                                                                              ""type"": ""record"",
                                                                                                                                                                                                                                                                                                                                                              ""name"": ""Level59"",
                                                                                                                                                                                                                                                                                                                                                              ""fields"": [
                                                                                                                                                                                                                                                                                                                                                                {
                                                                                                                                                                                                                                                                                                                                                                  ""name"": ""level60"",
                                                                                                                                                                                                                                                                                                                                                                  ""type"": {
                                                                                                                                                                                                                                                                                                                                                                    ""type"": ""record"",
                                                                                                                                                                                                                                                                                                                                                                    ""name"": ""Level60"",
                                                                                                                                                                                                                                                                                                                                                                    ""fields"": [
                                                                                                                                                                                                                                                                                                                                                                      {
                                                                                                                                                                                                                                                                                                                                                                        ""name"": ""level61"",
                                                                                                                                                                                                                                                                                                                                                                        ""type"": {
                                                                                                                                                                                                                                                                                                                                                                          ""type"": ""record"",
                                                                                                                                                                                                                                                                                                                                                                          ""name"": ""Level61"",
                                                                                                                                                                                                                                                                                                                                                                          ""fields"": [
                                                                                                                                                                                                                                                                                                                                                                            {
                                                                                                                                                                                                                                                                                                                                                                              ""name"": ""level62"",
                                                                                                                                                                                                                                                                                                                                                                              ""type"": {
                                                                                                                                                                                                                                                                                                                                                                                ""type"": ""record"",
                                                                                                                                                                                                                                                                                                                                                                                ""name"": ""Level62"",
                                                                                                                                                                                                                                                                                                                                                                                ""fields"": [
                                                                                                                                                                                                                                                                                                                                                                                  {
                                                                                                                                                                                                                                                                                                                                                                                    ""name"": ""level63"",
                                                                                                                                                                                                                                                                                                                                                                                    ""type"": {
                                                                                                                                                                                                                                                                                                                                                                                      ""type"": ""record"",
                                                                                                                                                                                                                                                                                                                                                                                      ""name"": ""Level63"",
                                                                                                                                                                                                                                                                                                                                                                                      ""fields"": [
                                                                                                                                                                                                                                                                                                                                                                                        {
                                                                                                                                                                                                                                                                                                                                                                                          ""name"": ""level64"",
                                                                                                                                                                                                                                                                                                                                                                                          ""type"": ""string""
                                                                                                                                                                                                                                                                                                                                                                                        }
                                                                                                                                                                                                                                                                                                                                                                                      ]
                                                                                                                                                                                                                                                                                                                                                                                    }
                                                                                                                                                                                                                                                                                                                                                                                  }
                                                                                                                                                                                                                                                                                                                                                                                ]
                                                                                                                                                                                                                                                                                                                                                                              }
                                                                                                                                                                                                                                                                                                                                                                            }
                                                                                                                                                                                                                                                                                                                                                                          ]
                                                                                                                                                                                                                                                                                                                                                                        }
                                                                                                                                                                                                                                                                                                                                                                      }
                                                                                                                                                                                                                                                                                                                                                                    ]
                                                                                                                                                                                                                                                                                                                                                                  }
                                                                                                                                                                                                                                                                                                                                                                }
                                                                                                                                                                                                                                                                                                                                                              ]
                                                                                                                                                                                                                                                                                                                                                            }
                                                                                                                                                                                                                                                                                                                                                          }
                                                                                                                                                                                                                                                                                                                                                        ]
                                                                                                                                                                                                                                                                                                                                                      }
                                                                                                                                                                                                                                                                                                                                                    }
                                                                                                                                                                                                                                                                                                                                                  ]
                                                                                                                                                                                                                                                                                                                                                }
                                                                                                                                                                                                                                                                                                                                              }
                                                                                                                                                                                                                                                                                                                                            ]
                                                                                                                                                                                                                                                                                                                                          }
                                                                                                                                                                                                                                                                                                                                        }
                                                                                                                                                                                                                                                                                                                                      ]
                                                                                                                                                                                                                                                                                                                                    }
                                                                                                                                                                                                                                                                                                                                  }
                                                                                                                                                                                                                                                                                                                                ]
                                                                                                                                                                                                                                                                                                                              }
                                                                                                                                                                                                                                                                                                                            }
                                                                                                                                                                                                                                                                                                                          ]
                                                                                                                                                                                                                                                                                                                        }
                                                                                                                                                                                                                                                                                                                      }
                                                                                                                                                                                                                                                                                                                    ]
                                                                                                                                                                                                                                                                                                                  }
                                                                                                                                                                                                                                                                                                                }
                                                                                                                                                                                                                                                                                                              ]
                                                                                                                                                                                                                                                                                                            }
                                                                                                                                                                                                                                                                                                          }
                                                                                                                                                                                                                                                                                                        ]
                                                                                                                                                                                                                                                                                                      }
                                                                                                                                                                                                                                                                                                    }
                                                                                                                                                                                                                                                                                                  ]
                                                                                                                                                                                                                                                                                                }
                                                                                                                                                                                                                                                                                              }
                                                                                                                                                                                                                                                                                            ]
                                                                                                                                                                                                                                                                                          }
                                                                                                                                                                                                                                                                                        }
                                                                                                                                                                                                                                                                                      ]
                                                                                                                                                                                                                                                                                    }
                                                                                                                                                                                                                                                                                  }
                                                                                                                                                                                                                                                                                ]
                                                                                                                                                                                                                                                                              }
                                                                                                                                                                                                                                                                            }
                                                                                                                                                                                                                                                                          ]
                                                                                                                                                                                                                                                                        }
                                                                                                                                                                                                                                                                      }
                                                                                                                                                                                                                                                                    ]
                                                                                                                                                                                                                                                                  }
                                                                                                                                                                                                                                                                }
                                                                                                                                                                                                                                                              ]
                                                                                                                                                                                                                                                            }
                                                                                                                                                                                                                                                          }
                                                                                                                                                                                                                                                        ]
                                                                                                                                                                                                                                                      }
                                                                                                                                                                                                                                                    }
                                                                                                                                                                                                                                                  ]
                                                                                                                                                                                                                                                }
                                                                                                                                                                                                                                              }
                                                                                                                                                                                                                                            ]
                                                                                                                                                                                                                                          }
                                                                                                                                                                                                                                        }
                                                                                                                                                                                                                                      ]
                                                                                                                                                                                                                                    }
                                                                                                                                                                                                                                  }
                                                                                                                                                                                                                                ]
                                                                                                                                                                                                                              }
                                                                                                                                                                                                                            }
                                                                                                                                                                                                                          ]
                                                                                                                                                                                                                        }
                                                                                                                                                                                                                      }
                                                                                                                                                                                                                    ]
                                                                                                                                                                                                                  }
                                                                                                                                                                                                                }
                                                                                                                                                                                                              ]
                                                                                                                                                                                                            }
                                                                                                                                                                                                          }
                                                                                                                                                                                                        ]
                                                                                                                                                                                                      }
                                                                                                                                                                                                    }
                                                                                                                                                                                                  ]
                                                                                                                                                                                                }
                                                                                                                                                                                              }
                                                                                                                                                                                            ]
                                                                                                                                                                                          }
                                                                                                                                                                                        }
                                                                                                                                                                                      ]
                                                                                                                                                                                    }
                                                                                                                                                                                  }
                                                                                                                                                                                ]
                                                                                                                                                                              }
                                                                                                                                                                            }
                                                                                                                                                                          ]
                                                                                                                                                                        }
                                                                                                                                                                      }
                                                                                                                                                                    ]
                                                                                                                                                                  }
                                                                                                                                                                }
                                                                                                                                                              ]
                                                                                                                                                            }
                                                                                                                                                          }
                                                                                                                                                        ]
                                                                                                                                                      }
                                                                                                                                                    }
                                                                                                                                                  ]
                                                                                                                                                }
                                                                                                                                              }
                                                                                                                                            ]
                                                                                                                                          }
                                                                                                                                        }
                                                                                                                                      ]
                                                                                                                                    }
                                                                                                                                  }
                                                                                                                                ]
                                                                                                                              }
                                                                                                                            }
                                                                                                                          ]
                                                                                                                        }
                                                                                                                      }
                                                                                                                    ]
                                                                                                                  }
                                                                                                                }
                                                                                                              ]
                                                                                                            }
                                                                                                          }
                                                                                                        ]
                                                                                                      }
                                                                                                    }
                                                                                                  ]
                                                                                                }
                                                                                              }
                                                                                            ]
                                                                                          }
                                                                                        }
                                                                                      ]
                                                                                    }
                                                                                  }
                                                                                ]
                                                                              }
                                                                            }
                                                                          ]
                                                                        }
                                                                      }
                                                                    ]
                                                                  }
                                                                }
                                                              ]
                                                            }
                                                          }
                                                        ]
                                                      }
                                                    }
                                                  ]
                                                }
                                              }
                                            ]
                                          }
                                        }
                                      ]
                                    }
                                  }
                                ]
                              }
                            }
                          ]
                        }
                      }
                    ]
                  }
                }
              ]
            }
          }
        ]
      }
    }
  ]
}
";

            var schema = Schema.Parse(json);
        }

       [TestCase]
        public void ParseRealWorldSchemaTest()
        {
            var json = @"
{
  ""type"": ""record"",
  ""name"": ""IcaInstructionSchema"",
  ""namespace"": ""com.capgroup.casa"",
  ""fields"": [
    {
      ""name"": ""ICAINSTRUCTIONDOWNLOAD"",
      ""type"": [
        ""null"",
        {
          ""type"": ""record"",
          ""name"": ""ICAINSTRUCTIONDOWNLOAD"",
          ""fields"": [
            {
              ""name"": ""Header"",
              ""type"": [
                ""null"",
                {
                  ""type"": ""record"",
                  ""name"": ""Header"",
                  ""fields"": [
                    {
                      ""name"": ""FileName"",
                      ""type"": [
                        ""null"",
                        ""string""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""CreationDate"",
                      ""type"": [
                        ""null"",
                        ""string""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""Reference"",
                      ""type"": [
                        ""null"",
                        ""string""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""VersionNo"",
                      ""type"": [
                        ""null"",
                        ""string""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""MessageType"",
                      ""type"": [
                        ""null"",
                        ""string""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""NoOfRecords"",
                      ""type"": [
                        ""null"",
                        ""int""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""StatusIndicator"",
                      ""type"": [
                        ""null"",
                        ""int""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""RelatedRef"",
                      ""type"": [
                        ""null"",
                        ""int""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""To"",
                      ""type"": [
                        ""null"",
                        ""string""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""From"",
                      ""type"": [
                        ""null"",
                        ""string""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""FromIndicator"",
                      ""type"": [
                        ""null"",
                        ""string""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""Direction"",
                      ""type"": [
                        ""null"",
                        ""string""
                      ],
                      ""default"": null
                    }
                  ]
                }
              ],
              ""default"": null
            },
            {
              ""name"": ""CorporateAction"",
              ""type"": [
                ""null"",
                {
                  ""type"": ""record"",
                  ""name"": ""CorporateAction"",
                  ""fields"": [
                    {
                      ""name"": ""TotalBlockNo"",
                      ""type"": [
                        ""null"",
                        ""long""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""BlockNo"",
                      ""type"": [
                        ""null"",
                        ""long""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""FIBic"",
                      ""type"": [
                        ""null"",
                        ""string""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""CorporateActionId"",
                      ""type"": [
                        ""null"",
                        ""long""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""ActionType"",
                      ""type"": [
                        ""null"",
                        ""string""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""ActionIndicator"",
                      ""type"": [
                        ""null"",
                        ""string""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""CAStatusCd"",
                      ""type"": [
                        ""null"",
                        ""string""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""EffectiveDate"",
                      ""type"": [
                        ""null"",
                        ""string""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""ClientEntitlementBasisDate"",
                      ""type"": [
                        ""null"",
                        ""string""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""MarketEntitlementBasisDate"",
                      ""type"": [
                        ""null"",
                        ""string""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""Dates"",
                      ""type"": [
                        ""null"",
                        {
                          ""type"": ""record"",
                          ""name"": ""Dates"",
                          ""namespace"": ""com.capgroup.dates"",
                          ""fields"": [
                            {
                              ""name"": ""Date"",
                              ""type"": [
                                ""null"",
                                {
                                  ""type"": ""array"",
                                  ""items"": [
                                    ""null"",
                                    {
                                      ""type"": ""record"",
                                      ""name"": ""Date"",
                                      ""fields"": [
                                        {
                                          ""name"": ""DateQualifierCd"",
                                          ""type"": [
                                            ""null"",
                                            ""string""
                                          ],
                                          ""default"": null
                                        },
                                        {
                                          ""name"": ""DateValue"",
                                          ""type"": [
                                            ""null"",
                                            ""double""
                                          ],
                                          ""default"": null
                                        },
                                        {
                                          ""name"": ""DateCd"",
                                          ""type"": [
                                            ""null"",
                                            ""string""
                                          ],
                                          ""default"": null
                                        }
                                      ]
                                    }
                                  ]
                                }
                              ],
                              ""default"": null
                            }
                          ]
                        }
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""CouponNumber"",
                      ""type"": [
                        ""null"",
                        ""string""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""ExpectedSettlementDate"",
                      ""type"": [
                        ""null"",
                        ""string""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""PriceFactorCurrencyCd"",
                      ""type"": [
                        ""null"",
                        ""string""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""PriceFactor"",
                      ""type"": [
                        ""null"",
                        ""double""
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""SecurityDetails"",
                      ""type"": [
                        ""null"",
                        {
                          ""type"": ""record"",
                          ""name"": ""SecurityDetails"",
                          ""fields"": [
                            {
                              ""name"": ""IssueCountryCd"",
                              ""type"": [
                                ""null"",
                                ""string""
                              ],
                              ""default"": null
                            },
                            {
                              ""name"": ""IssueCurrencyCd"",
                              ""type"": [
                                ""null"",
                                ""string""
                              ],
                              ""default"": null
                            },
                            {
                              ""name"": ""SecurityExternalReferences"",
                              ""type"": [
                                ""null"",
                                {
                                  ""type"": ""record"",
                                  ""name"": ""SecurityExternalReferences"",
                                  ""fields"": [
                                    {
                                      ""name"": ""SecurityExternalReference"",
                                      ""type"": {
                                        ""type"": ""array"",
                                        ""items"": {
                                          ""type"": ""record"",
                                          ""name"": ""SecurityExternalReference"",
                                          ""fields"": [
                                            {
                                              ""name"": ""SecurityIdentifierTypeCd"",
                                              ""type"": [
                                                ""null"",
                                                ""string""
                                              ],
                                              ""default"": null
                                            },
                                            {
                                              ""name"": ""SecurityIdentifierValue"",
                                              ""type"": [
                                                ""null"",
                                                ""string""
                                              ],
                                              ""default"": null
                                            }
                                          ]
                                        }
                                      }
                                    }
                                  ]
                                }
                              ],
                              ""default"": null
                            },
                            {
                              ""name"": ""ReportingClassificationGroups"",
                              ""type"": [
                                ""null"",
                                {
                                  ""type"": ""record"",
                                  ""name"": ""ReportingClassificationGroups"",
                                  ""fields"": [
                                    {
                                      ""name"": ""ReportingClassificationGroup"",
                                      ""type"": {
                                        ""type"": ""array"",
                                        ""items"": {
                                          ""type"": ""record"",
                                          ""name"": ""ReportingClassificationGroup"",
                                          ""fields"": [
                                            {
                                              ""name"": ""ReportingClassificationGroupId"",
                                              ""type"": [
                                                ""null"",
                                                ""long""
                                              ],
                                              ""default"": null
                                            },
                                            {
                                              ""name"": ""ReportingClassificationGroupName"",
                                              ""type"": [
                                                ""null"",
                                                ""string""
                                              ],
                                              ""default"": null
                                            },
                                            {
                                              ""name"": ""ValueId"",
                                              ""type"": [
                                                ""null"",
                                                ""long""
                                              ],
                                              ""default"": null
                                            },
                                            {
                                              ""name"": ""ValueName"",
                                              ""type"": [
                                                ""null"",
                                                ""string""
                                              ],
                                              ""default"": null
                                            },
                                            {
                                              ""name"": ""ValueCode"",
                                              ""type"": [
                                                ""null"",
                                                ""string""
                                              ],
                                              ""default"": null
                                            },
                                            {
                                              ""name"": ""ValueSequence"",
                                              ""type"": [
                                                ""null"",
                                                ""string""
                                              ],
                                              ""default"": null
                                            }
                                          ]
                                        }
                                      }
                                    }
                                  ]
                                }
                              ],
                              ""default"": null
                            }
                          ]
                        }
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""Options"",
                      ""type"": [
                        ""null"",
                        {
                          ""type"": ""record"",
                          ""name"": ""Options"",
                          ""fields"": [
                            {
                              ""name"": ""Option"",
                              ""type"": {
                                ""type"": ""array"",
                                ""items"": {
                                  ""type"": ""record"",
                                  ""name"": ""Option"",
                                  ""fields"": [
                                    {
                                      ""name"": ""OptionId"",
                                      ""type"": [
                                        ""null"",
                                        ""long""
                                      ],
                                      ""default"": null
                                    },
                                    {
                                      ""name"": ""OptionNumber"",
                                      ""type"": [
                                        ""null"",
                                        ""string""
                                      ],
                                      ""default"": null
                                    },
                                    {
                                      ""name"": ""OptionCd"",
                                      ""type"": [
                                        ""null"",
                                        ""string""
                                      ],
                                      ""default"": null
                                    },
                                    {
                                      ""name"": ""OptionTemplateId"",
                                      ""type"": [
                                        ""null"",
                                        ""int""
                                      ],
                                      ""default"": null
                                    },
                                    {
                                      ""name"": ""LotLevelProcessingFlag"",
                                      ""type"": [
                                        ""null"",
                                        ""int""
                                      ],
                                      ""default"": null
                                    },
                                    {
                                      ""name"": ""AccountingMethodBasedFlag"",
                                      ""type"": [
                                        ""null"",
                                        ""int""
                                      ],
                                      ""default"": null
                                    },
                                    {
                                      ""name"": ""ParentLineBookcostPercentage"",
                                      ""type"": [
                                        ""null"",
                                        ""string""
                                      ],
                                      ""default"": null
                                    },
                                    {
                                      ""name"": ""Rates"",
                                      ""type"": [
                                        ""null"",
                                        {
                                          ""type"": ""record"",
                                          ""name"": ""Rates"",
                                          ""namespace"": ""com.capgroup.option.rates"",
                                          ""fields"": [
                                            {
                                              ""name"": ""Rate"",
                                              ""type"": {
                                                ""type"": ""array"",
                                                ""items"": {
                                                  ""type"": ""record"",
                                                  ""name"": ""Rate"",
                                                  ""fields"": [
                                                    {
                                                      ""name"": ""RateQualifierCd"",
                                                      ""type"": [
                                                        ""null"",
                                                        ""string""
                                                      ],
                                                      ""default"": null
                                                    },
                                                    {
                                                      ""name"": ""RateValue"",
                                                      ""type"": [
                                                        ""null"",
                                                        ""double""
                                                      ],
                                                      ""default"": null
                                                    },
                                                    {
                                                      ""name"": ""RateTypeCd"",
                                                      ""type"": [
                                                        ""null"",
                                                        ""string""
                                                      ],
                                                      ""default"": null
                                                    },
                                                    {
                                                      ""name"": ""CurrencyCd"",
                                                      ""type"": [
                                                        ""null"",
                                                        ""string""
                                                      ],
                                                      ""default"": null
                                                    },
                                                    {
                                                      ""name"": ""Amount1"",
                                                      ""type"": [
                                                        ""null"",
                                                        ""double""
                                                      ],
                                                      ""default"": null
                                                    },
                                                    {
                                                      ""name"": ""RateStatusCd"",
                                                      ""type"": [
                                                        ""null"",
                                                        ""string""
                                                      ],
                                                      ""default"": null
                                                    }
                                                  ]
                                                }
                                              }
                                            }
                                          ]
                                        }
                                      ],
                                      ""default"": null
                                    },
                                    {
                                      ""name"": ""Dates"",
                                      ""type"": [
                                        ""null"",
                                        {
                                          ""type"": ""record"",
                                          ""name"": ""Dates"",
                                          ""namespace"": ""com.capgroup.option.dates"",
                                          ""fields"": [
                                            {
                                              ""name"": ""Date"",
                                              ""type"": {
                                                ""type"": ""array"",
                                                ""items"": {
                                                  ""type"": ""record"",
                                                  ""name"": ""Date"",
                                                  ""fields"": [
                                                    {
                                                      ""name"": ""DateQualifierCd"",
                                                      ""type"": [
                                                        ""null"",
                                                        ""string""
                                                      ],
                                                      ""default"": null
                                                    },
                                                    {
                                                      ""name"": ""DateValue"",
                                                      ""type"": [
                                                        ""null"",
                                                        ""double""
                                                      ],
                                                      ""default"": null
                                                    },
                                                    {
                                                      ""name"": ""DateCd"",
                                                      ""type"": [
                                                        ""null"",
                                                        ""string""
                                                      ],
                                                      ""default"": null
                                                    }
                                                  ]
                                                }
                                              }
                                            }
                                          ]
                                        }
                                      ],
                                      ""default"": null
                                    },
                                    {
                                      ""name"": ""CashMovements"",
                                      ""type"": [
                                        ""null"",
                                        {
                                          ""type"": ""record"",
                                          ""name"": ""CashMovements"",
                                          ""fields"": [
                                            {
                                              ""name"": ""CashMovement"",
                                              ""type"": [
                                                ""null"",
                                                {
                                                  ""type"": ""array"",
                                                  ""items"": [
                                                    ""null"",
                                                    {
                                                      ""type"": ""record"",
                                                      ""name"": ""CashMovement"",
                                                      ""fields"": [
                                                        {
                                                          ""name"": ""CashMovementId"",
                                                          ""type"": [
                                                            ""null"",
                                                            ""long""
                                                          ],
                                                          ""default"": null
                                                        },
                                                        {
                                                          ""name"": ""Rates"",
                                                          ""type"": [
                                                            ""null"",
                                                            {
                                                              ""type"": ""record"",
                                                              ""name"": ""Rates"",
                                                              ""namespace"": ""com.capgroup.option.cashmove.rates"",
                                                              ""fields"": [
                                                                {
                                                                  ""name"": ""Rate"",
                                                                  ""type"": {
                                                                    ""type"": ""array"",
                                                                    ""items"": {
                                                                      ""type"": ""record"",
                                                                      ""name"": ""Rate"",
                                                                      ""fields"": [
                                                                        {
                                                                          ""name"": ""RateQualifierCd"",
                                                                          ""type"": [
                                                                            ""null"",
                                                                            ""string""
                                                                          ],
                                                                          ""default"": null
                                                                        },
                                                                        {
                                                                          ""name"": ""RateValue"",
                                                                          ""type"": [
                                                                            ""null"",
                                                                            ""double""
                                                                          ],
                                                                          ""default"": null
                                                                        },
                                                                        {
                                                                          ""name"": ""RateTypeCd"",
                                                                          ""type"": [
                                                                            ""null"",
                                                                            ""string""
                                                                          ],
                                                                          ""default"": null
                                                                        },
                                                                        {
                                                                          ""name"": ""CurrencyCd"",
                                                                          ""type"": [
                                                                            ""null"",
                                                                            ""string""
                                                                          ],
                                                                          ""default"": null
                                                                        },
                                                                        {
                                                                          ""name"": ""Amount1"",
                                                                          ""type"": [
                                                                            ""null"",
                                                                            ""double""
                                                                          ],
                                                                          ""default"": null
                                                                        },
                                                                        {
                                                                          ""name"": ""RateStatusCd"",
                                                                          ""type"": [
                                                                            ""null"",
                                                                            ""string""
                                                                          ],
                                                                          ""default"": null
                                                                        }
                                                                      ]
                                                                    }
                                                                  }
                                                                }
                                                              ]
                                                            }
                                                          ],
                                                          ""default"": null
                                                        },
                                                        {
                                                          ""name"": ""Places"",
                                                          ""type"": [
                                                            ""null"",
                                                            {
                                                              ""type"": ""record"",
                                                              ""name"": ""Places"",
                                                              ""namespace"": ""com.capgroup.option.cashmove.place"",
                                                              ""fields"": [
                                                                {
                                                                  ""name"": ""Place"",
                                                                  ""type"": {
                                                                    ""type"": ""array"",
                                                                    ""items"": {
                                                                      ""type"": ""record"",
                                                                      ""name"": ""Place"",
                                                                      ""fields"": [
                                                                        {
                                                                          ""name"": ""PlaceQualifierCd"",
                                                                          ""type"": [
                                                                            ""null"",
                                                                            ""string""
                                                                          ],
                                                                          ""default"": null
                                                                        },
                                                                        {
                                                                          ""name"": ""PlaceValue"",
                                                                          ""type"": [
                                                                            ""null"",
                                                                            ""string""
                                                                          ],
                                                                          ""default"": null
                                                                        }
                                                                      ]
                                                                    }
                                                                  }
                                                                }
                                                              ]
                                                            }
                                                          ],
                                                          ""default"": null
                                                        },
                                                        {
                                                          ""name"": ""Dates"",
                                                          ""type"": [
                                                            ""null"",
                                                            {
                                                              ""type"": ""record"",
                                                              ""name"": ""Dates"",
                                                              ""namespace"": ""com.capgroup.option.cashmove.dates"",
                                                              ""fields"": [
                                                                {
                                                                  ""name"": ""Date"",
                                                                  ""type"": {
                                                                    ""type"": ""array"",
                                                                    ""items"": {
                                                                      ""type"": ""record"",
                                                                      ""name"": ""Date"",
                                                                      ""fields"": [
                                                                        {
                                                                          ""name"": ""DateQualifierCd"",
                                                                          ""type"": [
                                                                            ""null"",
                                                                            ""string""
                                                                          ],
                                                                          ""default"": null
                                                                        },
                                                                        {
                                                                          ""name"": ""DateValue"",
                                                                          ""type"": [
                                                                            ""null"",
                                                                            ""double""
                                                                          ],
                                                                          ""default"": null
                                                                        },
                                                                        {
                                                                          ""name"": ""DateCd"",
                                                                          ""type"": [
                                                                            ""null"",
                                                                            ""string""
                                                                          ],
                                                                          ""default"": null
                                                                        }
                                                                      ]
                                                                    }
                                                                  }
                                                                }
                                                              ]
                                                            }
                                                          ],
                                                          ""default"": null
                                                        }
                                                      ]
                                                    }
                                                  ]
                                                }
                                              ],
                                              ""default"": null
                                            }
                                          ]
                                        }
                                      ],
                                      ""default"": null
                                    },
                                    {
                                      ""name"": ""SecurityMovements"",
                                      ""type"": [
                                        ""null"",
                                        {
                                          ""type"": ""record"",
                                          ""name"": ""SecurityMovements"",
                                          ""namespace"": ""com.capgroup.security.movement"",
                                          ""fields"": [
                                            {
                                              ""name"": ""SecurityMovement"",
                                              ""type"": [
                                                ""null"",
                                                {
                                                  ""type"": ""array"",
                                                  ""items"": [
                                                    ""null"",
                                                    {
                                                      ""type"": ""record"",
                                                      ""name"": ""SecurityMovement"",
                                                      ""fields"": [
                                                        {
                                                          ""name"": ""SecurityMovementId"",
                                                          ""type"": [
                                                            ""null"",
                                                            ""long""
                                                          ],
                                                          ""default"": null
                                                        },
                                                        {
                                                          ""name"": ""SecurityDetails"",
                                                          ""type"": [
                                                            ""null"",
                                                            {
                                                              ""type"": ""record"",
                                                              ""name"": ""SecurityDetails"",
                                                              ""fields"": [
                                                                {
                                                                  ""name"": ""IssueCountryCd"",
                                                                  ""type"": [
                                                                    ""null"",
                                                                    ""string""
                                                                  ],
                                                                  ""default"": null
                                                                },
                                                                {
                                                                  ""name"": ""IssueCurrencyCd"",
                                                                  ""type"": [
                                                                    ""null"",
                                                                    ""string""
                                                                  ],
                                                                  ""default"": null
                                                                },
                                                                {
                                                                  ""name"": ""SecurityExternalReferences"",
                                                                  ""type"": [
                                                                    ""null"",
                                                                    {
                                                                      ""type"": ""record"",
                                                                      ""name"": ""SecurityExternalReferences"",
                                                                      ""fields"": [
                                                                        {
                                                                          ""name"": ""SecurityExternalReference"",
                                                                          ""type"": {
                                                                            ""type"": ""array"",
                                                                            ""items"": {
                                                                              ""type"": ""record"",
                                                                              ""name"": ""SecurityExternalReference"",
                                                                              ""fields"": [
                                                                                {
                                                                                  ""name"": ""SecurityIdentifierTypeCd"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""string""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""SecurityIdentifierValue"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""string""
                                                                                  ],
                                                                                  ""default"": null
                                                                                }
                                                                              ]
                                                                            }
                                                                          }
                                                                        }
                                                                      ]
                                                                    }
                                                                  ],
                                                                  ""default"": null
                                                                },
                                                                {
                                                                  ""name"": ""ReportingClassificationGroups"",
                                                                  ""type"": [
                                                                    ""null"",
                                                                    {
                                                                      ""type"": ""record"",
                                                                      ""name"": ""ReportingClassificationGroups"",
                                                                      ""namespace"": ""com.capgroup.option.secmove.ReportingClassificationGroups"",
                                                                      ""fields"": [
                                                                        {
                                                                          ""name"": ""ReportingClassificationGroup"",
                                                                          ""type"": {
                                                                            ""type"": ""array"",
                                                                            ""items"": {
                                                                              ""type"": ""record"",
                                                                              ""name"": ""ReportingClassificationGroup"",
                                                                              ""fields"": [
                                                                                {
                                                                                  ""name"": ""ReportingClassificationGroupId"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""long""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""ReportingClassificationGroupName"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""string""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""ValueId"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""long""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""ValueName"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""string""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""ValueCode"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""string""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""ValueSequence"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""string""
                                                                                  ],
                                                                                  ""default"": null
                                                                                }
                                                                              ]
                                                                            }
                                                                          }
                                                                        }
                                                                      ]
                                                                    }
                                                                  ],
                                                                  ""default"": null
                                                                }
                                                              ]
                                                            }
                                                          ],
                                                          ""default"": null
                                                        },
                                                        {
                                                          ""name"": ""Rates"",
                                                          ""type"": [
                                                            ""null"",
                                                            {
                                                              ""type"": ""record"",
                                                              ""name"": ""Rates"",
                                                              ""namespace"": ""com.capgroup.option.secmove.rates"",
                                                              ""fields"": [
                                                                {
                                                                  ""name"": ""Rate"",
                                                                  ""type"": [
                                                                    ""null"",
                                                                    {
                                                                      ""type"": ""array"",
                                                                      ""items"": [
                                                                        ""null"",
                                                                        {
                                                                          ""type"": ""record"",
                                                                          ""name"": ""Rate"",
                                                                          ""fields"": [
                                                                            {
                                                                              ""name"": ""RateQualifierCd"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                ""string""
                                                                              ],
                                                                              ""default"": null
                                                                            },
                                                                            {
                                                                              ""name"": ""Description"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                ""string""
                                                                              ],
                                                                              ""default"": null
                                                                            },
                                                                            {
                                                                              ""name"": ""RateValue"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                ""double""
                                                                              ],
                                                                              ""default"": null
                                                                            },
                                                                            {
                                                                              ""name"": ""ExchangeFromCurrencyCd"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                ""double""
                                                                              ],
                                                                              ""default"": null
                                                                            },
                                                                            {
                                                                              ""name"": ""ExchangeToCurrencyCd"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                ""double""
                                                                              ],
                                                                              ""default"": null
                                                                            },
                                                                            {
                                                                              ""name"": ""RateTypeCd"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                ""string""
                                                                              ],
                                                                              ""default"": null
                                                                            },
                                                                            {
                                                                              ""name"": ""CurrencyCd"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                ""string""
                                                                              ],
                                                                              ""default"": null
                                                                            },
                                                                            {
                                                                              ""name"": ""Amount1"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                ""double""
                                                                              ],
                                                                              ""default"": null
                                                                            },
                                                                            {
                                                                              ""name"": ""Amount2"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                ""double""
                                                                              ],
                                                                              ""default"": null
                                                                            },
                                                                            {
                                                                              ""name"": ""RateStatusCd"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                ""string""
                                                                              ],
                                                                              ""default"": null
                                                                            },
                                                                            {
                                                                              ""name"": ""Quantity1"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                ""double""
                                                                              ],
                                                                              ""default"": null
                                                                            },
                                                                            {
                                                                              ""name"": ""Quantity2"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                ""double""
                                                                              ],
                                                                              ""default"": null
                                                                            },
                                                                            {
                                                                              ""name"": ""CurrencyCd1"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                ""string""
                                                                              ],
                                                                              ""default"": null
                                                                            },
                                                                            {
                                                                              ""name"": ""CurrencyCd2"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                ""string""
                                                                              ],
                                                                              ""default"": null
                                                                            },
                                                                            {
                                                                              ""name"": ""IndexPoint"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                ""double""
                                                                              ],
                                                                              ""default"": null
                                                                            }
                                                                          ]
                                                                        }
                                                                      ]
                                                                    }
                                                                  ],
                                                                  ""default"": null
                                                                }
                                                              ]
                                                            }
                                                          ],
                                                          ""default"": null
                                                        },
                                                        {
                                                          ""name"": ""Places"",
                                                          ""type"": [
                                                            ""null"",
                                                            {
                                                              ""type"": ""record"",
                                                              ""name"": ""Places"",
                                                              ""namespace"": ""com.capgroup.option.secmove.places"",
                                                              ""fields"": [
                                                                {
                                                                  ""name"": ""Place"",
                                                                  ""type"": [
                                                                    ""null"",
                                                                    {
                                                                      ""type"": ""array"",
                                                                      ""items"": [
                                                                        ""null"",
                                                                        {
                                                                          ""type"": ""record"",
                                                                          ""name"": ""Place"",
                                                                          ""fields"": [
                                                                            {
                                                                              ""name"": ""PlaceQualifierCd"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                ""string""
                                                                              ],
                                                                              ""default"": null
                                                                            },
                                                                            {
                                                                              ""name"": ""PlaceValue"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                ""string""
                                                                              ],
                                                                              ""default"": null
                                                                            }
                                                                          ]
                                                                        }
                                                                      ]
                                                                    }
                                                                  ],
                                                                  ""default"": null
                                                                }
                                                              ]
                                                            }
                                                          ],
                                                          ""default"": null
                                                        },
                                                        {
                                                          ""name"": ""StockDirectionInd"",
                                                          ""type"": [
                                                            ""null"",
                                                            ""string""
                                                          ],
                                                          ""default"": null
                                                        },
                                                        {
                                                          ""name"": ""PDEFPrerefundedSecurityFlag"",
                                                          ""type"": [
                                                            ""null"",
                                                            ""string""
                                                          ],
                                                          ""default"": null
                                                        },
                                                        {
                                                          ""name"": ""CostBasisAllocationPercentage"",
                                                          ""type"": [
                                                            ""null"",
                                                            ""double""
                                                          ],
                                                          ""default"": null
                                                        },
                                                        {
                                                          ""name"": ""Dates"",
                                                          ""type"": [
                                                            ""null"",
                                                            {
                                                              ""type"": ""record"",
                                                              ""name"": ""Dates"",
                                                              ""namespace"": ""com.capgroup.option.secmove.dates"",
                                                              ""fields"": [
                                                                {
                                                                  ""name"": ""Date"",
                                                                  ""type"": [
                                                                    ""null"",
                                                                    {
                                                                      ""type"": ""array"",
                                                                      ""items"": [
                                                                        ""null"",
                                                                        {
                                                                          ""type"": ""record"",
                                                                          ""name"": ""Date"",
                                                                          ""fields"": [
                                                                            {
                                                                              ""name"": ""DateQualifierCd"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                ""string""
                                                                              ],
                                                                              ""default"": null
                                                                            },
                                                                            {
                                                                              ""name"": ""DateValue"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                ""double""
                                                                              ],
                                                                              ""default"": null
                                                                            },
                                                                            {
                                                                              ""name"": ""DateCd"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                ""string""
                                                                              ],
                                                                              ""default"": null
                                                                            }
                                                                          ]
                                                                        }
                                                                      ]
                                                                    }
                                                                  ],
                                                                  ""default"": null
                                                                }
                                                              ]
                                                            }
                                                          ],
                                                          ""default"": null
                                                        }
                                                      ]
                                                    }
                                                  ]
                                                }
                                              ],
                                              ""default"": null
                                            }
                                          ]
                                        }
                                      ],
                                      ""default"": null
                                    }
                                  ]
                                }
                              }
                            }
                          ]
                        }
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""IcaInstructions"",
                      ""type"": [
                        ""null"",
                        {
                          ""type"": ""record"",
                          ""name"": ""IcaInstructions"",
                          ""namespace"": ""com.capgroup.ica.instructions"",
                          ""fields"": [
                            {
                              ""name"": ""IcaInstruction"",
                              ""type"": [
                                ""null"",
                                {
                                  ""type"": ""array"",
                                  ""items"": [
                                    ""null"",
                                    {
                                      ""type"": ""record"",
                                      ""name"": ""IcaInstruction"",
                                      ""fields"": [
                                        {
                                          ""name"": ""CustodyAccount"",
                                          ""type"": [
                                            ""null"",
                                            {
                                              ""type"": ""record"",
                                              ""name"": ""CustodyAccount"",
                                              ""fields"": [
                                                {
                                                  ""name"": ""AccountExternalReference"",
                                                  ""type"": [
                                                    ""null"",
                                                    {
                                                      ""type"": ""record"",
                                                      ""name"": ""AccountExternalReference"",
                                                      ""fields"": [
                                                        {
                                                          ""name"": ""ExternalRefType"",
                                                          ""type"": [
                                                            ""null"",
                                                            ""string""
                                                          ],
                                                          ""default"": null
                                                        },
                                                        {
                                                          ""name"": ""ExternalRefValue"",
                                                          ""type"": [
                                                            ""null"",
                                                            ""string""
                                                          ],
                                                          ""default"": null
                                                        }
                                                      ]
                                                    }
                                                  ],
                                                  ""default"": null
                                                }
                                              ]
                                            }
                                          ],
                                          ""default"": null
                                        },
                                        {
                                          ""name"": ""Option"",
                                          ""type"": [
                                            {
                                              ""type"": ""array"",
                                              ""items"": {
                                                ""type"": ""record"",
                                                ""name"": ""Option"",
                                                ""fields"": [
                                                  {
                                                    ""name"": ""OptionId"",
                                                    ""type"": [
                                                      ""null"",
                                                      ""long""
                                                    ],
                                                    ""default"": null
                                                  },
                                                  {
                                                    ""name"": ""OptionNumber"",
                                                    ""type"": [
                                                      ""null"",
                                                      ""string""
                                                    ],
                                                    ""default"": null
                                                  },
                                                  {
                                                    ""name"": ""OptionCd"",
                                                    ""type"": [
                                                      ""null"",
                                                      ""string""
                                                    ],
                                                    ""default"": null
                                                  },
                                                  {
                                                    ""name"": ""OptionTemplateId"",
                                                    ""type"": [
                                                      ""null"",
                                                      ""string""
                                                    ],
                                                    ""default"": null
                                                  },
                                                  {
                                                    ""name"": ""CustodyAccountIcaInstruction"",
                                                    ""type"": [
                                                      ""null"",
                                                      {
                                                        ""type"": ""record"",
                                                        ""name"": ""CustodyAccountIcaInstruction"",
                                                        ""fields"": [
                                                          {
                                                            ""name"": ""CashAccount"",
                                                            ""type"": [
                                                              ""null"",
                                                              {
                                                                ""type"": ""array"",
                                                                ""items"": [
                                                                  ""null"",
                                                                  {
                                                                    ""type"": ""record"",
                                                                    ""name"": ""CashAccount"",
                                                                    ""namespace"": ""com.capgroup.custInst.cashaccount"",
                                                                    ""fields"": [
                                                                      {
                                                                        ""name"": ""AccountExternalReference"",
                                                                        ""type"": [
                                                                          ""null"",
                                                                          {
                                                                            ""type"": ""record"",
                                                                            ""name"": ""AccountExternalReference"",
                                                                            ""fields"": [
                                                                              {
                                                                                ""name"": ""ExternalRefType"",
                                                                                ""type"": [
                                                                                  ""null"",
                                                                                  ""string""
                                                                                ],
                                                                                ""default"": null
                                                                              },
                                                                              {
                                                                                ""name"": ""ExternalRefValue"",
                                                                                ""type"": [
                                                                                  ""null"",
                                                                                  ""string""
                                                                                ],
                                                                                ""default"": null
                                                                              }
                                                                            ]
                                                                          }
                                                                        ],
                                                                        ""default"": null
                                                                      },
                                                                      {
                                                                        ""name"": ""CashAccountType"",
                                                                        ""type"": [
                                                                          ""null"",
                                                                          ""string""
                                                                        ],
                                                                        ""default"": null
                                                                      },
                                                                      {
                                                                        ""name"": ""CashAccountCurrencyCd"",
                                                                        ""type"": [
                                                                          ""null"",
                                                                          ""string""
                                                                        ],
                                                                        ""default"": null
                                                                      }
                                                                    ]
                                                                  }
                                                                ]
                                                              }
                                                            ],
                                                            ""default"": null
                                                          },
                                                          {
                                                            ""name"": ""Allocation"",
                                                            ""type"": [
                                                              {
                                                                ""type"": ""array"",
                                                                ""items"": {
                                                                  ""type"": ""record"",
                                                                  ""name"": ""Allocation"",
                                                                  ""fields"": [
                                                                    {
                                                                      ""name"": ""AllocationReferences"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        {
                                                                          ""type"": ""record"",
                                                                          ""name"": ""AllocationReferences"",
                                                                          ""fields"": [
                                                                            {
                                                                              ""name"": ""AllocationReference"",
                                                                              ""type"": [
                                                                                {
                                                                                  ""type"": ""array"",
                                                                                  ""items"": {
                                                                                    ""type"": ""record"",
                                                                                    ""name"": ""AllocationReference"",
                                                                                    ""fields"": [
                                                                                      {
                                                                                        ""name"": ""ReferenceTypeCd"",
                                                                                        ""type"": [
                                                                                          ""null"",
                                                                                          ""string""
                                                                                        ],
                                                                                        ""default"": null
                                                                                      },
                                                                                      {
                                                                                        ""name"": ""ReferenceValue"",
                                                                                        ""type"": [
                                                                                          ""null"",
                                                                                          ""string""
                                                                                        ],
                                                                                        ""default"": null
                                                                                      }
                                                                                    ]
                                                                                  }
                                                                                }
                                                                              ]
                                                                            }
                                                                          ]
                                                                        }
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""SequenceNumber"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        ""long""
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""AllocationStatusCd"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        ""string""
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""Source"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        ""string""
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""AllocationKeyValues"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        {
                                                                          ""type"": ""record"",
                                                                          ""name"": ""AllocationKeyValues"",
                                                                          ""namespace"": ""com.capgroup.allocation.keyvalue"",
                                                                          ""fields"": [
                                                                            {
                                                                              ""name"": ""AllocationKeyValue"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                {
                                                                                  ""type"": ""array"",
                                                                                  ""items"": [
                                                                                    ""null"",
                                                                                    {
                                                                                      ""type"": ""record"",
                                                                                      ""name"": ""AllocationKeyValue"",
                                                                                      ""fields"": [
                                                                                        {
                                                                                          ""name"": ""Key"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""Value"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""Value2"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""Value3"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        }
                                                                                      ]
                                                                                    }
                                                                                  ]
                                                                                }
                                                                              ],
                                                                              ""default"": null
                                                                            }
                                                                          ]
                                                                        }
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""ContractualCashSettlementFlag"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        ""string""
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""ContractualStockSettlementFlag"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        ""string""
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""TransactionTypeCd"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        ""string""
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""ExpectedSettlementDate"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        ""string""
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""OverrideSettlementDate"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        ""string""
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""ElectionId"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        ""long""
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""ElectionDetails"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        {
                                                                          ""type"": ""record"",
                                                                          ""name"": ""ElectionDetails"",
                                                                          ""namespace"": ""com.capgroup.allocation.electiondetails"",
                                                                          ""fields"": [
                                                                            {
                                                                              ""name"": ""ElectionDetail"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                {
                                                                                  ""type"": ""array"",
                                                                                  ""items"": [
                                                                                    ""null"",
                                                                                    {
                                                                                      ""type"": ""record"",
                                                                                      ""name"": ""ElectionDetail"",
                                                                                      ""fields"": [
                                                                                        {
                                                                                          ""name"": ""ElectionDetailId"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""long""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""ElectionQuantity"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        }
                                                                                      ]
                                                                                    }
                                                                                  ]
                                                                                }
                                                                              ],
                                                                              ""default"": null
                                                                            }
                                                                          ]
                                                                        }
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""BlockingPositionId"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        ""long""
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""FXRequestId"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        ""long""
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""ApportionedPosition"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        ""double""
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""PositionType"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        ""double""
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""ClaimAssociation"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        ""string""
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""TradeReference"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        {
                                                                          ""type"": ""array"",
                                                                          ""items"": [
                                                                            ""null"",
                                                                            {
                                                                              ""type"": ""record"",
                                                                              ""name"": ""TradeReference"",
                                                                              ""fields"": [
                                                                                {
                                                                                  ""name"": ""ReferenceTypeCd"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""string""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""ReferenceValue"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""string""
                                                                                  ],
                                                                                  ""default"": null
                                                                                }
                                                                              ]
                                                                            }
                                                                          ]
                                                                        }
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""Counterparty"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        {
                                                                          ""type"": ""array"",
                                                                          ""items"": [
                                                                            ""null"",
                                                                            {
                                                                              ""type"": ""record"",
                                                                              ""name"": ""Counterparty"",
                                                                              ""fields"": [
                                                                                {
                                                                                  ""name"": ""ExternalRefType"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""string""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""ExternalRefValue"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""string""
                                                                                  ],
                                                                                  ""default"": null
                                                                                }
                                                                              ]
                                                                            }
                                                                          ]
                                                                        }
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""CashMovement"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        {
                                                                          ""type"": ""array"",
                                                                          ""items"": [
                                                                            ""null"",
                                                                            {
                                                                              ""type"": ""record"",
                                                                              ""name"": ""CashMovement"",
                                                                              ""fields"": [
                                                                                {
                                                                                  ""name"": ""CashMovementId"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""long""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""Rate"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""double""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""CreditDebitIndicator"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""string""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""CashAmounts"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    {
                                                                                      ""type"": ""record"",
                                                                                      ""name"": ""CashAmounts"",
                                                                                      ""namespace"": ""com.capgroup.custaccount.cashmove.cashmmounts"",
                                                                                      ""fields"": [
                                                                                        {
                                                                                          ""name"": ""TradeAmount"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            {
                                                                                              ""type"": ""record"",
                                                                                              ""name"": ""TradeAmount"",
                                                                                              ""fields"": [
                                                                                                {
                                                                                                  ""name"": ""CustodianNetAmount"",
                                                                                                  ""type"": [
                                                                                                    ""null"",
                                                                                                    ""double""
                                                                                                  ],
                                                                                                  ""default"": null
                                                                                                },
                                                                                                {
                                                                                                  ""name"": ""NetAmount"",
                                                                                                  ""type"": [
                                                                                                    ""null"",
                                                                                                    ""double""
                                                                                                  ],
                                                                                                  ""default"": null
                                                                                                },
                                                                                                {
                                                                                                  ""name"": ""GrossAmount"",
                                                                                                  ""type"": [
                                                                                                    ""null"",
                                                                                                    ""double""
                                                                                                  ],
                                                                                                  ""default"": null
                                                                                                },
                                                                                                {
                                                                                                  ""name"": ""Currency"",
                                                                                                  ""type"": [
                                                                                                    ""null"",
                                                                                                    ""string""
                                                                                                  ],
                                                                                                  ""default"": null
                                                                                                }
                                                                                              ]
                                                                                            }
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""TradeToSettlementFxRate"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""CustodianPaymentAmount"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            {
                                                                                              ""type"": ""array"",
                                                                                              ""items"": [
                                                                                                ""null"",
                                                                                                {
                                                                                                  ""type"": ""record"",
                                                                                                  ""name"": ""CustodianPaymentAmount"",
                                                                                                  ""fields"": [
                                                                                                    {
                                                                                                      ""name"": ""CustodianPaymentCurrency"",
                                                                                                      ""type"": [
                                                                                                        ""null"",
                                                                                                        ""string""
                                                                                                      ],
                                                                                                      ""default"": null
                                                                                                    },
                                                                                                    {
                                                                                                      ""name"": ""SettlementAmountInCustodianCurrency"",
                                                                                                      ""type"": [
                                                                                                        ""null"",
                                                                                                        ""string""
                                                                                                      ],
                                                                                                      ""default"": null
                                                                                                    },
                                                                                                    {
                                                                                                      ""name"": ""CustodianPaymentToSettlementFxRate"",
                                                                                                      ""type"": [
                                                                                                        ""null"",
                                                                                                        ""string""
                                                                                                      ],
                                                                                                      ""default"": null
                                                                                                    },
                                                                                                    {
                                                                                                      ""name"": ""CustodianPaymentToTradeFxRate"",
                                                                                                      ""type"": [
                                                                                                        ""null"",
                                                                                                        ""string""
                                                                                                      ],
                                                                                                      ""default"": null
                                                                                                    }
                                                                                                  ]
                                                                                                }
                                                                                              ]
                                                                                            }
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""SettlementAmount"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            {
                                                                                              ""type"": ""record"",
                                                                                              ""name"": ""SettlementAmount"",
                                                                                              ""fields"": [
                                                                                                {
                                                                                                  ""name"": ""NetAmount"",
                                                                                                  ""type"": [
                                                                                                    ""null"",
                                                                                                    ""double""
                                                                                                  ],
                                                                                                  ""default"": null
                                                                                                },
                                                                                                {
                                                                                                  ""name"": ""AggregatedSettledAmount"",
                                                                                                  ""type"": [
                                                                                                    ""null"",
                                                                                                    ""double""
                                                                                                  ],
                                                                                                  ""default"": null
                                                                                                },
                                                                                                {
                                                                                                  ""name"": ""RemainingSettlementAmount"",
                                                                                                  ""type"": [
                                                                                                    ""null"",
                                                                                                    ""double""
                                                                                                  ],
                                                                                                  ""default"": null
                                                                                                },
                                                                                                {
                                                                                                  ""name"": ""Currency"",
                                                                                                  ""type"": [
                                                                                                    ""null"",
                                                                                                    ""string""
                                                                                                  ],
                                                                                                  ""default"": null
                                                                                                },
                                                                                                {
                                                                                                  ""name"": ""SettlementAmountDifference"",
                                                                                                  ""type"": [
                                                                                                    ""null"",
                                                                                                    ""double""
                                                                                                  ],
                                                                                                  ""default"": null
                                                                                                }
                                                                                              ]
                                                                                            }
                                                                                          ],
                                                                                          ""default"": null
                                                                                        }
                                                                                      ]
                                                                                    }
                                                                                  ],
                                                                                  ""default"": null
                                                                                }
                                                                              ]
                                                                            }
                                                                          ],
                                                                          ""default"": null
                                                                        }
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""SecurityMovement"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        {
                                                                          ""type"": ""array"",
                                                                          ""items"": [
                                                                            ""null"",
                                                                            {
                                                                              ""type"": ""record"",
                                                                              ""name"": ""SecurityMovement"",
                                                                              ""fields"": [
                                                                                {
                                                                                  ""name"": ""SecurityMovementId"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""long""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""StockDirectionInd"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""string""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""SecurityQuantity"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""double""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""AggregatedSettledSecurityQuantity"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""double""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""RemainingSettlementSecurityQuantity"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""double""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""CostPriceSecurityCurrency"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""double""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""PriceInTradeCurrency"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""double""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""SecurityDetails"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    {
                                                                                      ""type"": ""array"",
                                                                                      ""items"": [
                                                                                        ""null"",
                                                                                        {
                                                                                          ""type"": ""record"",
                                                                                          ""name"": ""SecurityDetails"",
                                                                                          ""fields"": [
                                                                                            {
                                                                                              ""name"": ""IssueCountryCd"",
                                                                                              ""type"": [
                                                                                                ""null"",
                                                                                                ""string""
                                                                                              ],
                                                                                              ""default"": null
                                                                                            },
                                                                                            {
                                                                                              ""name"": ""IssueCurrencyCd"",
                                                                                              ""type"": [
                                                                                                ""null"",
                                                                                                ""string""
                                                                                              ],
                                                                                              ""default"": null
                                                                                            },
                                                                                            {
                                                                                              ""name"": ""SecurityExternalReferences"",
                                                                                              ""type"": [
                                                                                                ""null"",
                                                                                                {
                                                                                                  ""type"": ""record"",
                                                                                                  ""name"": ""SecurityExternalReferences"",
                                                                                                  ""fields"": [
                                                                                                    {
                                                                                                      ""name"": ""SecurityExternalReference"",
                                                                                                      ""type"": [
                                                                                                        {
                                                                                                          ""type"": ""array"",
                                                                                                          ""items"": {
                                                                                                            ""type"": ""record"",
                                                                                                            ""name"": ""SecurityExternalReference"",
                                                                                                            ""fields"": [
                                                                                                              {
                                                                                                                ""name"": ""SecurityIdentifierTypeCd"",
                                                                                                                ""type"": [
                                                                                                                  ""null"",
                                                                                                                  ""string""
                                                                                                                ],
                                                                                                                ""default"": null
                                                                                                              },
                                                                                                              {
                                                                                                                ""name"": ""SecurityIdentifierValue"",
                                                                                                                ""type"": [
                                                                                                                  ""null"",
                                                                                                                  ""string""
                                                                                                                ],
                                                                                                                ""default"": null
                                                                                                              }
                                                                                                            ]
                                                                                                          }
                                                                                                        }
                                                                                                      ]
                                                                                                    }
                                                                                                  ]
                                                                                                }
                                                                                              ],
                                                                                              ""default"": null
                                                                                            },
                                                                                            {
                                                                                              ""name"": ""ReportingClassificationGroups"",
                                                                                              ""type"": [
                                                                                                ""null"",
                                                                                                {
                                                                                                  ""type"": ""record"",
                                                                                                  ""name"": ""ReportingClassificationGroups"",
                                                                                                  ""fields"": [
                                                                                                    {
                                                                                                      ""name"": ""ReportingClassificationGroup"",
                                                                                                      ""type"": [
                                                                                                        ""null"",
                                                                                                        {
                                                                                                          ""type"": ""array"",
                                                                                                          ""items"": [
                                                                                                            {
                                                                                                              ""type"": ""record"",
                                                                                                              ""name"": ""ReportingClassificationGroup"",
                                                                                                              ""fields"": [
                                                                                                                {
                                                                                                                  ""name"": ""ReportingClassificationGroupId"",
                                                                                                                  ""type"": [
                                                                                                                    ""null"",
                                                                                                                    ""long""
                                                                                                                  ],
                                                                                                                  ""default"": null
                                                                                                                },
                                                                                                                {
                                                                                                                  ""name"": ""ReportingClassificationGroupName"",
                                                                                                                  ""type"": [
                                                                                                                    ""null"",
                                                                                                                    ""string""
                                                                                                                  ],
                                                                                                                  ""default"": null
                                                                                                                },
                                                                                                                {
                                                                                                                  ""name"": ""ValueId"",
                                                                                                                  ""type"": [
                                                                                                                    ""null"",
                                                                                                                    ""long""
                                                                                                                  ],
                                                                                                                  ""default"": null
                                                                                                                },
                                                                                                                {
                                                                                                                  ""name"": ""ValueName"",
                                                                                                                  ""type"": [
                                                                                                                    ""null"",
                                                                                                                    ""string""
                                                                                                                  ],
                                                                                                                  ""default"": null
                                                                                                                },
                                                                                                                {
                                                                                                                  ""name"": ""ValueCode"",
                                                                                                                  ""type"": [
                                                                                                                    ""null"",
                                                                                                                    ""string""
                                                                                                                  ],
                                                                                                                  ""default"": null
                                                                                                                },
                                                                                                                {
                                                                                                                  ""name"": ""ValueSequence"",
                                                                                                                  ""type"": [
                                                                                                                    ""null"",
                                                                                                                    ""string""
                                                                                                                  ],
                                                                                                                  ""default"": null
                                                                                                                }
                                                                                                              ]
                                                                                                            }
                                                                                                          ]
                                                                                                        }
                                                                                                      ],
                                                                                                      ""default"": null
                                                                                                    }
                                                                                                  ]
                                                                                                }
                                                                                              ],
                                                                                              ""default"": null
                                                                                            }
                                                                                          ]
                                                                                        }
                                                                                      ]
                                                                                    }
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""CashAmounts"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    {
                                                                                      ""type"": ""record"",
                                                                                      ""name"": ""CashAmounts"",
                                                                                      ""namespace"": ""com.capgroup.custaccount.secmove.cashmmounts"",
                                                                                      ""fields"": [
                                                                                        {
                                                                                          ""name"": ""TradeAmount"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            {
                                                                                              ""type"": ""record"",
                                                                                              ""name"": ""TradeAmount"",
                                                                                              ""fields"": [
                                                                                                {
                                                                                                  ""name"": ""CustodianNetAmount"",
                                                                                                  ""type"": [
                                                                                                    ""null"",
                                                                                                    ""double""
                                                                                                  ],
                                                                                                  ""default"": null
                                                                                                },
                                                                                                {
                                                                                                  ""name"": ""NetAmount"",
                                                                                                  ""type"": [
                                                                                                    ""null"",
                                                                                                    ""double""
                                                                                                  ],
                                                                                                  ""default"": null
                                                                                                },
                                                                                                {
                                                                                                  ""name"": ""GrossAmount"",
                                                                                                  ""type"": [
                                                                                                    ""null"",
                                                                                                    ""double""
                                                                                                  ],
                                                                                                  ""default"": null
                                                                                                },
                                                                                                {
                                                                                                  ""name"": ""Currency"",
                                                                                                  ""type"": [
                                                                                                    ""null"",
                                                                                                    ""string""
                                                                                                  ],
                                                                                                  ""default"": null
                                                                                                }
                                                                                              ]
                                                                                            }
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""TradeToSettlementFxRate"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""CustodianPaymentAmount"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            {
                                                                                              ""type"": ""array"",
                                                                                              ""items"": [
                                                                                                ""null"",
                                                                                                {
                                                                                                  ""type"": ""record"",
                                                                                                  ""name"": ""CustodianPaymentAmount"",
                                                                                                  ""namespace"": ""com.capgroup.custaccount.custpayamount"",
                                                                                                  ""fields"": [
                                                                                                    {
                                                                                                      ""name"": ""CustodianPaymentCurrency"",
                                                                                                      ""type"": [
                                                                                                        ""null"",
                                                                                                        ""string""
                                                                                                      ],
                                                                                                      ""default"": null
                                                                                                    },
                                                                                                    {
                                                                                                      ""name"": ""SettlementAmountInCustodianCurrency"",
                                                                                                      ""type"": [
                                                                                                        ""null"",
                                                                                                        ""double""
                                                                                                      ],
                                                                                                      ""default"": null
                                                                                                    },
                                                                                                    {
                                                                                                      ""name"": ""CustodianPaymentToSettlementFxRate"",
                                                                                                      ""type"": [
                                                                                                        ""null"",
                                                                                                        ""double""
                                                                                                      ],
                                                                                                      ""default"": null
                                                                                                    },
                                                                                                    {
                                                                                                      ""name"": ""CustodianPaymentToTradeFxRate"",
                                                                                                      ""type"": [
                                                                                                        ""null"",
                                                                                                        ""double""
                                                                                                      ],
                                                                                                      ""default"": null
                                                                                                    }
                                                                                                  ]
                                                                                                }
                                                                                              ]
                                                                                            }
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""SettlementAmount"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            {
                                                                                              ""type"": ""record"",
                                                                                              ""name"": ""SettlementAmount"",
                                                                                              ""fields"": [
                                                                                                {
                                                                                                  ""name"": ""NetAmount"",
                                                                                                  ""type"": [
                                                                                                    ""null"",
                                                                                                    ""double""
                                                                                                  ],
                                                                                                  ""default"": null
                                                                                                },
                                                                                                {
                                                                                                  ""name"": ""AggregatedSettledAmount"",
                                                                                                  ""type"": [
                                                                                                    ""null"",
                                                                                                    ""double""
                                                                                                  ],
                                                                                                  ""default"": null
                                                                                                },
                                                                                                {
                                                                                                  ""name"": ""RemainingSettlementAmount"",
                                                                                                  ""type"": [
                                                                                                    ""null"",
                                                                                                    ""double""
                                                                                                  ],
                                                                                                  ""default"": null
                                                                                                },
                                                                                                {
                                                                                                  ""name"": ""Currency"",
                                                                                                  ""type"": [
                                                                                                    ""null"",
                                                                                                    ""string""
                                                                                                  ],
                                                                                                  ""default"": null
                                                                                                },
                                                                                                {
                                                                                                  ""name"": ""SettlementAmountDifference"",
                                                                                                  ""type"": [
                                                                                                    ""null"",
                                                                                                    ""string""
                                                                                                  ],
                                                                                                  ""default"": null
                                                                                                }
                                                                                              ]
                                                                                            }
                                                                                          ],
                                                                                          ""default"": null
                                                                                        }
                                                                                      ]
                                                                                    }
                                                                                  ],
                                                                                  ""default"": null
                                                                                }
                                                                              ]
                                                                            }
                                                                          ]
                                                                        }
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""BookCost"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        {
                                                                          ""type"": ""array"",
                                                                          ""items"": [
                                                                            ""null"",
                                                                            {
                                                                              ""type"": ""record"",
                                                                              ""name"": ""BookCost"",
                                                                              ""fields"": [
                                                                                {
                                                                                  ""name"": ""BookCostIsEstimateFlag"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""string""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""CleanBookCostInBaseCurr"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""double""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""CleanBookCostInSecCurr"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""double""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""DirtyBookCostInBaseCurr"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""double""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""DirtyBookCostInSecCurr"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""double""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""TradeToSecFxRate"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""double""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""TradeToBaseFxRate"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""double""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""ClientBaseCurrency"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""string""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""BookCostCurr"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""string""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""DirtyBookCostInBookCostCurr"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""double""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""CleanBookCostInBookCostCurr"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""double""
                                                                                  ],
                                                                                  ""default"": null
                                                                                }
                                                                              ]
                                                                            }
                                                                          ]
                                                                        }
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""Charges"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        {
                                                                          ""type"": ""record"",
                                                                          ""name"": ""Charges"",
                                                                          ""namespace"": ""com.capgroup.option.Charges"",
                                                                          ""fields"": [
                                                                            {
                                                                              ""name"": ""Charge"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                {
                                                                                  ""type"": ""array"",
                                                                                  ""items"": [
                                                                                    ""null"",
                                                                                    {
                                                                                      ""type"": ""record"",
                                                                                      ""name"": ""Charge"",
                                                                                      ""fields"": [
                                                                                        {
                                                                                          ""name"": ""ChargeId"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""long""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""ChargeClassificationType"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""ChargeCategoryCd"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""ChargeCd"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""ChargeableQuantity"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""ChargeCurrencyCd"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""ChargeableAmount"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""RateCd"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""ChargeRateTypeCd"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""ChargeRate"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""VatRate"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""VatTypeCd"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""VatAmount"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""GrossChargeAmount"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""ChargeAmount"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""ChargeSourceCd"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""ChargeOriginCd"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""ApplyChargeAsCd"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""EstimateFlag"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""ReportableOnly"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""ReclaimFlag"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""ReclaimChargeRate"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""ChargeReference"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""QualifierCd"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        }
                                                                                      ]
                                                                                    }
                                                                                  ]
                                                                                }
                                                                              ]
                                                                            }
                                                                          ]
                                                                        }
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""SettlementDetails"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        {
                                                                          ""type"": ""record"",
                                                                          ""name"": ""SettlementDetails"",
                                                                          ""fields"": [
                                                                            {
                                                                              ""name"": ""SettlementDetail"",
                                                                              ""type"": [
                                                                                ""null"",
                                                                                {
                                                                                  ""type"": ""array"",
                                                                                  ""items"": [
                                                                                    ""null"",
                                                                                    {
                                                                                      ""type"": ""record"",
                                                                                      ""name"": ""SettlementDetail"",
                                                                                      ""fields"": [
                                                                                        {
                                                                                          ""name"": ""CreationDate"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""StatusId"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""long""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""AllocationStatusCd"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""StatusActionCd"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""CashValueDate"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""CashPostingDate"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""SettlementAmount"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""AggregatedSettledAmount"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""RemainingSettlementAmount"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""StockValueDate"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""StockPostingDate"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""SettlementSecurityQuantity"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""AggregatedSettledSecurityQuantity"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""RemainingSettlementSecurityQuantity"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""Charges"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            {
                                                                                              ""type"": ""record"",
                                                                                              ""name"": ""Charges"",
                                                                                              ""namespace"": ""com.capgroup.option.settlement.charges"",
                                                                                              ""fields"": [
                                                                                                {
                                                                                                  ""name"": ""Charge"",
                                                                                                  ""type"": [
                                                                                                    {
                                                                                                      ""type"": ""array"",
                                                                                                      ""items"": {
                                                                                                        ""type"": ""record"",
                                                                                                        ""name"": ""Charge"",
                                                                                                        ""fields"": [
                                                                                                          {
                                                                                                            ""name"": ""ChargeId"",
                                                                                                            ""type"": [
                                                                                                              ""null"",
                                                                                                              ""long""
                                                                                                            ],
                                                                                                            ""default"": null
                                                                                                          },
                                                                                                          {
                                                                                                            ""name"": ""SettlementChargeAmount"",
                                                                                                            ""type"": [
                                                                                                              ""null"",
                                                                                                              ""double""
                                                                                                            ],
                                                                                                            ""default"": null
                                                                                                          }
                                                                                                        ]
                                                                                                      }
                                                                                                    }
                                                                                                  ]
                                                                                                }
                                                                                              ]
                                                                                            }
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""ReasonCd"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""double""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""Comments"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        }
                                                                                      ]
                                                                                    }
                                                                                  ]
                                                                                }
                                                                              ]
                                                                            }
                                                                          ]
                                                                        }
                                                                      ],
                                                                      ""default"": null
                                                                    },
                                                                    {
                                                                      ""name"": ""CashAccount"",
                                                                      ""type"": [
                                                                        ""null"",
                                                                        {
                                                                          ""type"": ""array"",
                                                                          ""items"": [
                                                                            ""null"",
                                                                            {
                                                                              ""type"": ""record"",
                                                                              ""name"": ""CashAccount"",
                                                                              ""namespace"": ""com.capgroup.custInst.alloc.cashaccount"",
                                                                              ""fields"": [
                                                                                {
                                                                                  ""name"": ""AccountExternalReference"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    {
                                                                                      ""type"": ""record"",
                                                                                      ""name"": ""AccountExternalReference"",
                                                                                      ""fields"": [
                                                                                        {
                                                                                          ""name"": ""ExternalRefType"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        },
                                                                                        {
                                                                                          ""name"": ""ExternalRefValue"",
                                                                                          ""type"": [
                                                                                            ""null"",
                                                                                            ""string""
                                                                                          ],
                                                                                          ""default"": null
                                                                                        }
                                                                                      ]
                                                                                    }
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""CashAccountType"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""string""
                                                                                  ],
                                                                                  ""default"": null
                                                                                },
                                                                                {
                                                                                  ""name"": ""CashAccountCurrencyCd"",
                                                                                  ""type"": [
                                                                                    ""null"",
                                                                                    ""string""
                                                                                  ],
                                                                                  ""default"": null
                                                                                }
                                                                              ]
                                                                            }
                                                                          ]
                                                                        }
                                                                      ],
                                                                      ""default"": null
                                                                    }
                                                                  ]
                                                                }
                                                              }
                                                            ]
                                                          }
                                                        ]
                                                      }
                                                    ],
                                                    ""default"": null
                                                  },
                                                  {
                                                    ""name"": ""InvestmentAccountIcaInstructions"",
                                                    ""type"": [
                                                      ""null"",
                                                      {
                                                        ""type"": ""record"",
                                                        ""name"": ""InvestmentAccountIcaInstructions"",
                                                        ""namespace"": ""com.capgroup.inica.instructions.investment.account"",
                                                        ""fields"": [
                                                          {
                                                            ""name"": ""InvestmentAccountIcaInstruction"",
                                                            ""type"": [
                                                              ""null"",
                                                              {
                                                                ""type"": ""array"",
                                                                ""items"": [
                                                                  ""null"",
                                                                  {
                                                                    ""type"": ""record"",
                                                                    ""name"": ""InvestmentAccountIcaInstruction"",
                                                                    ""fields"": [
                                                                      {
                                                                        ""name"": ""InvestmentAccount"",
                                                                        ""type"": [
                                                                          ""null"",
                                                                          {
                                                                            ""type"": ""record"",
                                                                            ""name"": ""InvestmentAccount"",
                                                                            ""fields"": [
                                                                              {
                                                                                ""name"": ""AccountExternalReference"",
                                                                                ""type"": [
                                                                                  ""null"",
                                                                                  {
                                                                                    ""type"": ""record"",
                                                                                    ""name"": ""AccountExternalReference"",
                                                                                    ""fields"": [
                                                                                      {
                                                                                        ""name"": ""ExternalRefType"",
                                                                                        ""type"": [
                                                                                          ""null"",
                                                                                          ""string""
                                                                                        ],
                                                                                        ""default"": null
                                                                                      },
                                                                                      {
                                                                                        ""name"": ""ExternalRefValue"",
                                                                                        ""type"": [
                                                                                          ""null"",
                                                                                          ""string""
                                                                                        ],
                                                                                        ""default"": null
                                                                                      }
                                                                                    ]
                                                                                  }
                                                                                ],
                                                                                ""default"": null
                                                                              },
                                                                              {
                                                                                ""name"": ""InvestmentAccountType"",
                                                                                ""type"": [
                                                                                  ""null"",
                                                                                  ""string""
                                                                                ],
                                                                                ""default"": null
                                                                              },
                                                                              {
                                                                                ""name"": ""TaxResidencies"",
                                                                                ""type"": [
                                                                                  ""null"",
                                                                                  {
                                                                                    ""type"": ""record"",
                                                                                    ""name"": ""TaxResidencies"",
                                                                                    ""namespace"": ""com.capgroup.option.taxresidencies"",
                                                                                    ""fields"": [
                                                                                      {
                                                                                        ""name"": ""TaxResidency"",
                                                                                        ""type"": [
                                                                                          ""null"",
                                                                                          {
                                                                                            ""type"": ""array"",
                                                                                            ""items"": [
                                                                                              ""null"",
                                                                                              {
                                                                                                ""type"": ""record"",
                                                                                                ""name"": ""TaxResidency"",
                                                                                                ""fields"": [
                                                                                                  {
                                                                                                    ""name"": ""TaxResidencyCd"",
                                                                                                    ""type"": [
                                                                                                      ""null"",
                                                                                                      ""string""
                                                                                                    ],
                                                                                                    ""default"": null
                                                                                                  },
                                                                                                  {
                                                                                                    ""name"": ""TaxResidencyEffectiveDate"",
                                                                                                    ""type"": [
                                                                                                      ""null"",
                                                                                                      ""string""
                                                                                                    ],
                                                                                                    ""default"": null
                                                                                                  }
                                                                                                ]
                                                                                              }
                                                                                            ]
                                                                                          }
                                                                                        ]
                                                                                      }
                                                                                    ]
                                                                                  }
                                                                                ],
                                                                                ""default"": null
                                                                              },
                                                                              {
                                                                                ""name"": ""ClassificationGroups"",
                                                                                ""type"": [
                                                                                  ""null"",
                                                                                  {
                                                                                    ""type"": ""record"",
                                                                                    ""name"": ""ClassificationGroups"",
                                                                                    ""fields"": [
                                                                                      {
                                                                                        ""name"": ""ClassificationGroup"",
                                                                                        ""type"": [
                                                                                          {
                                                                                            ""type"": ""array"",
                                                                                            ""items"": {
                                                                                              ""type"": ""record"",
                                                                                              ""name"": ""ClassificationGroup"",
                                                                                              ""fields"": [
                                                                                                {
                                                                                                  ""name"": ""ClassificationGroupId"",
                                                                                                  ""type"": [
                                                                                                    ""null"",
                                                                                                    ""long""
                                                                                                  ],
                                                                                                  ""default"": null
                                                                                                },
                                                                                                {
                                                                                                  ""name"": ""ClassificationGroupName"",
                                                                                                  ""type"": [
                                                                                                    ""null"",
                                                                                                    ""string""
                                                                                                  ],
                                                                                                  ""default"": null
                                                                                                },
                                                                                                {
                                                                                                  ""name"": ""ClassificationValueId"",
                                                                                                  ""type"": [
                                                                                                    ""null"",
                                                                                                    ""int""
                                                                                                  ],
                                                                                                  ""default"": null
                                                                                                },
                                                                                                {
                                                                                                  ""name"": ""ClassificationValue"",
                                                                                                  ""type"": [
                                                                                                    ""null"",
                                                                                                    ""string""
                                                                                                  ],
                                                                                                  ""default"": null
                                                                                                }
                                                                                              ]
                                                                                            }
                                                                                          }
                                                                                        ]
                                                                                      }
                                                                                    ]
                                                                                  }
                                                                                ],
                                                                                ""default"": null
                                                                              },
                                                                              {
                                                                                ""name"": ""EligibleBalance"",
                                                                                ""type"": [
                                                                                  ""null"",
                                                                                  ""double""
                                                                                ],
                                                                                ""default"": null
                                                                              },
                                                                              {
                                                                                ""name"": ""TotalPosition"",
                                                                                ""type"": [
                                                                                  ""null"",
                                                                                  ""double""
                                                                                ],
                                                                                ""default"": null
                                                                              }
                                                                            ]
                                                                          }
                                                                        ],
                                                                        ""default"": null
                                                                      },
                                                                      {
                                                                        ""name"": ""CashAccount"",
                                                                        ""type"": [
                                                                          ""null"",
                                                                          {
                                                                            ""type"": ""array"",
                                                                            ""items"": [
                                                                              ""null"",
                                                                              {
                                                                                ""type"": ""record"",
                                                                                ""name"": ""CashAccount"",
                                                                                ""namespace"": ""com.capgroup.invinst.allocation.cashaccount"",
                                                                                ""fields"": [
                                                                                  {
                                                                                    ""name"": ""AccountExternalReference"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      {
                                                                                        ""type"": ""record"",
                                                                                        ""name"": ""AccountExternalReference"",
                                                                                        ""fields"": [
                                                                                          {
                                                                                            ""name"": ""ExternalRefType"",
                                                                                            ""type"": [
                                                                                              ""null"",
                                                                                              ""string""
                                                                                            ],
                                                                                            ""default"": null
                                                                                          },
                                                                                          {
                                                                                            ""name"": ""ExternalRefValue"",
                                                                                            ""type"": [
                                                                                              ""null"",
                                                                                              ""string""
                                                                                            ],
                                                                                            ""default"": null
                                                                                          }
                                                                                        ]
                                                                                      }
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""CashAccountType"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      ""string""
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""CashAccountCurrencyCd"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      ""string""
                                                                                    ],
                                                                                    ""default"": null
                                                                                  }
                                                                                ]
                                                                              }
                                                                            ]
                                                                          }
                                                                        ],
                                                                        ""default"": null
                                                                      },
                                                                      {
                                                                        ""name"": ""Allocation"",
                                                                        ""type"": [
                                                                          ""null"",
                                                                          {
                                                                            ""type"": ""array"",
                                                                            ""items"": [
                                                                              ""null"",
                                                                              {
                                                                                ""type"": ""record"",
                                                                                ""name"": ""Allocation"",
                                                                                ""fields"": [
                                                                                  {
                                                                                    ""name"": ""AllocationReferences"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      {
                                                                                        ""type"": ""record"",
                                                                                        ""name"": ""AllocationReferences"",
                                                                                        ""fields"": [
                                                                                          {
                                                                                            ""name"": ""AllocationReference"",
                                                                                            ""type"": {
                                                                                              ""type"": ""array"",
                                                                                              ""items"": {
                                                                                                ""type"": ""record"",
                                                                                                ""name"": ""AllocationReference"",
                                                                                                ""fields"": [
                                                                                                  {
                                                                                                    ""name"": ""ReferenceTypeCd"",
                                                                                                    ""type"": [
                                                                                                      ""null"",
                                                                                                      ""string""
                                                                                                    ],
                                                                                                    ""default"": null
                                                                                                  },
                                                                                                  {
                                                                                                    ""name"": ""ReferenceValue"",
                                                                                                    ""type"": [
                                                                                                      ""null"",
                                                                                                      ""string""
                                                                                                    ],
                                                                                                    ""default"": null
                                                                                                  }
                                                                                                ]
                                                                                              }
                                                                                            }
                                                                                          }
                                                                                        ]
                                                                                      }
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""SequenceNumber"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      ""long""
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""AllocationStatusCd"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      ""string""
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""Source"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      ""string""
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""AllocationKeyValues"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      {
                                                                                        ""type"": ""record"",
                                                                                        ""name"": ""AllocationKeyValues"",
                                                                                        ""namespace"": ""com.capgroup.inavacc.allocation.keyvalue"",
                                                                                        ""fields"": [
                                                                                          {
                                                                                            ""name"": ""AllocationKeyValue"",
                                                                                            ""type"": [
                                                                                              ""null"",
                                                                                              {
                                                                                                ""type"": ""array"",
                                                                                                ""items"": [
                                                                                                  ""null"",
                                                                                                  {
                                                                                                    ""type"": ""record"",
                                                                                                    ""name"": ""AllocationKeyValue"",
                                                                                                    ""fields"": [
                                                                                                      {
                                                                                                        ""name"": ""Key"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""Value"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""Value2"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""Value3"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      }
                                                                                                    ]
                                                                                                  }
                                                                                                ]
                                                                                              }
                                                                                            ]
                                                                                          }
                                                                                        ]
                                                                                      }
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""ContractualCashSettlementFlag"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      ""string""
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""ContractualStockSettlementFlag"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      ""string""
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""TransactionTypeCd"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      ""string""
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""ExpectedSettlementDate"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      ""string""
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""OverrideSettlementDate"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      ""string""
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""ElectionId"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      ""long""
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""ElectionDetails"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      {
                                                                                        ""type"": ""record"",
                                                                                        ""name"": ""ElectionDetails"",
                                                                                        ""namespace"": ""com.capgroup.inavacc.allocation.electiondetails"",
                                                                                        ""fields"": [
                                                                                          {
                                                                                            ""name"": ""ElectionDetail"",
                                                                                            ""type"": [
                                                                                              ""null"",
                                                                                              {
                                                                                                ""type"": ""array"",
                                                                                                ""items"": [
                                                                                                  ""null"",
                                                                                                  {
                                                                                                    ""type"": ""record"",
                                                                                                    ""name"": ""ElectionDetail"",
                                                                                                    ""fields"": [
                                                                                                      {
                                                                                                        ""name"": ""ElectionDetailId"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""long""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ElectionQuantity"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      }
                                                                                                    ]
                                                                                                  }
                                                                                                ]
                                                                                              }
                                                                                            ]
                                                                                          }
                                                                                        ]
                                                                                      }
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""BlockingPositionId"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      ""long""
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""FXRequestId"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      ""long""
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""ApportionedPosition"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      ""double""
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""PositionType"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      ""string""
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""ClaimAssociation"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      ""string""
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""TradeReference"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      {
                                                                                        ""type"": ""array"",
                                                                                        ""items"": [
                                                                                          ""null"",
                                                                                          {
                                                                                            ""type"": ""record"",
                                                                                            ""name"": ""TradeReference"",
                                                                                            ""fields"": [
                                                                                              {
                                                                                                ""name"": ""ReferenceTypeCd"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""string""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""ReferenceValue"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""string""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              }
                                                                                            ]
                                                                                          }
                                                                                        ]
                                                                                      }
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""Counterparty"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      {
                                                                                        ""type"": ""array"",
                                                                                        ""items"": [
                                                                                          ""null"",
                                                                                          {
                                                                                            ""type"": ""record"",
                                                                                            ""name"": ""Counterparty"",
                                                                                            ""fields"": [
                                                                                              {
                                                                                                ""name"": ""ExternalRefType"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""string""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""ExternalRefValue"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""string""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              }
                                                                                            ]
                                                                                          }
                                                                                        ]
                                                                                      }
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""IPExternalReferences"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      {
                                                                                        ""type"": ""record"",
                                                                                        ""name"": ""IPExternalReferences"",
                                                                                        ""namespace"": ""com.capgroup.option.ipexternalreferences"",
                                                                                        ""fields"": [
                                                                                          {
                                                                                            ""name"": ""ExternalReference"",
                                                                                            ""type"": [
                                                                                              ""null"",
                                                                                              {
                                                                                                ""type"": ""array"",
                                                                                                ""items"": [
                                                                                                  ""null"",
                                                                                                  {
                                                                                                    ""type"": ""record"",
                                                                                                    ""name"": ""ExternalReference"",
                                                                                                    ""fields"": [
                                                                                                      {
                                                                                                        ""name"": ""ExternalReferenceType"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ExternalReferenceValue"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      }
                                                                                                    ]
                                                                                                  }
                                                                                                ]
                                                                                              }
                                                                                            ]
                                                                                          }
                                                                                        ]
                                                                                      }
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""RoleTypeCd"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      ""string""
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""CashMovement"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      {
                                                                                        ""type"": ""array"",
                                                                                        ""items"": [
                                                                                          ""null"",
                                                                                          {
                                                                                            ""type"": ""record"",
                                                                                            ""name"": ""CashMovement"",
                                                                                            ""fields"": [
                                                                                              {
                                                                                                ""name"": ""CashMovementId"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""long""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""Rate"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""double""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""CreditDebitIndicator"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""string""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""CashAmounts"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  {
                                                                                                    ""type"": ""record"",
                                                                                                    ""name"": ""CashAmounts"",
                                                                                                    ""namespace"": ""com.capgroup.invaccount.cashmove.cashmmounts"",
                                                                                                    ""fields"": [
                                                                                                      {
                                                                                                        ""name"": ""TradeAmount"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          {
                                                                                                            ""type"": ""record"",
                                                                                                            ""name"": ""TradeAmount"",
                                                                                                            ""fields"": [
                                                                                                              {
                                                                                                                ""name"": ""CustodianNetAmount"",
                                                                                                                ""type"": [
                                                                                                                  ""null"",
                                                                                                                  ""double""
                                                                                                                ],
                                                                                                                ""default"": null
                                                                                                              },
                                                                                                              {
                                                                                                                ""name"": ""NetAmount"",
                                                                                                                ""type"": [
                                                                                                                  ""null"",
                                                                                                                  ""double""
                                                                                                                ],
                                                                                                                ""default"": null
                                                                                                              },
                                                                                                              {
                                                                                                                ""name"": ""GrossAmount"",
                                                                                                                ""type"": [
                                                                                                                  ""null"",
                                                                                                                  ""double""
                                                                                                                ],
                                                                                                                ""default"": null
                                                                                                              },
                                                                                                              {
                                                                                                                ""name"": ""Currency"",
                                                                                                                ""type"": [
                                                                                                                  ""null"",
                                                                                                                  ""string""
                                                                                                                ],
                                                                                                                ""default"": null
                                                                                                              }
                                                                                                            ]
                                                                                                          }
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""TradeToSettlementFxRate"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""CustodianPaymentAmount"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          {
                                                                                                            ""type"": ""array"",
                                                                                                            ""items"": [
                                                                                                              ""null"",
                                                                                                              {
                                                                                                                ""type"": ""record"",
                                                                                                                ""name"": ""CustodianPaymentAmount"",
                                                                                                                ""namespace"": ""com.capgroup.invaccount.custpayamount"",
                                                                                                                ""fields"": [
                                                                                                                  {
                                                                                                                    ""name"": ""CustodianPaymentCurrency"",
                                                                                                                    ""type"": [
                                                                                                                      ""null"",
                                                                                                                      ""string""
                                                                                                                    ],
                                                                                                                    ""default"": null
                                                                                                                  },
                                                                                                                  {
                                                                                                                    ""name"": ""SettlementAmountInCustodianCurrency"",
                                                                                                                    ""type"": [
                                                                                                                      ""null"",
                                                                                                                      ""double""
                                                                                                                    ],
                                                                                                                    ""default"": null
                                                                                                                  },
                                                                                                                  {
                                                                                                                    ""name"": ""CustodianPaymentToSettlementFxRate"",
                                                                                                                    ""type"": [
                                                                                                                      ""null"",
                                                                                                                      ""double""
                                                                                                                    ],
                                                                                                                    ""default"": null
                                                                                                                  },
                                                                                                                  {
                                                                                                                    ""name"": ""CustodianPaymentToTradeFxRate"",
                                                                                                                    ""type"": [
                                                                                                                      ""null"",
                                                                                                                      ""double""
                                                                                                                    ],
                                                                                                                    ""default"": null
                                                                                                                  }
                                                                                                                ]
                                                                                                              }
                                                                                                            ]
                                                                                                          }
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""SettlementAmount"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          {
                                                                                                            ""type"": ""record"",
                                                                                                            ""name"": ""SettlementAmount"",
                                                                                                            ""fields"": [
                                                                                                              {
                                                                                                                ""name"": ""NetAmount"",
                                                                                                                ""type"": [
                                                                                                                  ""null"",
                                                                                                                  ""double""
                                                                                                                ],
                                                                                                                ""default"": null
                                                                                                              },
                                                                                                              {
                                                                                                                ""name"": ""AggregatedSettledAmount"",
                                                                                                                ""type"": [
                                                                                                                  ""null"",
                                                                                                                  ""double""
                                                                                                                ],
                                                                                                                ""default"": null
                                                                                                              },
                                                                                                              {
                                                                                                                ""name"": ""RemainingSettlementAmount"",
                                                                                                                ""type"": [
                                                                                                                  ""null"",
                                                                                                                  ""double""
                                                                                                                ],
                                                                                                                ""default"": null
                                                                                                              },
                                                                                                              {
                                                                                                                ""name"": ""Currency"",
                                                                                                                ""type"": [
                                                                                                                  ""null"",
                                                                                                                  ""string""
                                                                                                                ],
                                                                                                                ""default"": null
                                                                                                              }
                                                                                                            ]
                                                                                                          }
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      }
                                                                                                    ]
                                                                                                  }
                                                                                                ],
                                                                                                ""default"": null
                                                                                              }
                                                                                            ]
                                                                                          }
                                                                                        ]
                                                                                      }
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""SecurityMovement"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      {
                                                                                        ""type"": ""array"",
                                                                                        ""items"": [
                                                                                          ""null"",
                                                                                          {
                                                                                            ""type"": ""record"",
                                                                                            ""name"": ""SecurityMovement"",
                                                                                            ""fields"": [
                                                                                              {
                                                                                                ""name"": ""SecurityMovementId"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""long""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""StockDirectionInd"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""string""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""SecurityQuantity"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""double""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""AggregatedSettledSecurityQuantity"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""double""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""RemainingSettlementSecurityQuantity"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""double""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""CostPriceSecurityCurrency"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""double""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""PriceInTradeCurrency"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""double""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""SecurityDetails"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  {
                                                                                                    ""type"": ""record"",
                                                                                                    ""name"": ""SecurityDetails"",
                                                                                                    ""fields"": [
                                                                                                      {
                                                                                                        ""name"": ""IssueCountryCd"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""IssueCurrencyCd"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""SecurityExternalReferences"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          {
                                                                                                            ""type"": ""record"",
                                                                                                            ""name"": ""SecurityExternalReferences"",
                                                                                                            ""fields"": [
                                                                                                              {
                                                                                                                ""name"": ""SecurityExternalReference"",
                                                                                                                ""type"": [
                                                                                                                  {
                                                                                                                    ""type"": ""array"",
                                                                                                                    ""items"": {
                                                                                                                      ""type"": ""record"",
                                                                                                                      ""name"": ""SecurityExternalReference"",
                                                                                                                      ""fields"": [
                                                                                                                        {
                                                                                                                          ""name"": ""SecurityIdentifierTypeCd"",
                                                                                                                          ""type"": [
                                                                                                                            ""null"",
                                                                                                                            ""string""
                                                                                                                          ],
                                                                                                                          ""default"": null
                                                                                                                        },
                                                                                                                        {
                                                                                                                          ""name"": ""SecurityIdentifierValue"",
                                                                                                                          ""type"": [
                                                                                                                            ""null"",
                                                                                                                            ""string""
                                                                                                                          ],
                                                                                                                          ""default"": null
                                                                                                                        }
                                                                                                                      ]
                                                                                                                    }
                                                                                                                  }
                                                                                                                ]
                                                                                                              }
                                                                                                            ]
                                                                                                          }
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ReportingClassificationGroups"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          {
                                                                                                            ""type"": ""record"",
                                                                                                            ""name"": ""ReportingClassificationGroups"",
                                                                                                            ""fields"": [
                                                                                                              {
                                                                                                                ""name"": ""ReportingClassificationGroup"",
                                                                                                                ""type"": [
                                                                                                                  {
                                                                                                                    ""type"": ""array"",
                                                                                                                    ""items"": {
                                                                                                                      ""type"": ""record"",
                                                                                                                      ""name"": ""ReportingClassificationGroup"",
                                                                                                                      ""fields"": [
                                                                                                                        {
                                                                                                                          ""name"": ""ReportingClassificationGroupId"",
                                                                                                                          ""type"": [
                                                                                                                            ""null"",
                                                                                                                            ""long""
                                                                                                                          ],
                                                                                                                          ""default"": null
                                                                                                                        },
                                                                                                                        {
                                                                                                                          ""name"": ""ReportingClassificationGroupName"",
                                                                                                                          ""type"": [
                                                                                                                            ""null"",
                                                                                                                            ""string""
                                                                                                                          ],
                                                                                                                          ""default"": null
                                                                                                                        },
                                                                                                                        {
                                                                                                                          ""name"": ""ValueId"",
                                                                                                                          ""type"": [
                                                                                                                            ""null"",
                                                                                                                            ""long""
                                                                                                                          ],
                                                                                                                          ""default"": null
                                                                                                                        },
                                                                                                                        {
                                                                                                                          ""name"": ""ValueName"",
                                                                                                                          ""type"": [
                                                                                                                            ""null"",
                                                                                                                            ""string""
                                                                                                                          ],
                                                                                                                          ""default"": null
                                                                                                                        },
                                                                                                                        {
                                                                                                                          ""name"": ""ValueCode"",
                                                                                                                          ""type"": [
                                                                                                                            ""null"",
                                                                                                                            ""string""
                                                                                                                          ],
                                                                                                                          ""default"": null
                                                                                                                        },
                                                                                                                        {
                                                                                                                          ""name"": ""ValueSequence"",
                                                                                                                          ""type"": [
                                                                                                                            ""null"",
                                                                                                                            ""string""
                                                                                                                          ],
                                                                                                                          ""default"": null
                                                                                                                        }
                                                                                                                      ]
                                                                                                                    }
                                                                                                                  }
                                                                                                                ]
                                                                                                              }
                                                                                                            ]
                                                                                                          }
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      }
                                                                                                    ]
                                                                                                  }
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""CashAmounts"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  {
                                                                                                    ""type"": ""record"",
                                                                                                    ""name"": ""CashAmounts"",
                                                                                                    ""namespace"": ""com.capgroup.custaccount.cashmmounts"",
                                                                                                    ""fields"": [
                                                                                                      {
                                                                                                        ""name"": ""TradeAmount"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          {
                                                                                                            ""type"": ""record"",
                                                                                                            ""name"": ""TradeAmount"",
                                                                                                            ""fields"": [
                                                                                                              {
                                                                                                                ""name"": ""CustodianNetAmount"",
                                                                                                                ""type"": [
                                                                                                                  ""null"",
                                                                                                                  ""double""
                                                                                                                ],
                                                                                                                ""default"": null
                                                                                                              },
                                                                                                              {
                                                                                                                ""name"": ""NetAmount"",
                                                                                                                ""type"": [
                                                                                                                  ""null"",
                                                                                                                  ""double""
                                                                                                                ],
                                                                                                                ""default"": null
                                                                                                              },
                                                                                                              {
                                                                                                                ""name"": ""GrossAmount"",
                                                                                                                ""type"": [
                                                                                                                  ""null"",
                                                                                                                  ""double""
                                                                                                                ],
                                                                                                                ""default"": null
                                                                                                              },
                                                                                                              {
                                                                                                                ""name"": ""Currency"",
                                                                                                                ""type"": [
                                                                                                                  ""null"",
                                                                                                                  ""string""
                                                                                                                ],
                                                                                                                ""default"": null
                                                                                                              }
                                                                                                            ]
                                                                                                          }
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""TradeToSettlementFxRate"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""CustodianPaymentAmount"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          {
                                                                                                            ""type"": ""array"",
                                                                                                            ""items"": [
                                                                                                              ""null"",
                                                                                                              {
                                                                                                                ""type"": ""record"",
                                                                                                                ""name"": ""CustodianPaymentAmount"",
                                                                                                                ""fields"": [
                                                                                                                  {
                                                                                                                    ""name"": ""CustodianPaymentCurrency"",
                                                                                                                    ""type"": [
                                                                                                                      ""null"",
                                                                                                                      ""string""
                                                                                                                    ],
                                                                                                                    ""default"": null
                                                                                                                  },
                                                                                                                  {
                                                                                                                    ""name"": ""SettlementAmountInCustodianCurrency"",
                                                                                                                    ""type"": [
                                                                                                                      ""null"",
                                                                                                                      ""string""
                                                                                                                    ],
                                                                                                                    ""default"": null
                                                                                                                  },
                                                                                                                  {
                                                                                                                    ""name"": ""CustodianPaymentToSettlementFxRate"",
                                                                                                                    ""type"": [
                                                                                                                      ""null"",
                                                                                                                      ""string""
                                                                                                                    ],
                                                                                                                    ""default"": null
                                                                                                                  },
                                                                                                                  {
                                                                                                                    ""name"": ""CustodianPaymentToTradeFxRate"",
                                                                                                                    ""type"": [
                                                                                                                      ""null"",
                                                                                                                      ""string""
                                                                                                                    ],
                                                                                                                    ""default"": null
                                                                                                                  }
                                                                                                                ]
                                                                                                              }
                                                                                                            ]
                                                                                                          }
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""SettlementAmount"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          {
                                                                                                            ""type"": ""record"",
                                                                                                            ""name"": ""SettlementAmount"",
                                                                                                            ""fields"": [
                                                                                                              {
                                                                                                                ""name"": ""NetAmount"",
                                                                                                                ""type"": [
                                                                                                                  ""null"",
                                                                                                                  ""double""
                                                                                                                ],
                                                                                                                ""default"": null
                                                                                                              },
                                                                                                              {
                                                                                                                ""name"": ""AggregatedSettledAmount"",
                                                                                                                ""type"": [
                                                                                                                  ""null"",
                                                                                                                  ""double""
                                                                                                                ],
                                                                                                                ""default"": null
                                                                                                              },
                                                                                                              {
                                                                                                                ""name"": ""RemainingSettlementAmount"",
                                                                                                                ""type"": [
                                                                                                                  ""null"",
                                                                                                                  ""double""
                                                                                                                ],
                                                                                                                ""default"": null
                                                                                                              },
                                                                                                              {
                                                                                                                ""name"": ""Currency"",
                                                                                                                ""type"": [
                                                                                                                  ""null"",
                                                                                                                  ""string""
                                                                                                                ],
                                                                                                                ""default"": null
                                                                                                              },
                                                                                                              {
                                                                                                                ""name"": ""SettlementAmountDifference"",
                                                                                                                ""type"": [
                                                                                                                  ""null"",
                                                                                                                  ""double""
                                                                                                                ],
                                                                                                                ""default"": null
                                                                                                              }
                                                                                                            ]
                                                                                                          }
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      }
                                                                                                    ]
                                                                                                  }
                                                                                                ],
                                                                                                ""default"": null
                                                                                              }
                                                                                            ]
                                                                                          }
                                                                                        ]
                                                                                      }
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""BookCost"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      {
                                                                                        ""type"": ""array"",
                                                                                        ""items"": [
                                                                                          ""null"",
                                                                                          {
                                                                                            ""type"": ""record"",
                                                                                            ""name"": ""BookCost"",
                                                                                            ""fields"": [
                                                                                              {
                                                                                                ""name"": ""BookCostIsEstimateFlag"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""string""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""CleanBookCostInBaseCurr"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""double""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""CleanBookCostInSecCurr"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""double""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""DirtyBookCostInBaseCurr"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""double""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""DirtyBookCostInSecCurr"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""double""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""TradeToSecFxRate"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""double""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""TradeToBaseFxRate"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""double""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""ClientBaseCurrency"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""string""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""BookCostCurr"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""string""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""DirtyBookCostInBookCostCurr"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""double""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""CleanBookCostInBookCostCurr"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""double""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              }
                                                                                            ]
                                                                                          }
                                                                                        ]
                                                                                      }
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""Charges"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      {
                                                                                        ""type"": ""record"",
                                                                                        ""name"": ""Charges"",
                                                                                        ""namespace"": ""com.capgroup.invacc.Charges"",
                                                                                        ""fields"": [
                                                                                          {
                                                                                            ""name"": ""Charge"",
                                                                                            ""type"": [
                                                                                              ""null"",
                                                                                              {
                                                                                                ""type"": ""array"",
                                                                                                ""items"": [
                                                                                                  ""null"",
                                                                                                  {
                                                                                                    ""type"": ""record"",
                                                                                                    ""name"": ""Charge"",
                                                                                                    ""fields"": [
                                                                                                      {
                                                                                                        ""name"": ""ChargeId"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""long""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ChargeClassificationType"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ChargeCategoryCd"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ChargeCd"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ChargeableQuantity"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ChargeCurrencyCd"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ChargeableAmount"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""RateCd"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ChargeRateTypeCd"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ChargeRate"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""VatRate"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""VatTypeCd"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""VatAmount"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""GrossChargeAmount"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ChargeAmount"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ChargeSourceCd"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ChargeOriginCd"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ApplyChargeAsCd"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""EstimateFlag"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ReportableOnly"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ReclaimFlag"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ReclaimChargeRate"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ChargeReference"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""QualifierCd"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""NationalCurrencyCd"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""NationalChargeableAmount"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""NationalChargeAmount"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""TradeToNationalFxRate"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ReclaimableChargeRate"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""NonReclaimableChargeRate"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""AppliedNetRate"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ReclaimableChargeAmount"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""NonReclaimableChargeAmount"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      }
                                                                                                    ]
                                                                                                  }
                                                                                                ]
                                                                                              }
                                                                                            ]
                                                                                          }
                                                                                        ]
                                                                                      }
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""SettlementDetails"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      {
                                                                                        ""type"": ""record"",
                                                                                        ""name"": ""SettlementDetails"",
                                                                                        ""fields"": [
                                                                                          {
                                                                                            ""name"": ""SettlementDetail"",
                                                                                            ""type"": [
                                                                                              ""null"",
                                                                                              {
                                                                                                ""type"": ""array"",
                                                                                                ""items"": [
                                                                                                  ""null"",
                                                                                                  {
                                                                                                    ""type"": ""record"",
                                                                                                    ""name"": ""SettlementDetail"",
                                                                                                    ""fields"": [
                                                                                                      {
                                                                                                        ""name"": ""CreationDate"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""StatusId"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""long""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""AllocationStatusCd"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""StatusActionCd"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""CashValueDate"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""CashPostingDate"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""SettlementAmount"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""AggregatedSettledAmount"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""RemainingSettlementAmount"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""StockValueDate"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""StockPostingDate"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""SettlementSecurityQuantity"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""AggregatedSettledSecurityQuantity"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""RemainingSettlementSecurityQuantity"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""Charges"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          {
                                                                                                            ""type"": ""record"",
                                                                                                            ""name"": ""Charges"",
                                                                                                            ""namespace"": ""com.capgroup.option.settlements.charges"",
                                                                                                            ""fields"": [
                                                                                                              {
                                                                                                                ""name"": ""Charge"",
                                                                                                                ""type"": [
                                                                                                                  {
                                                                                                                    ""type"": ""array"",
                                                                                                                    ""items"": {
                                                                                                                      ""type"": ""record"",
                                                                                                                      ""name"": ""Charge"",
                                                                                                                      ""fields"": [
                                                                                                                        {
                                                                                                                          ""name"": ""ChargeId"",
                                                                                                                          ""type"": [
                                                                                                                            ""null"",
                                                                                                                            ""long""
                                                                                                                          ],
                                                                                                                          ""default"": null
                                                                                                                        },
                                                                                                                        {
                                                                                                                          ""name"": ""SettlementChargeAmount"",
                                                                                                                          ""type"": [
                                                                                                                            ""null"",
                                                                                                                            ""double""
                                                                                                                          ],
                                                                                                                          ""default"": null
                                                                                                                        }
                                                                                                                      ]
                                                                                                                    }
                                                                                                                  }
                                                                                                                ]
                                                                                                              }
                                                                                                            ]
                                                                                                          }
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ReasonCd"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""Comments"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      }
                                                                                                    ]
                                                                                                  }
                                                                                                ]
                                                                                              }
                                                                                            ]
                                                                                          }
                                                                                        ]
                                                                                      }
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""CashAccount"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      {
                                                                                        ""type"": ""array"",
                                                                                        ""items"": [
                                                                                          ""null"",
                                                                                          {
                                                                                            ""type"": ""record"",
                                                                                            ""name"": ""CashAccount"",
                                                                                            ""namespace"": ""com.capgroup.invinst.cashaccount"",
                                                                                            ""fields"": [
                                                                                              {
                                                                                                ""name"": ""AccountExternalReference"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  {
                                                                                                    ""type"": ""record"",
                                                                                                    ""name"": ""AccountExternalReference"",
                                                                                                    ""fields"": [
                                                                                                      {
                                                                                                        ""name"": ""ExternalRefType"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""ExternalRefValue"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      }
                                                                                                    ]
                                                                                                  }
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""CashAccountType"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""string""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""CashAccountCurrencyCd"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""string""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              }
                                                                                            ]
                                                                                          }
                                                                                        ]
                                                                                      }
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""BeneficialOwnerDetail"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      {
                                                                                        ""type"": ""array"",
                                                                                        ""items"": [
                                                                                          ""null"",
                                                                                          {
                                                                                            ""type"": ""record"",
                                                                                            ""name"": ""BeneficialOwnerDetail"",
                                                                                            ""fields"": [
                                                                                              {
                                                                                                ""name"": ""BeneficialOwnerId"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""long""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""QuantityOfSecuritiesHeld"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""double""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""TaxResidency"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""double""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""TaxRate"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""double""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""ClientReference"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""string""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              },
                                                                                              {
                                                                                                ""name"": ""ClaimTypeCd"",
                                                                                                ""type"": [
                                                                                                  ""null"",
                                                                                                  ""string""
                                                                                                ],
                                                                                                ""default"": null
                                                                                              }
                                                                                            ]
                                                                                          }
                                                                                        ]
                                                                                      }
                                                                                    ],
                                                                                    ""default"": null
                                                                                  },
                                                                                  {
                                                                                    ""name"": ""SubPosAdjustments"",
                                                                                    ""type"": [
                                                                                      ""null"",
                                                                                      {
                                                                                        ""type"": ""record"",
                                                                                        ""name"": ""SubPosAdjustments"",
                                                                                        ""namespace"": ""com.capgroup.option.subposadjustments"",
                                                                                        ""fields"": [
                                                                                          {
                                                                                            ""name"": ""SubPosAdjustment"",
                                                                                            ""type"": [
                                                                                              ""null"",
                                                                                              {
                                                                                                ""type"": ""array"",
                                                                                                ""items"": [
                                                                                                  ""null"",
                                                                                                  {
                                                                                                    ""type"": ""record"",
                                                                                                    ""name"": ""SubPosAdjustment"",
                                                                                                    ""fields"": [
                                                                                                      {
                                                                                                        ""name"": ""AdjustmentReferences"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          {
                                                                                                            ""type"": ""record"",
                                                                                                            ""name"": ""AdjustmentReferences"",
                                                                                                            ""fields"": [
                                                                                                              {
                                                                                                                ""name"": ""AdjustmentReference"",
                                                                                                                ""type"": [
                                                                                                                  ""null"",
                                                                                                                  {
                                                                                                                    ""type"": ""array"",
                                                                                                                    ""items"": [
                                                                                                                      ""null"",
                                                                                                                      {
                                                                                                                        ""type"": ""record"",
                                                                                                                        ""name"": ""AdjustmentReference"",
                                                                                                                        ""fields"": [
                                                                                                                          {
                                                                                                                            ""name"": ""ExternalReferenceType"",
                                                                                                                            ""type"": [
                                                                                                                              ""null"",
                                                                                                                              ""string""
                                                                                                                            ],
                                                                                                                            ""default"": null
                                                                                                                          },
                                                                                                                          {
                                                                                                                            ""name"": ""ExternalReferenceValue"",
                                                                                                                            ""type"": [
                                                                                                                              ""null"",
                                                                                                                              ""string""
                                                                                                                            ],
                                                                                                                            ""default"": null
                                                                                                                          }
                                                                                                                        ]
                                                                                                                      }
                                                                                                                    ]
                                                                                                                  }
                                                                                                                ]
                                                                                                              }
                                                                                                            ]
                                                                                                          }
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""InterestedPartyDetails"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          {
                                                                                                            ""type"": ""record"",
                                                                                                            ""name"": ""InterestedPartyDetails"",
                                                                                                            ""fields"": [
                                                                                                              {
                                                                                                                ""name"": ""ExternalReferences"",
                                                                                                                ""type"": [
                                                                                                                  ""null"",
                                                                                                                  {
                                                                                                                    ""type"": ""record"",
                                                                                                                    ""name"": ""ExternalReferences"",
                                                                                                                    ""fields"": [
                                                                                                                      {
                                                                                                                        ""name"": ""ExternalReference"",
                                                                                                                        ""type"": {
                                                                                                                          ""type"": ""array"",
                                                                                                                          ""items"": {
                                                                                                                            ""type"": ""record"",
                                                                                                                            ""name"": ""ExternalReference"",
                                                                                                                            ""fields"": [
                                                                                                                              {
                                                                                                                                ""name"": ""ExternalReferenceType"",
                                                                                                                                ""type"": [
                                                                                                                                  ""null"",
                                                                                                                                  ""long""
                                                                                                                                ],
                                                                                                                                ""default"": null
                                                                                                                              },
                                                                                                                              {
                                                                                                                                ""name"": ""ExternalReferenceValue"",
                                                                                                                                ""type"": [
                                                                                                                                  ""null"",
                                                                                                                                  ""double""
                                                                                                                                ],
                                                                                                                                ""default"": null
                                                                                                                              }
                                                                                                                            ]
                                                                                                                          }
                                                                                                                        }
                                                                                                                      }
                                                                                                                    ]
                                                                                                                  }
                                                                                                                ],
                                                                                                                ""default"": null
                                                                                                              }
                                                                                                            ]
                                                                                                          }
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""RoleTypeCd"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""SubPositionTypeCd"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""Quantity"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""double""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""CreditDebitIndicator"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      },
                                                                                                      {
                                                                                                        ""name"": ""EndDate"",
                                                                                                        ""type"": [
                                                                                                          ""null"",
                                                                                                          ""string""
                                                                                                        ],
                                                                                                        ""default"": null
                                                                                                      }
                                                                                                    ]
                                                                                                  }
                                                                                                ]
                                                                                              }
                                                                                            ]
                                                                                          }
                                                                                        ]
                                                                                      }
                                                                                    ],
                                                                                    ""default"": null
                                                                                  }
                                                                                ]
                                                                              }
                                                                            ]
                                                                          }
                                                                        ]
                                                                      }
                                                                    ]
                                                                  }
                                                                ]
                                                              }
                                                            ]
                                                          }
                                                        ]
                                                      }
                                                    ],
                                                    ""default"": null
                                                  }
                                                ]
                                              }
                                            }
                                          ]
                                        }
                                      ]
                                    }
                                  ]
                                }
                              ],
                              ""default"": null
                            }
                          ]
                        }
                      ],
                      ""default"": null
                    },
                    {
                      ""name"": ""OwnerDetails"",
                      ""type"": [
                        ""null"",
                        {
                          ""type"": ""record"",
                          ""name"": ""OwnerDetails"",
                          ""fields"": [
                            {
                              ""name"": ""Team"",
                              ""type"": [
                                ""null"",
                                {
                                  ""type"": ""record"",
                                  ""name"": ""Team"",
                                  ""fields"": [
                                    {
                                      ""name"": ""TeamName"",
                                      ""type"": [
                                        ""null"",
                                        ""string""
                                      ],
                                      ""default"": null
                                    }
                                  ]
                                }
                              ],
                              ""default"": null
                            },
                            {
                              ""name"": ""Executive"",
                              ""type"": [
                                ""null"",
                                {
                                  ""type"": ""record"",
                                  ""name"": ""Executive"",
                                  ""fields"": [
                                    {
                                      ""name"": ""LoginName"",
                                      ""type"": [
                                        ""null"",
                                        ""string""
                                      ],
                                      ""default"": null
                                    }
                                  ]
                                }
                              ],
                              ""default"": null
                            }
                          ]
                        }
                      ],
                      ""default"": null
                    }
                  ]
                }
              ],
              ""default"": null
            }
          ]
        }
      ],
      ""default"": null
    }
  ]
}
";

            var schema = Schema.Parse(json);
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
