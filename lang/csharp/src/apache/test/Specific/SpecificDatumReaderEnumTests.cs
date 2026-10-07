/*
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
using System.IO;
using Avro.Generic;
using Avro.IO;
using Avro.Specific;
using Avro.Test.AvroGen;
using NUnit.Framework;

namespace Avro.Test.Specific
{
    [TestFixture]
    internal class SpecificDatumReaderEnumTests
    {
        private RecordSchema recordSchema;
        private Type recordType;
        private Type enum1Type;
        private Type enum2Type;

        [OneTimeSetUp]
        public void GenerateSpecificTypes()
        {
            // Generate and load the types once so ObjectCreator resolves the same assembly in every test.
            var assembly = AvroGenHelper.TestSchema(RecordSchemaJson);
            recordSchema = (RecordSchema)Schema.Parse(RecordSchemaJson);
            recordType = assembly.GetType(recordSchema.Fullname);
            enum1Type = assembly.GetType("Avro.Test.Specific.EnumReader.Enum1");
            enum2Type = assembly.GetType("Avro.Test.Specific.EnumReader.Enum2");

            Assert.That(recordType, Is.Not.Null, "Generated record type was not found.");
            Assert.That(enum1Type, Is.Not.Null, "Generated Enum1 type was not found.");
            Assert.That(enum2Type, Is.Not.Null, "Generated Enum2 type was not found.");
        }

        [TestCase("FIRST")]
        [TestCase("SECOND")]
        [TestCase("DEFAULT")]
        public void TestEnumProducesClrEnum(string symbol)
        {
            var schema = (EnumSchema)recordSchema.Fields[0].Schema;
            var expected = Enum.Parse(enum1Type, symbol);
            using (var stream = new MemoryStream())
            {
                new SpecificDatumWriter<object>(schema).Write(expected, new BinaryEncoder(stream));
                stream.Position = 0;
                var actual = new SpecificDatumReader<object>(schema, schema).Read(null, new BinaryDecoder(stream));

                Assert.That(actual.GetType(), Is.EqualTo(enum1Type));
                Assert.That(actual, Is.EqualTo(expected));
            }
        }

        [TestCase("enumField", "Enum1", 1)]
        [TestCase("nullableEnum", null, 0)]
        [TestCase("nullableEnum", "Enum1", 1)]
        [TestCase("intEnum", "int", 1)]
        [TestCase("intEnum", "Enum1", 1)]
        [TestCase("enumUnion", "Enum1", 1)]
        [TestCase("enumUnion", "Enum2", 1)]
        [TestCase("intEnumUnion", "int", 1)]
        [TestCase("intEnumUnion", "Enum1", 1)]
        [TestCase("intEnumUnion", "Enum2", 1)]
        [TestCase("nullableIntEnum", null, 0)]
        [TestCase("nullableIntEnum", "int", 1)]
        [TestCase("nullableIntEnum", "Enum1", 1)]
        public void TestGeneratedEnumFieldsRoundTrip(string fieldName, string typeName, int ordinal)
        {
            object expected = null;
            if (typeName == "int")
            {
                expected = ordinal;
            }
            else if (typeName != null)
            {
                expected = Enum.ToObject(typeName == "Enum1" ? enum1Type : enum2Type, ordinal);
            }

            var record = (ISpecificRecord)Activator.CreateInstance(recordType)!;
            record.Put(recordSchema["intEnum"].Pos, 0);
            record.Put(recordSchema["enumUnion"].Pos, Enum.ToObject(enum1Type, 0));
            record.Put(recordSchema["intEnumUnion"].Pos, 0);
            var field = recordSchema[fieldName];
            record.Put(field.Pos, expected);

            var writer = new SpecificDatumWriter<ISpecificRecord>(recordSchema);
            var reader = new SpecificDatumReader<ISpecificRecord>(recordSchema, recordSchema);
            using (var stream = new MemoryStream())
            {
                writer.Write(record, new BinaryEncoder(stream));
                stream.Position = 0;
                var decodedRecord = reader.Read(null, new BinaryDecoder(stream));
                var actual = decodedRecord.Get(field.Pos);

                Assert.That(actual?.GetType(), Is.EqualTo(expected?.GetType()));
                Assert.That(actual, Is.EqualTo(expected));

                // Reserializing must retain the union branch as well as the value.
                var originalBytes = stream.ToArray();
                stream.SetLength(0);
                stream.Position = 0;
                writer.Write(decodedRecord, new BinaryEncoder(stream));
                Assert.That(stream.ToArray(), Is.EqualTo(originalBytes));
            }
        }

        [TestCase("FIRST")]
        [TestCase("SECOND")]
        [TestCase("DEFAULT")]
        public void TestEnumSymbolReordering(string symbol)
        {
            var readerSchema = (EnumSchema)recordSchema.Fields[0].Schema;
            var writerSchema = (EnumSchema)Schema.Parse(
                """
                {
                    "type": "enum",
                    "name": "Avro.Test.Specific.EnumReader.Enum1",
                    "symbols": ["DEFAULT", "FIRST", "SECOND"]
                }
                """);
            Assert.That(writerSchema.Ordinal(symbol), Is.Not.EqualTo(readerSchema.Ordinal(symbol)));
            AssertResolvedEnum(writerSchema, readerSchema, symbol, symbol);
        }

        [Test]
        public void TestReaderEnumDefault()
        {
            var readerSchema = (EnumSchema)recordSchema.Fields[0].Schema;
            var writerSchema = (EnumSchema)Schema.Parse(
                """
                {
                    "type": "enum",
                    "name": "Avro.Test.Specific.EnumReader.Enum1",
                    "symbols": ["REMOVED", "FIRST", "SECOND", "DEFAULT"]
                }
                """);
            AssertResolvedEnum(writerSchema, readerSchema, "REMOVED", "SECOND");
        }

        [Test]
        public void TestReaderFieldDefaults()
        {
            var writerSchema = Schema.Parse(
                """
                {
                    "type": "record",
                    "name": "Avro.Test.Specific.EnumReader.SpecificEnumReaderRecord",
                    "fields": []
                }
                """);
            using (var stream = new MemoryStream())
            {
                var record = new SpecificDatumReader<ISpecificRecord>(writerSchema, recordSchema)
                    .Read(null, new BinaryDecoder(stream));
                Assert.That(record.Get(recordSchema["enumField"].Pos), Is.EqualTo(Enum.Parse(enum1Type, "SECOND")));
                var unionValue = record.Get(recordSchema["enumUnion"].Pos);
                Assert.That(unionValue.GetType(), Is.EqualTo(enum1Type));
                Assert.That(unionValue, Is.EqualTo(Enum.Parse(enum1Type, "FIRST")));
            }
        }

        private void AssertResolvedEnum(EnumSchema writerSchema, EnumSchema readerSchema, string writerSymbol, string readerSymbol)
        {
            using (var stream = new MemoryStream())
            {
                new GenericDatumWriter<GenericEnum>(writerSchema)
                    .Write(new GenericEnum(writerSchema, writerSymbol), new BinaryEncoder(stream));
                stream.Position = 0;
                var actual = new SpecificDatumReader<object>(writerSchema, readerSchema).Read(null, new BinaryDecoder(stream));
                Assert.That(actual.GetType(), Is.EqualTo(enum1Type));
                Assert.That(actual, Is.EqualTo(Enum.Parse(enum1Type, readerSymbol)));

                stream.Position = 0;
                var generic = new GenericDatumReader<GenericEnum>(writerSchema, readerSchema).Read(null, new BinaryDecoder(stream));
                Assert.That(generic, Is.TypeOf<GenericEnum>());
                Assert.That(generic.Value, Is.EqualTo(readerSymbol));
            }
        }

        private const string RecordSchemaJson = """
            {
                "type": "record",
                "name": "SpecificEnumReaderRecord",
                "namespace": "Avro.Test.Specific.EnumReader",
                "fields": [
                    {
                        "name": "enumField",
                        "type": {
                            "type": "enum",
                            "name": "Enum1",
                            "symbols": [
                                "FIRST",
                                "SECOND",
                                "DEFAULT"
                            ],
                            "default": "SECOND"
                        },
                        "default": "SECOND"
                    },
                    {
                        "name": "nullableEnum",
                        "type": [
                            "null",
                            "Enum1"
                        ],
                        "default": null
                    },
                    {
                        "name": "intEnum",
                        "type": [
                            "int",
                            "Enum1"
                        ],
                        "default": 0
                    },
                    {
                        "name": "enumUnion",
                        "type": [
                            "Enum1",
                            {
                                "type": "enum",
                                "name": "Enum2",
                                "symbols": [
                                    "THIRD",
                                    "FOURTH"
                                ]
                            }
                        ],
                        "default": "FIRST"
                    },
                    {
                        "name": "intEnumUnion",
                        "type": [
                            "int",
                            "Enum1",
                            "Enum2"
                        ],
                        "default": 0
                    },
                    {
                        "name": "nullableIntEnum",
                        "type": [
                            "null",
                            "int",
                            "Enum1"
                        ],
                        "default": null
                    }
                ]
            }
            """;
    }
}
