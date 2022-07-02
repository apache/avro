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
using System.Collections.Concurrent;
using System.IO;
using Avro.IO;
using Avro.Reflect;
using NUnit.Framework;
using System.Collections;

namespace Avro.Test
{
    [TestFixture]
    public class TestUnion
    {
        public const string BaseClassSchema = @"
        [
            { ""type"" : ""record"", ""name"" : ""Dervied1"", ""fields"" :
                [
                    { ""name"" : ""A"", ""type"" : ""string""},
                    { ""name"" : ""B"", ""type"" : ""int""}
                ]
            },
            { ""type"" : ""record"", ""name"" : ""Dervied2"", ""fields"" :
                [
                    { ""name"" : ""A"", ""type"" : ""string""},
                    { ""name"" : ""C"", ""type"" : ""double""}
                ]
            },

        ]
        ";

        public class BaseClass : IEquatable<BaseClass>
        {
            public string A { get; set; }

            public bool Equals(BaseClass other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return A == other.A;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != this.GetType()) return false;
                return Equals((BaseClass) obj);
            }

            public override int GetHashCode()
            {
                return A != null ? A.GetHashCode() : 0;
            }
        }

        public class Derived1 : BaseClass, IEquatable<Derived1>
        {
            public int B { get; set; }

            public bool Equals(Derived1 other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return base.Equals(other) && B == other.B;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != this.GetType()) return false;
                return Equals((Derived1) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (base.GetHashCode() * 397) ^ B;
                }
            }

            public static bool operator ==(Derived1 left, Derived1 right)
            {
                return Equals(left, right);
            }

            public static bool operator !=(Derived1 left, Derived1 right)
            {
                return !Equals(left, right);
            }
        }

        public class Derived2 : BaseClass, IEquatable<Derived2>
        {
            public double C { get; set; }

            public bool Equals(Derived2 other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return base.Equals(other) && C.Equals(other.C);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != this.GetType()) return false;
                return Equals((Derived2) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (base.GetHashCode() * 397) ^ C.GetHashCode();
                }
            }

            public static bool operator ==(Derived2 left, Derived2 right)
            {
                return Equals(left, right);
            }

            public static bool operator !=(Derived2 left, Derived2 right)
            {
                return !Equals(left, right);
            }
        }

        /// <summary>
        /// Test with a union that represents derived classes.
        /// </summary>
        [TestCase]
        public void BaseClassTest()
        {
            var schema = Schema.Parse(BaseClassSchema);
            var derived1write = new Derived1() { A = "derived1", B = 7 };
            var derived2write = new Derived2() { A = "derived2", C = 3.14 };

            // union types (except for [null, type]) need to be manually registered
            var unionSchema = schema as UnionSchema;
            var cache = new ClassCache();
            cache.LoadClassCache(typeof(Derived1), unionSchema[0]);
            cache.LoadClassCache(typeof(Derived2), unionSchema[1]);
            var x = schema as RecordSchema;

            var writer = new ReflectWriter<BaseClass>(schema, cache);
            var reader = new ReflectReader<BaseClass>(schema, schema, cache);

            using (var stream = new MemoryStream(256))
            {
                var encoder = new BinaryEncoder(stream);
                writer.Write(derived1write, encoder);
                writer.Write(derived2write, encoder);
                stream.Seek(0, SeekOrigin.Begin);

                var decoder = new BinaryDecoder(stream);
                var derived1read = (Derived1)reader.Read(decoder);
                var derived2read = (Derived2)reader.Read(decoder);
                Assert.AreEqual(derived1read.A, derived1write.A);
                Assert.AreEqual(derived1read.B, derived1write.B);
                Assert.AreEqual(derived2read.A, derived2write.A);
                Assert.AreEqual(derived2read.C, derived2write.C);
            }
        }

        /// <summary>
        /// If you fail to manually register types within a union that has more than one non-null
        /// schema, creating a <see cref="ReflectWriter{T}"/> throws an exception.
        /// </summary>
        [TestCase]
        public void ThrowsIfClassesNotLoadedTest()
        {
            var schema = Schema.Parse(BaseClassSchema);
            var cache = new ClassCache();
            Assert.Throws<AvroException>(() => new ReflectWriter<BaseClass>(schema, cache));
        }

        [TestCase]
        public void NullableTest()
        {
            var nullableSchema = @"
            [
                ""null"",
                { ""type"" : ""record"", ""name"" : ""Dervied2"", ""fields"" :
                    [
                        { ""name"" : ""A"", ""type"" : ""string""},
                        { ""name"" : ""C"", ""type"" : ""double""}
                    ]
                },

            ]
            ";
            var schema = Schema.Parse(nullableSchema);
            var derived2write = new Derived2() { A = "derived2", C = 3.14 };

            var writer = new ReflectWriter<Derived2>(schema);
            var reader = new ReflectReader<Derived2>(schema, schema);

            using (var stream = new MemoryStream(256))
            {
                var encoder = new BinaryEncoder(stream);
                writer.Write(derived2write, encoder);
                writer.Write(null, encoder);
                stream.Seek(0, SeekOrigin.Begin);

                var decoder = new BinaryDecoder(stream);
                var derived2read = reader.Read(decoder);
                var derived2null = reader.Read(decoder);
                Assert.AreEqual(derived2read.A, derived2write.A);
                Assert.AreEqual(derived2read.C, derived2write.C);
                Assert.IsNull(derived2null);
            }
        }

        public enum EnumMood
        {
            Happy,
            Sad,
            Cranky,
            Meh
        }

        [TestCase]
        public void NullableEnumTest()
        {
            var nullableSchema = @"
            [
                ""null"",
                { ""type"" : ""enum"", ""name"" : ""EnumMood"", ""symbols"" :
                    [
                        ""Happy"",
                        ""Sad"",
                        ""Cranky"",
                        ""Meh""
                    ]
                }
            ]
            ";

            var schema = Schema.Parse(nullableSchema);
            EnumMood? moodWrite = EnumMood.Meh;

            var writer = new ReflectWriter<EnumMood?>(schema);
            var reader = new ReflectReader<EnumMood?>(schema, schema);

            using (var stream = new MemoryStream(256))
            {
                var encoder = new BinaryEncoder(stream);
                writer.Write(moodWrite, encoder);
                writer.Write(null, encoder);
                stream.Seek(0, SeekOrigin.Begin);

                var decoder = new BinaryDecoder(stream);
                var moodRead = reader.Read(decoder);
                var moodNull = reader.Read(decoder);
                Assert.AreEqual(moodWrite, moodRead);
                Assert.IsNull(moodNull);
            }
        }

        [Test]
        public void NullableArrayTest()
        {
            var nullableSchema = @"
            [
                ""null"",
                { ""type"" : ""array"", ""items"" :
                    {
                        ""type"" : ""record"", ""name"": ""Derived2"", ""fields"": 
                        [
                            { ""name"" : ""A"", ""type"" : ""string""},
                            { ""name"" : ""C"", ""type"" : ""double""}
                        ]
                     }
                }
            ]
            ";

            var schema = Schema.Parse(nullableSchema);
            var recordsWrite = new List<Derived2>
            {
                new Derived2 {A = "derived2", C = 3.14},
                new Derived2 {A = "derived2.1", C = 42}
            };

            var writer = new ReflectWriter<List<Derived2>>(schema);
            var reader = new ReflectReader<List<Derived2>>(schema, schema);

            using (var stream = new MemoryStream(256))
            {
                var encoder = new BinaryEncoder(stream);
                writer.Write(recordsWrite, encoder);
                writer.Write(null, encoder);
                stream.Seek(0, SeekOrigin.Begin);

                var decoder = new BinaryDecoder(stream);
                var recordsRead = reader.Read(decoder);
                var recordsNull = reader.Read(decoder);
                Assert.AreEqual(recordsWrite, recordsRead);
                Assert.IsNull(recordsNull);
            }
        }

        [Test]
        public void HeterogeneousTest()
        {
            var heterogeneousSchema = @"
            [
                ""string"",
                ""null"",
                { ""type"" : ""record"", ""name"" : ""Dervied2"", ""fields"" :
                    [
                        { ""name"" : ""A"", ""type"" : ""string""},
                        { ""name"" : ""C"", ""type"" : ""double""}
                    ]
                },

            ]
            ";
            var schema = Schema.Parse(heterogeneousSchema);
            var derived2write = new Derived2() { A = "derived2", C = 3.14 };

            // union types (except for [null, type]) need to be manually registered
            var unionSchema = schema as UnionSchema;
            var cache = new ClassCache();
            cache.LoadClassCache(typeof(Derived2), unionSchema[2]);

            var writer = new ReflectWriter<object>(schema, cache);
            var reader = new ReflectReader<object>(schema, schema, cache);

            using (var stream = new MemoryStream(256))
            {
                var encoder = new BinaryEncoder(stream);
                writer.Write(derived2write, encoder);
                writer.Write("string value", encoder);
                writer.Write(null, encoder);
                stream.Seek(0, SeekOrigin.Begin);

                var decoder = new BinaryDecoder(stream);
                var derived2read = (Derived2)reader.Read(decoder);
                var stringRead = (string)reader.Read(decoder);
                var nullRead = reader.Read(decoder);
                Assert.AreEqual(derived2read.A, derived2write.A);
                Assert.AreEqual(derived2read.C, derived2write.C);
                Assert.AreEqual(stringRead, "string value");
                Assert.IsNull(nullRead);
            }
        }
    }
}
