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

using System.Collections;
using System.IO;
using NUnit.Framework;
using Avro.IO;
using Avro.Reflect;
using System.Collections.Generic;
using System;
using Avro.Specific;
using System.Linq;
using System.Diagnostics.CodeAnalysis;

namespace Avro.Test
{
    [TestFixture]
    class TestReflect
    {
        public class IDictionaryTestClass : IDictionary<string, int>
        {
            Dictionary<string, int> _pairs = new Dictionary<string, int>();
            public int this[string key] { get => _pairs[key]; set => _pairs[key] = value; }

            public ICollection<string> Keys => _pairs.Keys;

            public ICollection<int> Values => _pairs.Values;

            public int Count => _pairs.Count;

            public bool IsReadOnly => throw new NotImplementedException("readonly");

            public void Add(string key, int value) => _pairs.Add(key, value);
            public void Add(KeyValuePair<string, int> item) => throw new NotImplementedException("add_kvp");
            public void Clear() => throw new NotImplementedException("clear");
            public bool Contains(KeyValuePair<string, int> item) => throw new NotImplementedException("contains");
            public bool ContainsKey(string key) => throw new NotImplementedException("containskey");
            public void CopyTo(KeyValuePair<string, int>[] array, int arrayIndex) => throw new NotImplementedException("copyto");
            public IEnumerator<KeyValuePair<string, int>> GetEnumerator() => _pairs.GetEnumerator();
            public bool Remove(string key) => throw new NotImplementedException("remove");
            public bool Remove(KeyValuePair<string, int> item) => throw new NotImplementedException("remove_kvp");
            public bool TryGetValue(string key, out int value) => throw new NotImplementedException("trygetvalue");
            IEnumerator IEnumerable.GetEnumerator() => _pairs.GetEnumerator();
            public static implicit operator IDictionaryTestClass(Dictionary<string,int> instance)
            {
                IDictionaryTestClass result = new IDictionaryTestClass();
                foreach (var item in instance)
                {
                    result.Add(item.Key, item.Value);
                }
                return result;
            }
        }

        public class TestClassNestedInheritance : IDictionaryTestClass {}

        public class TestClassWithPropertyTestingInheritance
        {
            public TestClassNestedInheritance prop { get; set; }
        }

        public class DictionaryTestClass
        {
            public Dictionary<int, int> p { get; set; }
            public NestedTestClass ntc { get; set; }


            public class NestedTestClass
            {
                public int NestedTestClassInt { get; set; }
            }
        }

        public class DictionaryTestClass2
        {
            public Dictionary<int, string> p { get; set; }

        }
        class TestMapConverter : IAvroFieldConverter
        {
            public object FromAvroType(object o, Schema s) =>
                ((Dictionary<string, int>)o).ToDictionary(x => int.Parse(x.Key), y=>y.Value);
                
            
            public Type GetAvroType() => typeof(IDictionary<string, int>);
            public Type GetPropertyType() => typeof(Dictionary<int, int>);
            public object ToAvroType(object o, Schema s) =>
                ((Dictionary<int, int>)o).ToDictionary(x => x.Key.ToString(), y => y.Value);
        }

        class TestConverterForNestedInheritance : IAvroFieldConverter
        {
            public object FromAvroType(object o, Schema s)
            {
                TestClassNestedInheritance result = new TestClassNestedInheritance();
                foreach(var kvp in (Dictionary<string, int>)o)
                {
                    result.Add(kvp.Key, kvp.Value);
                }
                return result;
            }


            public Type GetAvroType() => typeof(IDictionary<string, int>);
            public Type GetPropertyType() => typeof(TestClassNestedInheritance);
            public object ToAvroType(object o, Schema s) =>
                ((IDictionary<string, int>)o).ToDictionary(x => x.Key.ToString(), y => y.Value);
        }


        [TestCase]
        public void TestMapWithConverterSucceeds()
        {
            ClassCache.AddDefaultConverter(new TestMapConverter());
            
            var schemaJson = "{\"fields\":[{\"name\":\"ntc\",\"type\":{\"type\":\"record\",\"name\":\"NestedTestClass\",\"fields\":[{\"name\":\"NestedTestClassInt\",\"type\":\"int\"}]}},{\"type\":{\"values\":\"int\",\"type\":\"map\"},\"name\":\"p\"}],\"type\":\"record\",\"name\":\"DictionaryTestClass\",\"namespace\":\"Avro.Test.TestReflect\\u002B\"}";
            var schema = Schema.Parse(schemaJson);
            DictionaryTestClass expected = new DictionaryTestClass() { p = new Dictionary<int, int>() { { 1, 1 }, { 2, 4 }, { 3, 5 } } , ntc = new DictionaryTestClass.NestedTestClass() { NestedTestClassInt = 1 } };

            using Stream stream =  serialize(schema, expected);
            var avroReader = new ReflectReader<DictionaryTestClass>(schema, schema);    
            stream.Position = 0;
            var actual = avroReader.Read(null, new BinaryDecoder(stream));
           
            CollectionAssert.AreEquivalent(expected.p, actual.p);
        }

        [TestCase]
        public void TestMapWithoutConverterFails()
        {
            var schemaJson = "{\"fields\":[{\"type\":{\"values\":\"string\",\"type\":\"map\"},\"name\":\"p\"}],\"type\":\"record\",\"name\":\"DictionaryTestClass2\",\"namespace\":\"Avro.Test.TestReflect\\u002B\"}";
            var schema = Schema.Parse(schemaJson);
            var ex = Assert.Throws<AvroException>(() => new ReflectWriter<DictionaryTestClass2>(schema));
            var ex2 = Assert.Throws<AvroException>(() => new ReflectReader<DictionaryTestClass2>(schema, schema));
            Assert.AreEqual("Property p in object Avro.Test.TestReflect+DictionaryTestClass2 isn't compatible with Avro schema type Map", ex.Message);
            Assert.AreEqual("Property p in object Avro.Test.TestReflect+DictionaryTestClass2 isn't compatible with Avro schema type Map", ex2.Message);
        }

        public class DictionaryTestClass3
        {
            public Hashtable p { get; set; }
        }

        [TestCase]
        public void TestHashmapFailsBecauseItDoesntImplmentIDictionaryTKeyTValue()
        {
            var schemaJson = "{\"fields\":[{\"type\":{\"values\":\"string\",\"type\":\"map\"},\"name\":\"p\"}],\"type\":\"record\",\"name\":\"DictionaryTestClass3\",\"namespace\":\"Avro.Test.TestReflect\\u002B\"}";
            var schema = Schema.Parse(schemaJson);            
            var ex = Assert.Throws<AvroException>(() => new ReflectWriter<DictionaryTestClass3>(schema));
            Assert.AreEqual("Property p in object Avro.Test.TestReflect+DictionaryTestClass3 isn't compatible with Avro schema type Map", ex.Message);

        }


        [TestCase]
        public void TestConverterForNestedInheritanceSucceeds()
        {
            ClassCache.AddDefaultConverter(new TestConverterForNestedInheritance());

            var schemaJson = "{\"fields\":[{\"type\":{\"values\":\"int\",\"type\":\"map\"},\"name\":\"prop\"}],\"type\":\"record\",\"name\":\"TestClassWithTestClassA\",\"namespace\":\"Avro.Test.TestReflect\\u002B\"}";
            var schema = Schema.Parse(schemaJson);


            TestClassWithPropertyTestingInheritance expected = new TestClassWithPropertyTestingInheritance() { prop = new TestClassNestedInheritance() };
            expected.prop["1"] = 1;

            using Stream stream = serialize(schema, expected);
            var avroReader = new ReflectReader<TestClassWithPropertyTestingInheritance>(schema, schema);
            stream.Position = 0;
            var actual = avroReader.Read(null, new BinaryDecoder(stream));

            CollectionAssert.AreEquivalent(expected.prop, actual.prop);   

        }

        enum EnumResolutionEnum
        {
            THIRD,
            FIRST,
            SECOND
        }

        class EnumResolutionRecord
        {
            public EnumResolutionEnum enumType { get; set; }
        }

        [TestCase]
        public void TestEnumResolution()
        {
            Schema writerSchema = Schema.Parse("{\"type\":\"record\",\"name\":\"EnumRecord\",\"namespace\":\"Avro.Test\"," +
                                        "\"fields\":[{\"name\":\"enumType\",\"type\": { \"type\": \"enum\", \"name\": \"EnumType\", \"symbols\": [\"FIRST\", \"SECOND\"]} }]}");

            var testRecord = new EnumResolutionRecord();

            Schema readerSchema = Schema.Parse("{\"type\":\"record\",\"name\":\"EnumRecord\",\"namespace\":\"Avro.Test\"," +
                                        "\"fields\":[{\"name\":\"enumType\",\"type\": { \"type\": \"enum\", \"name\":" +
                                        " \"EnumType\", \"symbols\": [\"THIRD\", \"FIRST\", \"SECOND\"]} }]}");;
            testRecord.enumType = EnumResolutionEnum.SECOND;

            // serialize
            var stream = serialize(writerSchema, testRecord);

            // deserialize
            var rec2 = deserialize<EnumResolutionRecord>(stream, writerSchema, readerSchema);
            Assert.AreEqual( EnumResolutionEnum.SECOND, rec2.enumType );
        }

        private static S deserialize<S>(Stream ms, Schema ws, Schema rs) where S : class
        {
            long initialPos = ms.Position;
            var r = new ReflectReader<S>(ws, rs);
            Decoder d = new BinaryDecoder(ms);
            S output = r.Read(null, d);
            Assert.AreEqual(ms.Length, ms.Position); // Ensure we have read everything.
            checkAlternateDeserializers(output, ms, initialPos, ws, rs);
            return output;
        }

        private static void checkAlternateDeserializers<S>(S expected, Stream input, long startPos, Schema ws, Schema rs) where S : class
        {
            input.Position = startPos;
            var reader = new ReflectReader<S>(ws, rs);
            Decoder d = new BinaryDecoder(input);
            S output = reader.Read(null, d);
            Assert.AreEqual(input.Length, input.Position); // Ensure we have read everything.
            AssertReflectRecordEqual(rs, expected, ws, output, reader.Reader.ClassCache);
        }

        private static Stream serialize<T>(Schema ws, T actual)
        {
            var ms = new MemoryStream();
            Encoder e = new BinaryEncoder(ms);
            var w = new ReflectWriter<T>(ws);
            w.Write(actual, e);
            ms.Flush();
            ms.Position = 0;
            checkAlternateSerializers(ms.ToArray(), actual, ws);
            return ms;
        }

        private static void checkAlternateSerializers<T>(byte[] expected, T value, Schema ws)
        {
            var ms = new MemoryStream();
            var writer = new ReflectWriter<T>(ws);
            var e = new BinaryEncoder(ms);
            writer.Write(value, e);
            var output = ms.ToArray();

            Assert.AreEqual(expected.Length, output.Length);
            Assert.True(expected.SequenceEqual(output));
        }

        private static void AssertReflectRecordEqual(Schema schema1, object rec1, Schema schema2, object rec2, ClassCache cache)
        {
            var recordSchema = (RecordSchema) schema1;
            foreach (var f in recordSchema.Fields)
            {
                var rec1Val = cache.GetClass(recordSchema).GetValue(rec1, f);
                var rec2Val = cache.GetClass(recordSchema).GetValue(rec2, f);
                if (rec1Val.GetType().IsClass)
                {
                    AssertReflectRecordEqual(f.Schema, rec1Val, f.Schema, rec2Val, cache);
                }
                else if (rec1Val is IList) //I don't see when this or the last block would be evaluated.  Isn't anything that is IList or IDictionary going to be a class?
                {
                    var schema1List = f.Schema as ArraySchema;
                    var rec1List = (IList) rec1Val;
                    if( rec1List.Count > 0 )
                    {
                        var rec2List = (IList) rec2Val;
                        Assert.AreEqual(rec1List.Count, rec2List.Count);
                        for (int j = 0; j < rec1List.Count; j++)
                        {
                            AssertReflectRecordEqual(schema1List.ItemSchema, rec1List[j], schema1List.ItemSchema, rec2List[j], cache);
                        }
                    }
                    else
                    {
                        Assert.AreEqual(rec1Val, rec2Val);
                    }
                }
                else if (rec1Val is IDictionary)
                {
                    var schema1Map = f.Schema as MapSchema;
                    var rec1Dict = (IDictionary) rec1Val;
                    var rec2Dict = (IDictionary) rec2Val;
                    Assert.AreEqual(rec2Dict.Count, rec2Dict.Count);
                    foreach (var key in rec1Dict.Keys)
                    {
                        var val1 = rec1Dict[key];
                        var val2 = rec2Dict[key];
                        if (f.Schema is RecordSchema)
                        {
                            AssertReflectRecordEqual(f.Schema as RecordSchema, val1, f.Schema as RecordSchema, val2, cache);
                        }
                        else
                        {
                            Assert.AreEqual(val1, val2);
                        }
                    }
                }
                else
                {
                    Assert.AreEqual(rec1Val, rec2Val);
                }
            }
        }
    }
}
