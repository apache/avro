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

using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using Avro.IO;
using Avro.Reflect;
using NUnit.Framework;

namespace Avro.Test
{


    [TestFixture]
    public class TestArray
    {
        private class ListRec
        {
            public string S { get; set; }
        }

        private const string _simpleList = @"
        {
            ""namespace"": ""MessageTypes"",
            ""type"": ""array"",
            ""doc"": ""A simple list with a string."",
            ""name"": ""A"",
            ""items"": ""string""
        }";

        private const string _recordList = @"
        {
            ""namespace"": ""MessageTypes"",
            ""type"": ""array"",
            ""doc"": ""A list with a custom type containing a string."",
            ""name"": ""A"",
            ""items"": {
                ""type"": ""record"",
                ""doc"": ""A simple type with a fixed."",
                ""name"": ""A"",
                ""fields"": [
                    { ""name"" : ""S"", ""type"" : ""string"" }
                ]
            }
        }";

        [TestCase]
        public void ListTest()
        {
            var schema = Schema.Parse(_simpleList);
            var fixedRecWrite = new List<string>() {"value"};

            var writer = new ReflectWriter<List<string>>(schema);
            var reader = new ReflectReader<List<string>>(schema, schema);

            using (var stream = new MemoryStream(256))
            {
                writer.Write(fixedRecWrite, new BinaryEncoder(stream));
                stream.Seek(0, SeekOrigin.Begin);
                var fixedRecRead = reader.Read(new BinaryDecoder(stream));
                Assert.IsTrue(fixedRecRead.Count == 1);
                Assert.AreEqual(fixedRecWrite[0],fixedRecRead[0]);
            }
        }

        [TestCase]
        public void ListRecTest()
        {
            var schema = Schema.Parse(_recordList);
            var fixedRecWrite = new List<ListRec>() { new ListRec() { S = "hello"}};

            var writer = new ReflectWriter<List<ListRec>>(schema);
            var reader = new ReflectReader<List<ListRec>>(schema, schema);

            using (var stream = new MemoryStream(256))
            {
                writer.Write(fixedRecWrite, new BinaryEncoder(stream));
                stream.Seek(0, SeekOrigin.Begin);
                var fixedRecRead = reader.Read(new BinaryDecoder(stream));
                Assert.IsTrue(fixedRecRead.Count == 1);
                Assert.AreEqual(fixedRecWrite[0].S,fixedRecRead[0].S);
            }
        }

        [TestCase]
        public void ConcurrentQueueTest()
        {
            var schema = Schema.Parse(_recordList);
            var fixedRecWrite = new ConcurrentQueue<ListRec>();
            fixedRecWrite.Enqueue(new ListRec() { S = "hello"});

            var arrayHelper = new ReflectArrayHelper();
            arrayHelper.CountFunc = e=>(e as dynamic).Count;
            arrayHelper.AddAction = (e,v)=>(e as dynamic).Enqueue(v as ListRec);
            arrayHelper.ClearAction = e=>(e as dynamic).Clear();
            arrayHelper.ArrayType = typeof(ConcurrentQueue<>);

            var writer = new ReflectWriter<ConcurrentQueue<ListRec>>(schema, arrayHelper);
            var reader = new ReflectReader<ConcurrentQueue<ListRec>>(schema, schema, arrayHelper );

            using (var stream = new MemoryStream(256))
            {
                writer.Write(fixedRecWrite, new BinaryEncoder(stream));
                stream.Seek(0, SeekOrigin.Begin);
                var fixedRecRead = reader.Read(new BinaryDecoder(stream));
                Assert.IsTrue(fixedRecRead.Count == 1);
                Assert.AreEqual(fixedRecWrite[0].S,fixedRecRead[0].S);
            }
        }

    }
}
