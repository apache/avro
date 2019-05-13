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
using System.Collections.Generic;
using Avro;
using Avro.IO;
using Avro.Generic;
using Avro.Specific;
using Avro.POCO;
using NUnit.Framework;

namespace Avro.Test
{
    [TestFixture]
    public class TestFromAvroProject
    {
        private string _avroTestSchemaV1 = @"{
        ""protocol"" : ""MyProtocol"",
        ""namespace"" : ""com.foo"",
        ""types"" :
        [
            {
                ""type"" : ""record"",
                ""name"" : ""A"",
                ""fields"" : [ { ""name"" : ""f1"", ""type"" : ""long"" } ]
            },
            {
                ""type"" : ""enum"",
                ""name"" : ""MyEnum"",
                ""symbols"" : [ ""A"", ""B"", ""C"" ]
            },
            {
                ""type"": ""fixed"",
                ""size"": 16,
                ""name"": ""MyFixed""
            },
            {
                ""type"" : ""record"",
                ""name"" : ""Z"",
                ""fields"" :
                [
                    { ""name"" : ""myUInt"", ""type"" : [ ""int"", ""null"" ] },
                    { ""name"" : ""myULong"", ""type"" : [ ""long"", ""null"" ] },
                    { ""name"" : ""myUBool"", ""type"" : [ ""boolean"", ""null"" ] },
                    { ""name"" : ""myUDouble"", ""type"" : [ ""double"", ""null"" ] },
                    { ""name"" : ""myUFloat"", ""type"" : [ ""float"", ""null"" ] },
                    { ""name"" : ""myUBytes"", ""type"" : [ ""bytes"", ""null"" ] },
                    { ""name"" : ""myUString"", ""type"" : [ ""string"", ""null"" ] },
                    { ""name"" : ""myInt"", ""type"" : ""int"" },
                    { ""name"" : ""myLong"", ""type"" : ""long"" },
                    { ""name"" : ""myBool"", ""type"" : ""boolean"" },
                    { ""name"" : ""myDouble"", ""type"" : ""double"" },
                    { ""name"" : ""myFloat"", ""type"" : ""float"" },
                    { ""name"" : ""myBytes"", ""type"" : ""bytes"" },
                    { ""name"" : ""myString"", ""type"" : ""string"" },
                    { ""name"" : ""myNull"", ""type"" : ""null"" },
                    { ""name"" : ""myFixed"", ""type"" : ""MyFixed"" },
                    { ""name"" : ""myA"", ""type"" : ""A"" },
                    { ""name"" : ""myE"", ""type"" : ""MyEnum"" },
                    { ""name"" : ""myArray"", ""type"" : { ""type"" : ""array"", ""items"" : ""bytes"" } },
                    { ""name"" : ""myArray2"", ""type"" : { ""type"" : ""array"", ""items"" : { ""type"" : ""record"", ""name"" : ""newRec"", ""fields"" : [ { ""name"" : ""f1"", ""type"" : ""long""} ] } } },
                    { ""name"" : ""myMap"", ""type"" : { ""type"" : ""map"", ""values"" : ""string"" } },
                    { ""name"" : ""myMap2"", ""type"" : { ""type"" : ""map"", ""values"" : ""newRec"" } },
                    { ""name"" : ""myObject"", ""type"" : [ ""MyEnum"", ""A"", ""null"" ] },
                    { ""name"" : ""myArray3"", ""type"" : { ""type"" : ""array"", ""items"" : { ""type"" : ""array"", ""items"" : [ ""double"", ""string"", ""null"" ] } } }
                ]
            }
        ]
    }";

        public Z SerializeDeserialize(Z z)
        {
            try
            {
                Protocol protocol = Protocol.Parse(_avroTestSchemaV1);
                Schema schema = null;
                foreach (var s in protocol.Types)
                {
                    if (s.Name == "Z")
                    {
                        schema = s;
                    }
                }

                var avroWriter = new POCOWriter<Z>(schema);
                var avroReader = new POCOReader<Z>(schema, schema);

                byte[] serialized;

                using (var stream = new MemoryStream(256))
                {
                    avroWriter.Write(z, new BinaryEncoder(stream));
                    serialized = stream.ToArray();
                }

                Z deserialized = null;
                using (var stream = new MemoryStream(serialized))
                {
                    deserialized = avroReader.Read(default(Z), new BinaryDecoder(stream));
                }

                return deserialized;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                throw ex;
            }
        }

        [TestCase]
        public void DefaultZ()
        {
            var z = new Z()
            {
                myBytes = new byte[10],
                myString = "123",
                myFixed = new byte[16],
                myA = new A(),
                myArray = new List<byte[]>(),
                myArray2 = new List<newRec>(),
                myMap = new Dictionary<string, string>(),
                myMap2 = new Dictionary<string, newRec>(),
                myArray3 = new List<List<object>>()
            };

            var zz = SerializeDeserialize(z);
        }

        [TestCase]
        public void PopulatedZ()
        {
            var z = new Z()
            {
                myUInt = 1,
                myULong = 2L,
                myUBool = true,
                myUDouble = 3.14,
                myUFloat = (float)1.59E-3,
                myUBytes = new byte[3] { 0x01, 0x02, 0x03 },
                myUString = "abc",
                myInt = 1,
                myLong = 2L,
                myBool = true,
                myDouble = 3.14,
                myFloat = (float)1.59E-2,
                myBytes = new byte[3] { 0x01, 0x02, 0x03 },
                myString = "def",
                myNull = null,
                myFixed = new byte[16] { 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04 },
                myA = new A() { f1 = 3L },
                myE = MyEnum.B,
                myArray = new List<byte[]>() { new byte[] { 0x01, 0x02, 0x03, 0x04 } },
                myArray2 = new List<newRec>() { new newRec() { f1 = 4L } },
                myMap = new Dictionary<string, string>()
                {
                    ["abc"] = "123"
                },
                myMap2 = new Dictionary<string, newRec>()
                {
                    ["abc"] = new newRec() { f1 = 5L }
                },
                myObject = new A() { f1 = 6L },
                myArray3 = new List<List<object>>() { new List<object>() { 7.0, "def" } }
            };

            var zz = SerializeDeserialize(z);
        }
    }
}
