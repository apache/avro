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

namespace Avro.Test.CodeGen
{
    [TestFixture]

    class CodeGenTest
    {

        [Test]
        public void TestGetNullableTypeException()
        {
            Assert.Throws<ArgumentNullException>(() => Avro.CodeGen.GetNullableType(null));
        }

        [TestFixture]
        public class CodeGenTestClass : Avro.CodeGen
        {
            [Test]
            public void TestGenerateNamesException()
            {
                Protocol protocol = null;
                Assert.Throws<ArgumentNullException>(() => this.GenerateNames(protocol));
            }

            // No mapping
            [TestCase(null, null, "my.avro.ns")]
            // Self mapping
            [TestCase("my.avro.ns", "my.avro.ns", "my.avro.ns")]
            // Full mappings
            [TestCase("my.avro.ns", "my", "my")]
            [TestCase("my.avro.ns", "my.csharp.ns", "my.csharp.ns")]
            [TestCase("my.avro.ns", "my.whatever.verylong.complicated.ns", "my.whatever.verylong.complicated.ns")]
            // Partial mappings
            [TestCase("my.avro", "your.csharp", "your.csharp.ns")]
            [TestCase("my.avro", "your.whatever.verylong.complicated.csharp", "your.whatever.verylong.complicated.csharp.ns")]
            [TestCase("my", "your", "your.avro.ns")]
            [TestCase("my", "your.whatever.verylong.complicated", "your.whatever.verylong.complicated.avro.ns")]
            public void TestNamespaceMapping(string mapNamespaceFrom, string mapNamespaceTo, string expectedFullNamespace)
            {
                string schemaText = @"
{
  ""type"" : ""record"",
  ""name"" : ""TestModel"",
  ""namespace"" : ""my.avro.ns"",
  ""fields"" : [ {
    ""name"" : ""eventType"",
    ""type"" : {
      ""type"" : ""enum"",
      ""name"" : ""EventType"",
      ""symbols"" : [ ""CREATE"", ""UPDATE"", ""DELETE"" ]
    }
  }]
}";

                Avro.CodeGen codegen = new Avro.CodeGen();

                if (mapNamespaceFrom == null)
                {
                    // No mapping
                    codegen.AddSchema(schemaText);
                }
                else
                {
                    // Use mapping
                    codegen.AddSchema(schemaText, new Dictionary<string, string>() { { mapNamespaceFrom, mapNamespaceTo } });
                }

                // Check top level namespace
                Assert.AreEqual(1, codegen.Schemas.Count);
                RecordSchema schema = (RecordSchema)codegen.Schemas[0];
                Assert.AreEqual(expectedFullNamespace, schema.Namespace);
                Assert.AreEqual($"TestModel", schema.Name);
                Assert.AreEqual($"{expectedFullNamespace}.TestModel", schema.Fullname);

                // Check namespace for the field
                Assert.AreEqual(1, schema.Fields.Count);
                Field field = schema.Fields[0];
                EnumSchema fieldSchema = (EnumSchema)field.Schema;
                Assert.AreEqual(expectedFullNamespace, fieldSchema.Namespace);
                Assert.AreEqual($"EventType", fieldSchema.Name);
                Assert.AreEqual($"{expectedFullNamespace}.EventType", fieldSchema.Fullname);
            }
        }
    }
}
