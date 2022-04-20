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
using System.IO;
using System.Linq;
using System.Reflection;
using System.Collections.Generic;
using Microsoft.CodeAnalysis;
using NUnit.Framework;
using Avro.Specific;

namespace Avro.Test.AvroGen
{
    [TestFixture]

    class AvroGenSchemaTests
    {
        private const string _customConversionWithLogicalTypes = @"
{
  ""namespace"": ""org.apache.avro.codegentest.testdata"",
  ""type"": ""record"",
  ""name"": ""CustomConversionWithLogicalTypes"",
  ""doc"" : ""Test custom conversion and logical types in generated Java classes"",
  ""fields"": [
    {
      ""name"": ""customEnum"",
      ""type"": [""null"", {
        ""namespace"": ""org.apache.avro.codegentest.testdata"",
        ""name"": ""CustomAvroEnum"",
        ""type"": ""enum"",
        ""logicalType"": ""custom-enum"",
        ""symbols"": [""ONE"", ""TWO"", ""THREE""]
    }]
    }]
}
";

        private const string _logicalTypesWithCustomConversion = @"
{
""namespace"": ""org.apache.avro.codegentest.testdata"",
  ""type"": ""record"",
  ""name"": ""LogicalTypesWithCustomConversion"",
  ""doc"" : ""Test unions with logical types in generated Java classes"",
  ""fields"": [
    {""name"": ""nullableCustomField"",  ""type"": [""null"", {""type"": ""bytes"", ""logicalType"": ""decimal"", ""precision"": 9, ""scale"": 2}], ""default"": null},
    { ""name"": ""nonNullCustomField"",  ""type"": { ""type"": ""bytes"", ""logicalType"": ""decimal"", ""precision"": 9, ""scale"": 2} },
    { ""name"": ""nullableFixedSizeString"",  ""type"": [""null"", { ""type"": ""bytes"", ""logicalType"": ""fixed-size-string"", ""minLength"": 1, ""maxLength"": 50}], ""default"": null},
    { ""name"": ""nonNullFixedSizeString"",  ""type"": { ""type"": ""bytes"", ""logicalType"": ""fixed-size-string"", ""minLength"": 1, ""maxLength"": 50} }
  ]
}
";

        private const string _logicalTypesWithDefaults = @"
{
""namespace"": ""org.apache.avro.codegentest.testdata"",
  ""type"": ""record"",
  ""name"": ""LogicalTypesWithDefaults"",
  ""doc"" : ""Test logical types and default values in generated Java classes"",
  ""fields"": [
    {""name"": ""nullableDate"",  ""type"": [{""type"": ""int"", ""logicalType"": ""date""}, ""null""], ""default"": 1234},
    { ""name"": ""nonNullDate"",  ""type"": { ""type"": ""int"", ""logicalType"": ""date""}, ""default"": 1234}
  ]
}";

        private const string _nestedLogicalTypesArray = @"
{""namespace"": ""org.apache.avro.codegentest.testdata"",
  ""type"": ""record"",
  ""name"": ""NestedLogicalTypesArray"",
  ""doc"" : ""Test nested types with logical types in generated Java classes"",
  ""fields"": [
    {
      ""name"": ""arrayOfRecords"",
      ""type"": {
        ""type"": ""array"",
        ""items"": {
          ""namespace"": ""org.apache.avro.codegentest.testdata"",
          ""name"": ""RecordInArray"",
          ""type"": ""record"",
          ""fields"": [
            {
              ""name"": ""nullableDateField"",
              ""type"": [""null"", {""type"": ""int"", ""logicalType"": ""date""}]
            }
          ]
        }
      }
    }]
}
";

        private const string _nestedLogicalTypesMap = @"
{""namespace"": ""org.apache.avro.codegentest.testdata"",
  ""type"": ""record"",
  ""name"": ""NestedLogicalTypesMap"",
  ""doc"" : ""Test nested types with logical types in generated Java classes"",
  ""fields"": [
    {
      ""name"": ""mapOfRecords"",
      ""type"": {
        ""type"": ""map"",
        ""values"": {
          ""namespace"": ""org.apache.avro.codegentest.testdata"",
          ""name"": ""RecordInMap"",
          ""type"": ""record"",
          ""fields"": [
            {
              ""name"": ""nullableDateField"",
              ""type"": [""null"", {""type"": ""int"", ""logicalType"": ""date""}]
            }
          ]
        }
      }
    }]
}";

        private const string _nestedLogicalTypesRecord = @"
{""namespace"": ""org.apache.avro.codegentest.testdata"",
  ""type"": ""record"",
  ""name"": ""NestedLogicalTypesRecord"",
  ""doc"" : ""Test nested types with logical types in generated Java classes"",
  ""fields"": [
    {
      ""name"": ""nestedRecord"",
      ""type"": {
        ""namespace"": ""org.apache.avro.codegentest.testdata"",
        ""type"": ""record"",
        ""name"": ""NestedRecord"",
        ""fields"": [
          {
            ""name"": ""nullableDateField"",
            ""type"": [""null"", {""type"": ""int"", ""logicalType"": ""date""}]
          }
        ]
      }
    }]
}";

        private const string _nestedLogicalTypesUnionFixedDecimal = @"
{""namespace"": ""org.apache.avro.codegentest.testdata"",
  ""type"": ""record"",
  ""name"": ""NestedLogicalTypesUnionFixedDecimal"",
  ""doc"" : ""Test nested types with logical types in generated Java classes"",
  ""fields"": [
    {
      ""name"": ""unionOfFixedDecimal"",
      ""type"": [""null"", {
        ""namespace"": ""org.apache.avro.codegentest.testdata"",
        ""name"": ""FixedInUnion"",
        ""type"": {
            ""type"": ""fixed"",
            ""size"": 12,
            ""name"": ""FixedName"",
        },
        ""logicalType"": ""decimal"",
        ""precision"": 28,
        ""scale"": 15
      }]
    }]
}";

        private const string _nestedLogicalTypesUnion = @"
{""namespace"": ""org.apache.avro.codegentest.testdata"",
  ""type"": ""record"",
  ""name"": ""NestedLogicalTypesUnion"",
  ""doc"" : ""Test nested types with logical types in generated Java classes"",
  ""fields"": [
    {
      ""name"": ""unionOfRecords"",
      ""type"": [""null"", {
        ""namespace"": ""org.apache.avro.codegentest.testdata"",
        ""name"": ""RecordInUnion"",
        ""type"": ""record"",
        ""fields"": [
          {
            ""name"": ""nullableDateField"",
            ""type"": [""null"", {""type"": ""int"", ""logicalType"": ""date""}]
          }
        ]
      }]
    }]
}";

        private const string _nestedSomeNamespaceRecord = @"
{""namespace"": ""org.apache.avro.codegentest.some"",
  ""type"": ""record"",
  ""name"": ""NestedSomeNamespaceRecord"",
  ""doc"" : ""Test nested types with different namespace than the outer type"",
  ""fields"": [
    {
      ""name"": ""nestedRecord"",
      ""type"": {
        ""namespace"": ""org.apache.avro.codegentest.other"",
        ""type"": ""record"",
        ""name"": ""NestedOtherNamespaceRecord"",
        ""fields"": [
          {
            ""name"": ""someField"",
            ""type"": ""int""
          }
        ]
      }
    }]
}";

        private const string _nullableLogicalTypesArray = @"
{""namespace"": ""org.apache.avro.codegentest.testdata"",
  ""type"": ""record"",
  ""name"": ""NullableLogicalTypesArray"",
  ""doc"" : ""Test nested types with logical types in generated Java classes"",
  ""fields"": [
    {
      ""name"": ""arrayOfLogicalType"",
      ""type"": {
        ""type"": ""array"",
        ""items"": [""null"", {""type"": ""int"", ""logicalType"": ""date""}]
      }
    }]
}";

        private const string _nullableLogicalTypes = @"
{""namespace"": ""org.apache.avro.codegentest.testdata"",
  ""type"": ""record"",
  ""name"": ""NullableLogicalTypes"",
  ""doc"" : ""Test unions with logical types in generated Java classes"",
  ""fields"": [
    {""name"": ""nullableDate"",  ""type"": [""null"", {""type"": ""int"", ""logicalType"": ""date""}], ""default"": null}
  ]
}";

        private const string _stringLogicalType = @"
{
  ""namespace"": ""org.apache.avro.codegentest.testdata"",
  ""type"": ""record"",
  ""name"": ""StringLogicalType"",
  ""doc"": ""Test logical type applied to field of type string"",
  ""fields"": [
    {
      ""name"": ""someIdentifier"",
      ""type"": {
        ""type"": ""string"",
        ""logicalType"": ""uuid""
      }
},
    {
    ""name"": ""someJavaString"",
      ""type"": ""string"",
      ""doc"": ""Just to ensure no one removed <stringType>String</stringType> because this is the basis of this test""
    }
  ]
}";

        // https://issues.apache.org/jira/browse/AVRO-2883
        private const string _schema_avro_2883 = @"
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
} ]
}";

        // https://issues.apache.org/jira/browse/AVRO-3046
        private const string _schema_avro_3046 = @"
{
  ""type"": ""record"",
  ""name"": ""ExampleRecord"",
  ""namespace"": ""com.example"",
  ""fields"": [
    {
      ""name"": ""Id"",
      ""type"": ""string"",
      ""logicalType"": ""UUID""
    },
    {
      ""name"": ""InnerRecord"",
      ""type"": {
        ""type"": ""record"",
        ""name"": ""InnerRecord"",
        ""fields"": [
          {
            ""name"": ""Id"",
            ""type"": ""string"",
            ""logicalType"": ""UUID""
          }
        ]
      }
    }
  ]
}";

        private Assembly TestSchema(
            string schema,
            IEnumerable<string> typeNamesToCheck = null,
            IEnumerable<KeyValuePair<string, string>> namespaceMapping = null,
            IEnumerable<string> generatedFilesToCheck = null)
        {
            // Create temp folder
            string outputDir = AvroGenHelper.CreateEmptyTemporyFolder(out string uniqueId);

            try
            {
                // Save schema
                string schemaFileName = Path.Combine(outputDir, $"{uniqueId}.avsc");
                System.IO.File.WriteAllText(schemaFileName, schema);

                // Generate from schema file
                Assert.That(AvroGenTool.GenSchema(schemaFileName, outputDir, namespaceMapping ?? new Dictionary<string, string>(), false), Is.EqualTo(0));

                // Check if all generated files exist
                if (generatedFilesToCheck != null)
                {
                    foreach (string generatedFile in generatedFilesToCheck)
                    {
                        Assert.That(new FileInfo(Path.Combine(outputDir, generatedFile)), Does.Exist);
                    }
                }

                // Compile into netstandard library and load assembly
                Assembly assembly = AvroGenHelper.CompileCSharpFilesIntoLibrary(
                    new DirectoryInfo(outputDir)
                        .EnumerateFiles("*.cs", SearchOption.AllDirectories)
                        .Select(fi => fi.FullName),
                        uniqueId);

                if (typeNamesToCheck != null)
                {
                    // Check if the compiled code has the same number of types defined as the check list
                    Assert.That(typeNamesToCheck.Count(), Is.EqualTo(assembly.DefinedTypes.Count()));

                    // Check if types available in compiled assembly
                    foreach (string typeName in typeNamesToCheck)
                    {
                        Type type = assembly.GetType(typeName);
                        Assert.That(type, Is.Not.Null);

                        // Instantiate
                        object obj = Activator.CreateInstance(type);
                        Assert.That(obj, Is.Not.Null);
                    }
                }

                // Verify GeneratedCodeAttribute
                foreach(System.Reflection.TypeInfo definedType in assembly.DefinedTypes)
                {
                    var generatedAttributes = definedType.CustomAttributes.Where(x => x.AttributeType.FullName == "System.CodeDom.Compiler.GeneratedCodeAttribute");
                    Assert.That(generatedAttributes, Is.Not.Null);
                }

                return assembly;
            }
            finally
            {
                Directory.Delete(outputDir, true);
            }
        }

        [TestCase(
            _logicalTypesWithDefaults,
            new string[]
            {
                "org.apache.avro.codegentest.testdata.LogicalTypesWithDefaults"
            },
            new string[]
            {
                "org/apache/avro/codegentest/testdata/LogicalTypesWithDefaults.cs"
            })]
        [TestCase(
            _nestedLogicalTypesArray,
            new string[]
            {
                "org.apache.avro.codegentest.testdata.NestedLogicalTypesArray",
                "org.apache.avro.codegentest.testdata.RecordInArray"
            },
            new string[]
            {
                "org/apache/avro/codegentest/testdata/NestedLogicalTypesArray.cs",
                "org/apache/avro/codegentest/testdata/RecordInArray.cs"
            })]
        [TestCase(
            _nestedLogicalTypesMap,
            new string[]
            {
                "org.apache.avro.codegentest.testdata.NestedLogicalTypesMap",
                "org.apache.avro.codegentest.testdata.RecordInMap"
            },
            new string[]
            {
                "org/apache/avro/codegentest/testdata/NestedLogicalTypesMap.cs",
                "org/apache/avro/codegentest/testdata/RecordInMap.cs"
            })]
        [TestCase(
            _nestedLogicalTypesRecord,
            new string[]
            {
                "org.apache.avro.codegentest.testdata.NestedLogicalTypesRecord",
                "org.apache.avro.codegentest.testdata.NestedRecord"
            },
            new string[]
            {
                "org/apache/avro/codegentest/testdata/NestedLogicalTypesRecord.cs",
                "org/apache/avro/codegentest/testdata/NestedRecord.cs"
            })]
        [TestCase(
            _nestedLogicalTypesUnion,
            new string[]
            {
                "org.apache.avro.codegentest.testdata.NestedLogicalTypesUnion",
                "org.apache.avro.codegentest.testdata.RecordInUnion"
            },
            new string[]
            {
                "org/apache/avro/codegentest/testdata/NestedLogicalTypesUnion.cs",
                "org/apache/avro/codegentest/testdata/RecordInUnion.cs"
            })]
        [TestCase(
            _nestedSomeNamespaceRecord,
            new string[]
            {
                "org.apache.avro.codegentest.some.NestedSomeNamespaceRecord",
                "org.apache.avro.codegentest.other.NestedOtherNamespaceRecord"
            },
            new string[]
            {
                "org/apache/avro/codegentest/some/NestedSomeNamespaceRecord.cs",
                "org/apache/avro/codegentest/other/NestedOtherNamespaceRecord.cs"
            })]
        [TestCase(
            _nestedLogicalTypesUnionFixedDecimal,
            new string[]
            {
                "org.apache.avro.codegentest.testdata.NestedLogicalTypesUnionFixedDecimal"
            },
            new string[]
            {
                "org/apache/avro/codegentest/testdata/NestedLogicalTypesUnionFixedDecimal.cs"
            })]
        [TestCase(
            _nullableLogicalTypes,
            new string[]
            {
               "org.apache.avro.codegentest.testdata.NullableLogicalTypes"
            },
            new string[]
            {
               "org/apache/avro/codegentest/testdata/NullableLogicalTypes.cs"
            })]
        [TestCase(
            _nullableLogicalTypesArray,
            new string[]
            {
                "org.apache.avro.codegentest.testdata.NullableLogicalTypesArray"
            },
            new string[]
            {
                "org/apache/avro/codegentest/testdata/NullableLogicalTypesArray.cs"
            })]
        [TestCase(
            _schema_avro_2883,
            new string[]
            {
                "my.avro.ns.TestModel",
                "my.avro.ns.EventType",
            },
            new string[]
            {
                "my/avro/ns/TestModel.cs",
                "my/avro/ns/EventType.cs"
            })]
        public void GenerateSchema(string schema, IEnumerable<string> typeNamesToCheck, IEnumerable<string> generatedFilesToCheck)
        {
            AvroGenHelper.TestSchema(schema, typeNamesToCheck, generatedFilesToCheck: generatedFilesToCheck);
        }

        [TestCase(
            _nullableLogicalTypesArray,
            "org.apache.avro.codegentest.testdata", "org.apache.csharp.codegentest.testdata",
            new string[]
            {
                "org.apache.csharp.codegentest.testdata.NullableLogicalTypesArray"
            },
            new string[]
            {
                "org/apache/csharp/codegentest/testdata/NullableLogicalTypesArray.cs"
            })]
        [TestCase(
            _nestedLogicalTypesUnion,
            "org.apache.avro.codegentest.testdata", "org.apache.csharp.codegentest.testdata",
            new string[]
            {
                "org.apache.csharp.codegentest.testdata.NestedLogicalTypesUnion",
                "org.apache.csharp.codegentest.testdata.RecordInUnion"
            },
            new string[]
            {
                "org/apache/csharp/codegentest/testdata/NestedLogicalTypesUnion.cs",
                "org/apache/csharp/codegentest/testdata/RecordInUnion.cs"
            })]
        [TestCase(
            _schema_avro_2883,
            "my.avro.ns", "my.csharp.ns",
            new string[]
            {
                "my.csharp.ns.TestModel",
                "my.csharp.ns.EventType",
            },
            new string[]
            {
                "my/csharp/ns/TestModel.cs",
                "my/csharp/ns/EventType.cs"
            })]
        [TestCase(
            _schema_avro_3046,
            "com.example", "Example",
            new string[]
            {
                "Example.ExampleRecord",
                "Example.InnerRecord",
            },
            new string[]
            {
                "Example/ExampleRecord.cs",
                "Example/InnerRecord.cs"
            })]
        [TestCase(
            _nullableLogicalTypesArray,
            "org.apache.avro.codegentest.testdata", "org.apache.@return.@int", // Reserved keywords in namespace
            new string[]
            {
                "org.apache.return.int.NullableLogicalTypesArray"
            },
            new string[]
            {
                "org/apache/return/int/NullableLogicalTypesArray.cs"
            })]
        [TestCase(
            _nullableLogicalTypesArray,
            "org.apache.avro.codegentest.testdata", "org.apache.value.partial", // Contextual keywords in namespace
            new string[]
            {
                "org.apache.value.partial.NullableLogicalTypesArray"
            },
            new string[]
            {
                "org/apache/value/partial/NullableLogicalTypesArray.cs"
            })]
        [TestCase(@"
{
    ""type"": ""fixed"",
    ""namespace"": ""com.base"",
    ""name"": ""MD5"",
    ""size"": 16
}",
            "com.base", "SchemaTest",
            new string[]
            {
                "SchemaTest.MD5"
            },
            new string[]
            {
                "SchemaTest/MD5.cs"
            })]
        [TestCase(@"
{
    ""type"": ""fixed"",
    ""namespace"": ""com.base"",
    ""name"": ""MD5"",
    ""size"": 16
}",
            "miss", "SchemaTest",
            new string[]
            {
                "com.base.MD5"
            },
            new string[]
            {
                "com/base/MD5.cs"
            })]
        public void GenerateSchemaWithNamespaceMapping(
            string schema,
            string namespaceMappingFrom,
            string namespaceMappingTo,
            IEnumerable<string> typeNamesToCheck,
            IEnumerable<string> generatedFilesToCheck)
        {
            AvroGenHelper.TestSchema(schema, typeNamesToCheck, new Dictionary<string, string> { { namespaceMappingFrom, namespaceMappingTo } }, generatedFilesToCheck);
        }

        [TestCase(_logicalTypesWithCustomConversion, typeof(AvroTypeException))]
        [TestCase(_customConversionWithLogicalTypes, typeof(SchemaParseException))]
        public void NotSupportedSchema(string schema, Type expectedException)
        {
            // Create temp folder
            string outputDir = AvroGenHelper.CreateEmptyTemporyFolder(out string uniqueId);

            try
            {
                // Save schema
                string schemaFileName = Path.Combine(outputDir, $"{uniqueId}.avsc");
                System.IO.File.WriteAllText(schemaFileName, schema);

                Assert.That(AvroGenTool.GenSchema(schemaFileName, outputDir, new Dictionary<string, string>(), false), Is.EqualTo(1));
            }
            finally
            {
                Directory.Delete(outputDir, true);
            }
        }

        [TestCase(@"
{
    ""type"" : ""record"",
    ""name"" : ""ClassKeywords"",
    ""namespace"" : ""com.base"",
    ""fields"" :
        [
            { ""name"" : ""int"", ""type"" : ""int"" },
            { ""name"" : ""base"", ""type"" : ""long"" },
            { ""name"" : ""event"", ""type"" : ""boolean"" },
            { ""name"" : ""foreach"", ""type"" : ""double"" },
            { ""name"" : ""bool"", ""type"" : ""float"" },
            { ""name"" : ""internal"", ""type"" : ""bytes"" },
            { ""name"" : ""while"", ""type"" : ""string"" },
            { ""name"" : ""return"", ""type"" : ""null"" },
            { ""name"" : ""enum"", ""type"" : { ""type"" : ""enum"", ""name"" : ""class"", ""symbols"" : [ ""Unknown"", ""A"", ""B"" ], ""default"" : ""Unknown"" } },
            { ""name"" : ""string"", ""type"" : { ""type"": ""fixed"", ""size"": 16, ""name"": ""static"" } }
        ]
}",
            new object[] { "com.base.ClassKeywords", typeof(int), typeof(long), typeof(bool), typeof(double), typeof(float), typeof(byte[]), typeof(string), typeof(object), "com.base.class", "com.base.static" })]
        [TestCase(@"
{
    ""type"" : ""record"",
    ""name"" : ""AvroNamespaceType"",
    ""namespace"" : ""My.Avro"",
    ""fields"" :
        [
            { ""name"" : ""justenum"", ""type"" : { ""type"" : ""enum"", ""name"" : ""justenumEnum"", ""symbols"" : [ ""One"", ""Two"" ] } },
        ]
}",
            new object[] { "My.Avro.AvroNamespaceType", "My.Avro.justenumEnum" })]
        [TestCase(@"
{
    ""type"" : ""record"",
    ""name"" : ""SchemaObject"",
    ""namespace"" : ""schematest"",
    ""fields"" :
        [
            { ""name"" : ""myobject"", ""type"" :
                [
                    ""null"",
                    { ""type"" : ""array"", ""items"" :
                        [
                            ""null"",
                            { ""type"" : ""enum"", ""name"" : ""MyEnum"", ""symbols"" : [ ""A"", ""B"" ] },
                            { ""type"": ""fixed"", ""size"": 16, ""name"": ""MyFixed"" }
                        ]
                    }
                ]
            }
        ]
}",
            new object[] { "schematest.SchemaObject", typeof(IList<object>) })]
        [TestCase(@"
{
  ""type"" : ""record"",
  ""name"" : ""LogicalTypes"",
  ""namespace"" : ""schematest"",
  ""fields"" :
    [
      { ""name"" : ""nullibleguid"", ""type"" : [""null"", {""type"": ""string"", ""logicalType"": ""uuid"" } ]},
      { ""name"" : ""guid"", ""type"" : {""type"": ""string"", ""logicalType"": ""uuid"" } },
      { ""name"" : ""nullibletimestampmillis"", ""type"" : [""null"", {""type"": ""long"", ""logicalType"": ""timestamp-millis""}]  },
      { ""name"" : ""timestampmillis"", ""type"" : {""type"": ""long"", ""logicalType"": ""timestamp-millis""} },
      { ""name"" : ""nullibiletimestampmicros"", ""type"" : [""null"", {""type"": ""long"", ""logicalType"": ""timestamp-micros""}]  },
      { ""name"" : ""timestampmicros"", ""type"" : {""type"": ""long"", ""logicalType"": ""timestamp-micros""} },
      { ""name"" : ""nullibiletimemicros"", ""type"" : [""null"", {""type"": ""long"", ""logicalType"": ""time-micros""}]  },
      { ""name"" : ""timemicros"", ""type"" : {""type"": ""long"", ""logicalType"": ""time-micros""} },
      { ""name"" : ""nullibiletimemillis"", ""type"" : [""null"", {""type"": ""int"", ""logicalType"": ""time-millis""}]  },
      { ""name"" : ""timemillis"", ""type"" : {""type"": ""int"", ""logicalType"": ""time-millis""} },
      { ""name"" : ""nullibledecimal"", ""type"" : [""null"", {""type"": ""bytes"", ""logicalType"": ""decimal"", ""precision"": 4, ""scale"": 2}]  },
      { ""name"" : ""decimal"", ""type"" : {""type"": ""bytes"", ""logicalType"": ""decimal"", ""precision"": 4, ""scale"": 2} },
      { ""name"" : ""nullibledecimalfixed"", ""type"" : [""null"", {""type"": {""type"" : ""fixed"", ""size"": 16, ""name"": ""ndf""}, ""logicalType"": ""decimal"", ""precision"": 4, ""scale"": 2}]  },
      { ""name"" : ""decimalfixed"", ""type"" : {""type"": {""type"" : ""fixed"", ""size"": 16, ""name"": ""df""}, ""logicalType"": ""decimal"", ""precision"": 4, ""scale"": 2} }
    ]
}",
            new object[] { "schematest.LogicalTypes", typeof(Guid?), typeof(Guid), typeof(DateTime?), typeof(DateTime), typeof(DateTime?), typeof(DateTime), typeof(TimeSpan?), typeof(TimeSpan), typeof(TimeSpan?), typeof(TimeSpan), typeof(AvroDecimal?), typeof(AvroDecimal), typeof(AvroDecimal?), typeof(AvroDecimal) })]
        [TestCase(@"
{
  ""namespace"": ""enum.base"",
  ""type"": ""record"",
  ""name"": ""EnumInDifferentNamespace"",
  ""doc"": ""Test enum with a default value in a different namespace"",
  ""fields"": [
    {
      ""name"": ""anEnum"",
      ""type"": {
        ""namespace"": ""enum.base.other"",
        ""type"": ""enum"",
        ""name"": ""AnEnum"",
        ""symbols"": [
          ""A"",
          ""B""
        ],
        ""default"": ""A""
      }
    }
  ]
}",
            new object[] { "enum.base.EnumInDifferentNamespace", "enum.base.other.AnEnum" })]
        public void GenerateSchemaCheckFields(string schema, object[] result)
        {
            Assembly assembly = AvroGenHelper.TestSchema(schema);

            // Instantiate object
            Type type = assembly.GetType((string)result[0]);
            Assert.That(type, Is.Not.Null);

            ISpecificRecord record = Activator.CreateInstance(type) as ISpecificRecord;
            Assert.IsNotNull(record);

            // test type of each fields
            for (int i = 1; i < result.Length; ++i)
            {
                object field = record.Get(i - 1);
                Type stype;
                if (result[i].GetType() == typeof(string))
                {
                    Type t = assembly.GetType((string)result[i]);
                    Assert.That(record, Is.Not.Null);

                    object obj = Activator.CreateInstance(t);
                    Assert.That(obj, Is.Not.Null);
                    stype = obj.GetType();
                }
                else
                {
                    stype = (Type)result[i];
                }
                if (!stype.IsValueType)
                {
                    Assert.That(field, Is.Null);   // can't test reference type, it will be null
                }
                else if (stype.IsValueType && field == null)
                {
                    Assert.That(field, Is.Null); // nullable value type, so we can't get the type using GetType
                }
                else
                {
                    Assert.That(field, Is.Not.Null);
                    Assert.That(field.GetType(), Is.EqualTo(stype));
                }
            }
        }
    }
}
