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
using System.Text;
using System.Collections.Generic;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Emit;
using NUnit.Framework;
using Avro.Specific;

namespace Avro.Test.AvroGen
{
    [TestFixture]

    class AvroGenTest
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
        ""type"": ""fixed"",
        ""size"": 12,
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

        enum GenerateType
        {
            Schema,
            Protocol
        };

        class ExecuteResult
        {
            public int ExitCode { get; set; }
            public string[] StdOut { get; set; }
            public string[] StdErr { get; set; }
        }

        private ExecuteResult RunAvroGen(IEnumerable<string> args)
        {
            // Save stdout and stderr
            TextWriter conOut = Console.Out;
            TextWriter conErr = Console.Error;

            try
            {
                ExecuteResult result = new ExecuteResult();
                StringBuilder strBuilderOut = new StringBuilder();
                StringBuilder strBuilderErr = new StringBuilder();

                using (StringWriter writerOut = new StringWriter(strBuilderOut))
                using (StringWriter writerErr = new StringWriter(strBuilderErr))
                {
                    writerOut.NewLine = "\n";
                    writerErr.NewLine = "\n";

                    // Overwrite stdout and stderr to be able to capture console output
                    Console.SetOut(writerOut);
                    Console.SetError(writerErr);

                    result.ExitCode = Avro.AvroGen.Main(args.ToArray());

                    writerOut.Flush();
                    writerErr.Flush();

                    result.StdOut = strBuilderOut.Length == 0 ? Array.Empty<string>() : strBuilderOut.ToString().Split(writerOut.NewLine);
                    result.StdErr = strBuilderErr.Length == 0 ? Array.Empty<string>() : strBuilderErr.ToString().Split(writerErr.NewLine);
                }

                return result;
            }
            finally
            {
                // Restore console
                Console.SetOut(conOut);
                Console.SetError(conErr);
            }
        }

        private void CompileAvroSchemaFiles(IEnumerable<string> schemaFileNames, string outputDir, GenerateType genType = GenerateType.Schema, IEnumerable<KeyValuePair<string, string>> namespaceMapping = null)
        {
            // Run avrogen on scema files
            foreach (string schemaFile in schemaFileNames)
            {
                List<string> avroGenArgs = new List<string>()
                {
                    "-s",
                    schemaFile,
                    outputDir
                };

                if (namespaceMapping != null)
                {
                    foreach (KeyValuePair<string, string> kv in namespaceMapping)
                    {
                        avroGenArgs.Add("--namespace");
                        avroGenArgs.Add($"{kv.Key}:{kv.Value}");
                    }
                }

                ExecuteResult result = RunAvroGen(avroGenArgs);

                // Verify result
                Assert.AreEqual(0, result.ExitCode);
                Assert.AreEqual(0, result.StdOut.Length);
                Assert.AreEqual(0, result.StdErr.Length);
            }
        }

        private Assembly CompileCharpFilesIntoLibrary(IEnumerable<string> sourceFiles, string assemblyName = null, bool loadAssembly = true)
        {
            // CReate random assenbly name if not specified
            if (assemblyName == null)
                assemblyName = Path.GetRandomFileName();

            // Base path to assemblies .NET assemblies
            var assemblyPath = Path.GetDirectoryName(typeof(object).Assembly.Location);

            using (var compilerStream = new MemoryStream())
            {
                List<string> assemblies = new List<string>()
                {
                    typeof(object).Assembly.Location,
                    typeof(Schema).Assembly.Location,
                    Path.Combine(assemblyPath, "System.Runtime.dll"),
                    Path.Combine(assemblyPath, "netstandard.dll")
                };

                // Create compiler
                CSharpCompilation compilation = CSharpCompilation
                    .Create(assemblyName)
                    .WithOptions(new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary))
                    .AddReferences(assemblies.Select(path => MetadataReference.CreateFromFile(path)))
                    .AddSyntaxTrees(sourceFiles.Select(sourceFile =>
                    {
                        string sourceText = System.IO.File.ReadAllText(sourceFile);
                        return CSharpSyntaxTree.ParseText(sourceText);
                    }));

                // Compile
                EmitResult compilationResult = compilation.Emit(compilerStream);
                if (!compilationResult.Success)
                {
                    foreach (Diagnostic diagnostic in compilationResult.Diagnostics)
                    {
                        if (diagnostic.IsWarningAsError || diagnostic.Severity == DiagnosticSeverity.Error)
                        {
                            TestContext.WriteLine($"{diagnostic.Id} - {diagnostic.GetMessage()} - {diagnostic.Location}");
                        }
                    }
                    Assert.IsTrue(compilationResult.Success, "Compilation failed");
                }

                if (!loadAssembly)
                {
                    return null;
                }

                compilerStream.Seek(0, SeekOrigin.Begin);
                return Assembly.Load(compilerStream.ToArray());
            }
        }

        [Test]
        public void NoArgs()
        {
            ExecuteResult result = RunAvroGen(Array.Empty<string>());
            Assert.AreEqual(1, result.ExitCode);
            Assert.AreNotEqual(0, result.StdOut.Length);
            Assert.AreEqual(0, result.StdErr.Length);
        }

        [TestCase("-h")]
        [TestCase("--help")]
        [TestCase("--help", "-h")]
        [TestCase("--help", "-s", "whatever.avsc", ".")]
        [TestCase("-p", "whatever.avsc", ".", "-h")]
        public void Help(params string[] args)
        {
            ExecuteResult result = RunAvroGen(args);

            Assert.AreEqual(0, result.ExitCode);
            Assert.AreNotEqual(0, result.StdOut.Length);
            Assert.AreEqual(0, result.StdErr.Length);
        }

        [TestCase("-p")]
        [TestCase("-s")]
        [TestCase("-p", "whatever.avsc")]
        [TestCase("-s", "whatever.avsc")]
        [TestCase(".")]
        public void MissingArgs(params string[] args)
        {
            ExecuteResult result = RunAvroGen(args);
            Assert.AreEqual(1, result.ExitCode);
            Assert.AreNotEqual(0, result.StdOut.Length);
            Assert.AreNotEqual(0, result.StdErr.Length);
        }

        private Assembly TestSchema(string schema, IEnumerable<string> typeNamesToCheck = null, IEnumerable<KeyValuePair<string, string>> namespaceMapping = null)
        {
            string compiledAssemblyName = Guid.NewGuid().ToString();
            string outputDir = Path.Combine(TestContext.CurrentContext.WorkDirectory, compiledAssemblyName);

            // Create temp folder
            Directory.CreateDirectory(outputDir);

            try
            {
                // Save schema into file
                string schemaFileName = Path.Combine(outputDir, Path.GetTempFileName());
                System.IO.File.WriteAllText(schemaFileName, schema);

                // Compile avro
                CompileAvroSchemaFiles(new List<string>() { schemaFileName }, outputDir, GenerateType.Schema, namespaceMapping);

                // Compile into netstandard library and load assembly
                Assembly assembly = CompileCharpFilesIntoLibrary(
                    new DirectoryInfo(outputDir)
                        .EnumerateFiles("*.cs", SearchOption.AllDirectories)
                        .Select(fi => fi.FullName),
                        compiledAssemblyName);

                if (typeNamesToCheck != null)
                {
                    // Check if the compiled code has exactly the same types defined as the check list
                    Assert.AreEqual(typeNamesToCheck.Count(), assembly.DefinedTypes.Count());

                    // Check if types available in compiled assembly
                    foreach (string typeName in typeNamesToCheck)
                    {
                        Type type = assembly.GetType(typeName);
                        Assert.IsNotNull(type);

                        // Instantiate
                        object obj = Activator.CreateInstance(type);
                    }
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
            })]
        [TestCase(
            _nestedLogicalTypesArray,
            new string[]
            {
                "org.apache.avro.codegentest.testdata.NestedLogicalTypesArray",
                "org.apache.avro.codegentest.testdata.RecordInArray"
            })]
        [TestCase(
            _nestedLogicalTypesMap,
            new string[]
            {
                "org.apache.avro.codegentest.testdata.NestedLogicalTypesMap",
                "org.apache.avro.codegentest.testdata.RecordInMap"
            })]
        [TestCase(
            _nestedLogicalTypesRecord,
            new string[]
            {
                "org.apache.avro.codegentest.testdata.NestedLogicalTypesRecord",
                "org.apache.avro.codegentest.testdata.NestedRecord"
            })]
        [TestCase(
            _nestedLogicalTypesUnion,
            new string[]
            {
                "org.apache.avro.codegentest.testdata.NestedLogicalTypesUnion",
                "org.apache.avro.codegentest.testdata.RecordInUnion"
            })]
        [TestCase(
            _nestedSomeNamespaceRecord,
            new string[]
            {
                "org.apache.avro.codegentest.some.NestedSomeNamespaceRecord",
                "org.apache.avro.codegentest.other.NestedOtherNamespaceRecord"
            })]
        [TestCase(
            _nullableLogicalTypes,
            new string[]
            {
               "org.apache.avro.codegentest.testdata.NullableLogicalTypes"
            })]
        [TestCase(
            _nullableLogicalTypesArray,
            new string[]
            {
                "org.apache.avro.codegentest.testdata.NullableLogicalTypesArray"
            })]
        public void GenerateSchema(string schema, IEnumerable<string> typeNamesToCheck)
        {
            TestSchema(schema, typeNamesToCheck);
        }

        [TestCase(
            _nestedLogicalTypesArray,
            "org.apache.avro", "my.csharp",
            new string[]
            {
                "my.csharp.codegentest.testdata.NestedLogicalTypesArray",
                "my.csharp.codegentest.testdata.RecordInArray"
            })]
        [TestCase(
            _nestedLogicalTypesArray,
            "org", "my",
            new string[]
            {
                "my.apache.avro.codegentest.testdata.NestedLogicalTypesArray",
                "my.apache.avro.codegentest.testdata.RecordInArray"
            })]
        [TestCase(
            _nullableLogicalTypesArray,
            "org.apache.avro.codegentest.testdata", "org.apache.csharp.codegentest.testdata",
            new string[]
            {
                "org.apache.csharp.codegentest.testdata.NullableLogicalTypesArray"
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
            })]
        public void GenerateSchemaWithNamespaceMapping(string schema, string namespaceMappingFrom, string namespaceMappingTo, IEnumerable<string> typeNamesToCheck)
        {
            TestSchema(schema, typeNamesToCheck, new Dictionary<string, string> { { namespaceMappingFrom, namespaceMappingTo } });
        }

        [TestCase(
            _nestedLogicalTypesArray,
            "org.apache.avro", "my.@class.@switch.@event",
            new string[]
            {
                "my.class.switch.event.codegentest.testdata.NestedLogicalTypesArray",
                "my.class.switch.event.codegentest.testdata.RecordInArray"
            })]
        [TestCase(
            _nestedLogicalTypesArray,
            "org", "my",
            new string[]
            {
                "my.apache.avro.codegentest.testdata.NestedLogicalTypesArray",
                "my.apache.avro.codegentest.testdata.RecordInArray"
            })]
        [TestCase(
            _nullableLogicalTypesArray,
            "org.apache.avro.codegentest.testdata", "org.apache.@return.@int",
            new string[]
            {
                "org.apache.return.int.NullableLogicalTypesArray"
            })]
        public void GenerateSchemaWithReservedNamespaceMapping(string schema, string namespaceMappingFrom, string namespaceMappingTo, IEnumerable<string> typeNamesToCheck)
        {
            TestSchema(schema, typeNamesToCheck, new Dictionary<string, string> { { namespaceMappingFrom, namespaceMappingTo } });
        }

        [TestCase(_logicalTypesWithCustomConversion)]
        [TestCase(_customConversionWithLogicalTypes)]
        [TestCase(_nestedLogicalTypesUnionFixedDecimal)]
        public void NotSupportedSchema(string schema)
        {
            string outputDir = Path.Combine(TestContext.CurrentContext.WorkDirectory, Guid.NewGuid().ToString());

            // Create temp folder
            Directory.CreateDirectory(outputDir);

            try
            {
                // Save schema into file
                string schemaFileName = Path.Combine(outputDir, Path.GetTempFileName());
                System.IO.File.WriteAllText(schemaFileName, schema);

                ExecuteResult result = RunAvroGen(new string[] { "-s", schemaFileName, outputDir });

                // Verify result
                Assert.AreEqual(1, result.ExitCode);
                Assert.AreEqual(0, result.StdOut.Length);
                Assert.AreNotEqual(0, result.StdErr.Length);
                Assert.True(result.StdErr[0].StartsWith("Exception occurred."));
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
            { ""name"" : ""decimal"", ""type"" : {""type"": ""bytes"", ""logicalType"": ""decimal"", ""precision"": 4, ""scale"": 2} }
		]
}",
            new object[] { "schematest.LogicalTypes", typeof(Guid?), typeof(Guid), typeof(DateTime?), typeof(DateTime), typeof(DateTime?), typeof(DateTime), typeof(TimeSpan?), typeof(TimeSpan), typeof(TimeSpan?), typeof(TimeSpan), typeof(AvroDecimal?), typeof(AvroDecimal) })]
        public void GenerateSchemaCheckFields(string schema, object[] result)
        {
            Assembly assembly = TestSchema(schema);

            // Instantiate object
            Type type = assembly.GetType((string)result[0]);
            Assert.IsNotNull(type);

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
                    Assert.IsNotNull(record);

                    object obj = Activator.CreateInstance(t);
                    Assert.IsNotNull(obj);
                    stype = obj.GetType();
                }
                else
                    stype = (Type)result[i];
                if (!stype.IsValueType)
                    Assert.IsNull(field);   // can't test reference type, it will be null
                else if (stype.IsValueType && field == null)
                    Assert.IsNull(field); // nullable value type, so we can't get the type using GetType
                else
                    Assert.AreEqual(stype, field.GetType());
            }
        }
    }
}
