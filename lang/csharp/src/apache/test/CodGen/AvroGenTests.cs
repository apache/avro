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

        private void CompileAvroFiles(IEnumerable<string> avroFiles, string outputDir, GenerateType genType = GenerateType.Schema, IEnumerable<KeyValuePair<string, string>> namespaceMapping = null)
        {
            // Make sure directory exists
            Directory.CreateDirectory(outputDir);

            // Run avrogen on scema files
            foreach (string schemaFile in avroFiles)
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

        private Assembly CompileCharpFilesIntoLibrary(IEnumerable<string> sourceFiles, string assemblyName, bool loadAssembly = true)
        {
            // Base path to assemblies .NET assemblies
            var assemblyPath = Path.GetDirectoryName(typeof(object).Assembly.Location);

            using (var compilerStream = new MemoryStream())
            {
                // Create compiler
                CSharpCompilation compilation = CSharpCompilation
                    .Create(assemblyName)
                    .WithOptions(new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary))
                    .AddReferences(
                        MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
                        MetadataReference.CreateFromFile(typeof(Schema).Assembly.Location),
                        MetadataReference.CreateFromFile(Path.Combine(assemblyPath, "System.Runtime.dll")),
                        MetadataReference.CreateFromFile(Path.Combine(assemblyPath, "netstandard.dll"))
                    )
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

                // Load assembly
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

        private void GenerateSchemaAssembly(string assemblyName, string schemaFileName, IEnumerable<string> typeNamesToCheck, IEnumerable<KeyValuePair<string, string>> namespaceMapping = null)
        {
            string outputDir = Path.Combine($"generated", assemblyName);

            // Make sure directory exists
            Directory.CreateDirectory(outputDir);

            // Compile avro
            CompileAvroFiles(new List<string>() { schemaFileName }, outputDir, GenerateType.Schema, namespaceMapping);

            // Compile into netstandard library and load assembly
            Assembly assembly = CompileCharpFilesIntoLibrary(
                new DirectoryInfo(outputDir)
                    .EnumerateFiles("*.cs", SearchOption.AllDirectories)
                    .Select(fi => fi.FullName),
                assemblyName);

            // Check if types available in compiled assembly
            foreach (string typeName in typeNamesToCheck)
            {
                Type type = assembly.GetType(typeName);

                Assert.IsNotNull(type);

                // Instantiate and sanity check if member can be called
                ISpecificRecord obj = (ISpecificRecord)Activator.CreateInstance(type);
                Assert.IsNotNull(obj.Schema);
            }
        }

        private static IEnumerable<TestCaseData> GenerateSchemaSource()
        {
            // No namespace mapping
            yield return new TestCaseData(
                "resources/avro/nested_logical_types_array.avsc",
                new string[]
                {
                    "org.apache.avro.codegentest.testdata.NestedLogicalTypesArray",
                    "org.apache.avro.codegentest.testdata.RecordInArray"
                }).SetName("{m}(nested_logical_types_array)");
            yield return new TestCaseData(
                "resources/avro/nested_logical_types_map.avsc",
                new string[]
                {
                    "org.apache.avro.codegentest.testdata.NestedLogicalTypesMap",
                    "org.apache.avro.codegentest.testdata.RecordInMap"
                }).SetName("{m}(nested_logical_types_map)");
            yield return new TestCaseData(
                "resources/avro/nested_logical_types_record.avsc",
                new string[]
                {
                    "org.apache.avro.codegentest.testdata.NestedLogicalTypesRecord",
                    "org.apache.avro.codegentest.testdata.NestedRecord"
                }).SetName("{m}(nested_logical_types_record)");
            yield return new TestCaseData(
                "resources/avro/nested_logical_types_union.avsc",
                new string[]
                {
                    "org.apache.avro.codegentest.testdata.NestedLogicalTypesUnion",
                    "org.apache.avro.codegentest.testdata.RecordInUnion"
                }).SetName("{m}(nested_logical_types_union)");
            yield return new TestCaseData(
                "resources/avro/nested_records_different_namespace.avsc",
                new string[]
                {
                    "org.apache.avro.codegentest.some.NestedSomeNamespaceRecord",
                    "org.apache.avro.codegentest.other.NestedOtherNamespaceRecord"
                }).SetName("{m}(nested_records_different_namespace)");
            yield return new TestCaseData(
                "resources/avro/nullable_logical_types.avsc",
                new string[]
                {
                    "org.apache.avro.codegentest.testdata.NullableLogicalTypes"
                }).SetName("{m}(nullable_logical_types)");
            yield return new TestCaseData(
                "resources/avro/nullable_logical_types_array.avsc",
                new string[]
                {
                    "org.apache.avro.codegentest.testdata.NullableLogicalTypesArray"
                }).SetName("{m}(nullable_logical_types_array)");
        }

        [TestCaseSource(nameof(GenerateSchemaSource))]
        public void GenerateSchema(string schemaFileName, IEnumerable<string> typeNamesToCheck)
        {
            GenerateSchemaAssembly($"GenerateSchema_{Path.GetFileNameWithoutExtension(schemaFileName)}", schemaFileName, typeNamesToCheck);
        }

        private static IEnumerable<TestCaseData> GenerateSchemaWithNamespaceMappingSource()
        {
            // Namespace mapping
            yield return new TestCaseData(
                "resources/avro/nested_logical_types_array.avsc",
                new Dictionary<string, string>
                {
                    { "org.apache.avro", "my.csharp"}
                },
                new string[]
                {
                    "my.csharp.codegentest.testdata.NestedLogicalTypesArray",
                    "my.csharp.codegentest.testdata.RecordInArray"
                }).SetName("{m}(nested_logical_types_array)");

            // Multiple namespace mapping
            yield return new TestCaseData(
                "resources/avro/nested_records_different_namespace.avsc",
                new Dictionary<string, string>
                {
                    { "org.apache.avro.codegentest.some", "my.csharp.some"},
                    { "org.apache.avro.codegentest.other", "my.csharp.other"}
                },
                new string[]
                {
                    "my.csharp.some.NestedSomeNamespaceRecord",
                    "my.csharp.other.NestedOtherNamespaceRecord"
                }).SetName("{m}(nested_records_different_namespace)");
        }

        [TestCaseSource(nameof(GenerateSchemaWithNamespaceMappingSource))]
        public void GenerateSchemaWithNamespaceMapping(string schemaFileName, IEnumerable<KeyValuePair<string, string>> namespaceMapping, IEnumerable<string> typeNamesToCheck)
        {
            GenerateSchemaAssembly(
                $"GenerateSchemaNamespaceMapping_{Path.GetFileNameWithoutExtension(schemaFileName)}",
                schemaFileName,
                typeNamesToCheck,
                namespaceMapping);
        }
    }
}
