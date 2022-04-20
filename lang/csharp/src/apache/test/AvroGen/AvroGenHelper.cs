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
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Emit;
using NUnit.Framework;
using Avro.Specific;

namespace Avro.Test.AvroGen
{
    class AvroGenToolResult
    {
        public int ExitCode { get; set; }
        public string[] StdOut { get; set; }
        public string[] StdErr { get; set; }
    }

    class AvroGenHelper
    {
        public static AvroGenToolResult RunAvroGenTool(params string[] args)
        {
            // Save stdout and stderr
            TextWriter conOut = Console.Out;
            TextWriter conErr = Console.Error;

            try
            {
                AvroGenToolResult result = new AvroGenToolResult();
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

                    result.ExitCode = AvroGenTool.Main(args.ToArray());

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

        public static Assembly CompileCSharpFilesIntoLibrary(IEnumerable<string> sourceFiles, string assemblyName = null, bool loadAssembly = true)
        {
            // Create random assembly name if not specified
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
                    typeof(System.CodeDom.Compiler.GeneratedCodeAttribute).Assembly.Location,
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

#if DEBUG
                if (!compilationResult.Success)
                {
                    foreach (Diagnostic diagnostic in compilationResult.Diagnostics)
                    {
                        if (diagnostic.IsWarningAsError || diagnostic.Severity == DiagnosticSeverity.Error)
                        {
                            TestContext.WriteLine($"{diagnostic.Id} - {diagnostic.GetMessage()} - {diagnostic.Location}");
                        }
                    }
                }
#endif

                Assert.That(compilationResult.Success, Is.True);

                if (!loadAssembly)
                {
                    return null;
                }

                // Load assembly from stream
                compilerStream.Seek(0, SeekOrigin.Begin);
                return Assembly.Load(compilerStream.ToArray());
            }
        }

        public static string CreateEmptyTemporyFolder(out string uniqueId, string path = null)
        {
            // Create unique id
            uniqueId = Guid.NewGuid().ToString();

            // Temporary folder name in working folder or the specified path
            string tempFolder = Path.Combine(path ?? TestContext.CurrentContext.WorkDirectory, uniqueId);

            // Create folder
            Directory.CreateDirectory(tempFolder);

            // Make sure it is empty
            Assert.That(new DirectoryInfo(tempFolder), Is.Empty);

            return tempFolder;
        }

        public static Assembly CompileCSharpFilesAndCheckTypes(
            string outputDir,
            string assemblyName,
            IEnumerable<string> typeNamesToCheck = null,
            IEnumerable<string> generatedFilesToCheck = null)
        {
            // Check if all generated files exist
            if (generatedFilesToCheck != null)
            {
                foreach (string generatedFile in generatedFilesToCheck)
                {
                    Assert.That(new FileInfo(Path.Combine(outputDir, generatedFile)), Does.Exist);
                }
            }

            // Compile into netstandard library and load assembly
            Assembly assembly = CompileCSharpFilesIntoLibrary(
                new DirectoryInfo(outputDir)
                    .EnumerateFiles("*.cs", SearchOption.AllDirectories)
                    .Select(fi => fi.FullName),
                    assemblyName);

            if (typeNamesToCheck != null)
            {
                // Check if the compiled code has the same number of types defined as the check list
                Assert.That(typeNamesToCheck.Count(), Is.EqualTo(assembly.DefinedTypes.Count()));

                // Check if types available in compiled assembly
                foreach (string typeName in typeNamesToCheck)
                {
                    Type type = assembly.GetType(typeName);
                    Assert.That(type, Is.Not.Null);

                    // Protocols are abstract and cannot be instantiated
                    if (typeof(ISpecificProtocol).IsAssignableFrom(type))
                    {
                        Assert.That(type.IsAbstract, Is.True);

                        // If directly inherited from ISpecificProtocol, use reflection to read static private field
                        // holding the protocol. Callback objects are not directly inherited from ISpecificProtocol,
                        // so private fields in the base class cannot be accessed
                        if (type.BaseType.Equals(typeof(ISpecificProtocol)))
                        {
                            // Use reflection to read static field, holding the protocol
                            FieldInfo protocolField = type.GetField("protocol", BindingFlags.NonPublic | BindingFlags.Static);
                            Protocol protocol = protocolField.GetValue(null) as Protocol;

                            Assert.That(protocol, Is.Not.Null);
                        }
                    }
                    else
                    {
                        Assert.That(type.IsClass || type.IsEnum, Is.True);

                        // Instantiate object
                        object obj = Activator.CreateInstance(type);
                        Assert.That(obj, Is.Not.Null);

                        // If ISpecificRecord, call its member for sanity check
                        if (obj is ISpecificRecord record)
                        {
                            // Read record's schema object
                            Assert.That(record.Schema, Is.Not.Null);
                            // Force exception by reading/writing invalid field
                            Assert.Throws<AvroRuntimeException>(() => record.Get(-1));
                            Assert.Throws<AvroRuntimeException>(() => record.Put(-1, null));
                        }
                    }
                }
            }

            return assembly;
        }

        public static Assembly TestSchema(
            string schema,
            IEnumerable<string> typeNamesToCheck = null,
            IEnumerable<KeyValuePair<string, string>> namespaceMapping = null,
            IEnumerable<string> generatedFilesToCheck = null)
        {
            // Create temp folder
            string outputDir = CreateEmptyTemporyFolder(out string uniqueId);

            try
            {
                // Save schema
                string schemaFileName = Path.Combine(outputDir, $"{uniqueId}.avsc");
                System.IO.File.WriteAllText(schemaFileName, schema);

                // Generate from schema file
                Assert.That(AvroGenTool.GenSchema(schemaFileName, outputDir, namespaceMapping ?? new Dictionary<string, string>(), false), Is.EqualTo(0));

                return CompileCSharpFilesAndCheckTypes(outputDir, uniqueId, typeNamesToCheck, generatedFilesToCheck);
            }
            finally
            {
                Directory.Delete(outputDir, true);
            }
        }

        public static Assembly TestProtocol(
            string protocol,
            IEnumerable<string> typeNamesToCheck = null,
            IEnumerable<KeyValuePair<string, string>> namespaceMapping = null,
            IEnumerable<string> generatedFilesToCheck = null)
        {
            // Create temp folder
            string outputDir = CreateEmptyTemporyFolder(out string uniqueId);

            try
            {
                // Save protocol
                string schemaFileName = Path.Combine(outputDir, $"{uniqueId}.avpr");
                System.IO.File.WriteAllText(schemaFileName, protocol);

                // Generate from protocol file
                Assert.That(AvroGenTool.GenProtocol(schemaFileName, outputDir, namespaceMapping ?? new Dictionary<string, string>()), Is.EqualTo(0));

                return CompileCSharpFilesAndCheckTypes(outputDir, uniqueId, typeNamesToCheck, generatedFilesToCheck);
            }
            finally
            {
                Directory.Delete(outputDir, true);
            }
        }
    }
}
