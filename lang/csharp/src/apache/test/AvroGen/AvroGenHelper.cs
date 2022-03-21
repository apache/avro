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
                    Path.Combine(assemblyPath, "System.Runtime.dll"),
                    Path.Combine(assemblyPath, "netstandard.dll")
                };

#if NETCOREAPP3_1
                assemblies.Add(typeof(System.CodeDom.Compiler.GeneratedCodeAttribute).Assembly.Location);
#endif

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

                //Note: Comment the following out to analyze the compiler errors if needed
                //if (!compilationResult.Success)
                //{
                //    foreach (Diagnostic diagnostic in compilationResult.Diagnostics)
                //    {
                //        if (diagnostic.IsWarningAsError || diagnostic.Severity == DiagnosticSeverity.Error)
                //        {
                //            TestContext.WriteLine($"{diagnostic.Id} - {diagnostic.GetMessage()} - {diagnostic.Location}");
                //        }
                //    }
                //}

                Assert.That(compilationResult.Success, Is.True);

                if (!loadAssembly)
                {
                    return null;
                }

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
    }
}
