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
using NUnit.Framework;
using System.Text;
using System.Collections.Generic;
using System.Diagnostics;

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

        private ExecuteResult ExecuteCommand(string cmd, string args, string workingDirectory)
        {
            ExecuteResult result = new ExecuteResult();
            List<string> stdOut = new List<string>();
            List<string> stdErr = new List<string>();

            Process process = Process.Start(new ProcessStartInfo(cmd, args)
            {
                WorkingDirectory = workingDirectory,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
                UseShellExecute = false
            });

            process.OutputDataReceived += (sender, e) =>
            {
                if (e.Data != null)
                {
                    stdOut.Add(e.Data);
                }
            };
            process.ErrorDataReceived += (sender, e) =>
            {
                if (e.Data != null)
                {
                    stdErr.Add(e.Data);
                }
            };

            process.BeginOutputReadLine();
            process.BeginErrorReadLine();

            process.WaitForExit();

            result.ExitCode = process.ExitCode;
            result.StdOut = stdOut.ToArray();
            result.StdErr = stdErr.ToArray();

            process.Close();

            return result;
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


        [TestCase(null, null, TestName = "GenerateSchemas")] // No namespace mapping
        [TestCase("org.apache.avro.codegentest", "my.csharp.codegentest", TestName = "GenerateSchemasWithMapping")] // Map namespace
        [TestCase("org.apache.avro.codegentest", "my.csharp.event.return.int.codegentest", TestName = "GenerateSchemasWithReservedMapping")] // Map namespace to name with reserved words
        public void GenerateSchemas(string namespaceMappingFrom, string namespaceMappingTo)
        {
            List<string> schemFiles = new List<string>()
            {
                "resources/avro/nested_logical_types_array.avsc",
                "resources/avro/nested_logical_types_map.avsc",
                "resources/avro/nested_logical_types_record.avsc",
                "resources/avro/nested_logical_types_union.avsc",
                "resources/avro/nested_records_different_namespace.avsc",
                "resources/avro/nullable_logical_types.avsc",
                "resources/avro/nullable_logical_types_array.avsc",
                "resources/avro/string_logical_type.avsc"
            };

            string outDir = $"./generated_{TestContext.CurrentContext.Test.Name}";
            ExecuteResult result;

            // Make sure directory exists
            Directory.CreateDirectory(outDir);

            // Run avrogen on scema files
            foreach (string schemaFile in schemFiles)
            {
                List<string> avroGenArgs = new List<string>()
                {
                    "-s",
                    schemaFile,
                    outDir
                };

                if (namespaceMappingFrom != null)
                {
                    avroGenArgs.Add("--namespace");
                    avroGenArgs.Add($"{namespaceMappingFrom}:{namespaceMappingTo}");
                }

                result = RunAvroGen(avroGenArgs);

                // Verify result
                Assert.AreEqual(0, result.ExitCode);
                Assert.AreEqual(0, result.StdOut.Length);
                Assert.AreEqual(0, result.StdErr.Length);
            }

            // Determine the current framework
#if NETCOREAPP3_1
            string framework = "netcoreapp3.1";
#elif NET5_0
            string framework = "net5.0";
#elif NET6_0
            string framework = "net6.0";
#endif

            // Create new console project
            result = ExecuteCommand("dotnet", $"new console --force -f {framework}", outDir);
            Assert.AreEqual(0, result.ExitCode);

            // Add project reference to Avro library
            result = ExecuteCommand("dotnet", "add reference ../../../../../main/Avro.main.csproj", outDir);
            Assert.AreEqual(0, result.ExitCode);

            // Build project
            result = ExecuteCommand("dotnet", "build", outDir);
            Assert.AreEqual(0, result.ExitCode);

            // Run project
            result = ExecuteCommand("dotnet", "run --no-build", outDir);
            Assert.AreEqual(0, result.ExitCode);
        }
    }
}
