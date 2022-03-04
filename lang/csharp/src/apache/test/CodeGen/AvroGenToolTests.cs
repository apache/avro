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
using System.Text;
using System.Collections.Generic;
using NUnit.Framework;

namespace Avro.Test.CodeGen
{
    [TestFixture]

    class AvroGenToolTests
    {
        class AvroGenToolResult
        {
            public int ExitCode { get; set; }
            public string[] StdOut { get; set; }
            public string[] StdErr { get; set; }
        }

        private AvroGenToolResult RunAvroGenTool(params string[] args)
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

        // The actual functionaluty of AvroGen is tested in `AvroGenTests.cs`
        // This tests just validates that the proper files are generated,
        // however it does not test the actual compilation of the generated source files
        private void TestSchema(string schema, IEnumerable<KeyValuePair<string, string>> namespaceMapping = null, IEnumerable<string> generatedFilesToCheck = null)
        {
            string uniqueId = Guid.NewGuid().ToString();
            string outputDir = Path.Combine(TestContext.CurrentContext.WorkDirectory, uniqueId);

            // Create temp folder
            Directory.CreateDirectory(outputDir);

            // Make sure to start with an empty working folder
            Assert.That(new DirectoryInfo(outputDir), Is.Empty);

            try
            {
                // Save schema into file
                string schemaFileName = Path.Combine(outputDir, $"{uniqueId}.avsc");
                System.IO.File.WriteAllText(schemaFileName, schema);

                // Generate avrogen tool arguments
                List<string> avroGenToolArgs = new List<string>()
                {
                    "-s",
                    schemaFileName,
                    outputDir
                };
                if (namespaceMapping != null)
                {
                    foreach (KeyValuePair<string, string> kv in namespaceMapping)
                    {
                        avroGenToolArgs.Add("--namespace");
                        avroGenToolArgs.Add($"{kv.Key}:{kv.Value}");
                    }
                }

                // Run avrogen tool
                AvroGenToolResult result = RunAvroGenTool(avroGenToolArgs.ToArray());

                // Check avrogen result
                Assert.That(result.ExitCode, Is.EqualTo(0));
                Assert.That(result.StdOut, Is.Empty);
                Assert.That(result.StdErr, Is.Empty);

                // Check if all generated files exist
                foreach (string generatedFile in generatedFilesToCheck)
                {
                    Assert.That(new FileInfo(Path.Combine(outputDir, generatedFile)), Does.Exist);
                }
            }
            finally
            {
                Directory.Delete(outputDir, true);
            }
        }

        [Test]
        public void CommandLineNoArgs()
        {
            AvroGenToolResult result = RunAvroGenTool(Array.Empty<string>());

            Assert.That(result.ExitCode, Is.EqualTo(1));
            Assert.That(result.StdOut, Is.Not.Empty);
            Assert.That(result.StdErr, Is.Empty);
        }

        [TestCase("-h")]
        [TestCase("--help")]
        [TestCase("--help", "-h")]
        [TestCase("--help", "-s", "whatever.avsc", ".")]
        [TestCase("-p", "whatever.avsc", ".", "-h")]
        public void CommandLineHelp(params string[] args)
        {
            AvroGenToolResult result = RunAvroGenTool(args);

            Assert.That(result.ExitCode, Is.EqualTo(0));
            Assert.That(result.StdOut, Is.Not.Empty);
            Assert.That(result.StdErr, Is.Empty);
        }

        [TestCase("-p")]
        [TestCase("-s")]
        [TestCase("-p", "whatever.avsc")]
        [TestCase("-s", "whatever.avsc")]
        [TestCase("whatever.avsc")]
        [TestCase("whatever.avsc .")]
        [TestCase(".")]
        [TestCase("-s", "whatever.avsc", "--namespace")]
        [TestCase("-s", "whatever.avsc", "--namespace", "org.apache")]
        [TestCase("-s", "whatever.avsc", "--namespace", "org.apache:")]
        [TestCase("-s", "whatever.avsc", ".", "whatever")]
        public void CommandLineInvalidArgs(params string[] args)
        {
            AvroGenToolResult result = RunAvroGenTool(args);

            Assert.That(result.ExitCode, Is.EqualTo(1));
            Assert.That(result.StdOut, Is.Not.Empty);
            Assert.That(result.StdErr, Is.Not.Empty);
        }

        [TestCase(@"
{
    ""type"": ""fixed"",
    ""namespace"": ""com.base"",
    ""name"": ""MD5"",
    ""size"": 16
}",
            new string[]
            {
                "com/base/MD5.cs"
            })]
        public void GenerateSchema(string schema, IEnumerable<string> generatedFilesToCheck)
        {
            TestSchema(schema, namespaceMapping: null, generatedFilesToCheck: generatedFilesToCheck);
        }

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
                "SchemaTest/MD5.cs"
            })]
        public void GenerateSchemaWithNamespaceMapping(string schema, string namespaceMappingFrom, string namespaceMappingTo, IEnumerable<string> generatedFilesToCheck)
        {
            TestSchema(schema, namespaceMapping: new Dictionary<string, string> { { namespaceMappingFrom, namespaceMappingTo } }, generatedFilesToCheck: generatedFilesToCheck);
        }
    }
}
