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
    }
}
