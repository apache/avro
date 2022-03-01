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
using NUnit.Framework;
using System.Text;

namespace Avro.Test.AvroGen
{
    [TestFixture]

    class AvroGenTest
    {
        class AvroGenResult
        {
            public int ExitCode { get; set; }
            public string[] StdOut { get; set; }
            public string[] StdErr { get; set; }
        }

        private AvroGenResult RunAvroGen(params string[] args)
        {
            // Save stdout and stderr
            TextWriter conOut = Console.Out;
            TextWriter conErr = Console.Error;

            try
            {
                AvroGenResult result = new AvroGenResult();
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

                    result.ExitCode = Avro.AvroGen.Main(args);

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
            AvroGenResult result = RunAvroGen(Array.Empty<string>());
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
            AvroGenResult result = RunAvroGen(args);

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
            AvroGenResult result = RunAvroGen(args);
            Assert.AreEqual(1, result.ExitCode);
            Assert.AreNotEqual(0, result.StdOut.Length);
            Assert.AreNotEqual(0, result.StdErr.Length);
        }

        // TODO:
        //   - Tests to generate actual source code from schema files
        //   - Compile the generated code
        //   - Execute the generated code and check for expected behaviour (?)
    }
}
