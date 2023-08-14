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
using System.Linq;
using System.Reflection;
using NUnit.Framework;

namespace Avro.Test.AvroGen
{
    [TestFixture]

    class AvroGenToolTests
    {
        [Test]
        public void CommandLineNoArgs()
        {
            AvroGenToolResult result = AvroGenHelper.RunAvroGenTool(Array.Empty<string>());

            Assert.That(result.ExitCode, Is.EqualTo(1));
            Assert.That(result.StdOut, Is.Not.Empty);
            Assert.That(result.StdErr, Is.Empty);
        }

        [TestCase("-h")]
        [TestCase("--help")]
        [TestCase("--help", "-h")]
        [TestCase("--help", "-s", "whatever.avsc", ".")]
        [TestCase("-p", "whatever.avpr", ".", "-h")]
        public void CommandLineHelp(params string[] args)
        {
            AvroGenToolResult result = AvroGenHelper.RunAvroGenTool(args);

            Assert.That(result.ExitCode, Is.EqualTo(0));
            Assert.That(result.StdOut, Is.Not.Empty);
            Assert.That(result.StdErr, Is.Empty);
        }

        [TestCase("--version")]
        [TestCase("-V")]
        public void CommandLineVersion(params string[] args)
        {
            AvroGenToolResult result = AvroGenHelper.RunAvroGenTool(args);

            Assert.That(result.ExitCode, Is.EqualTo(0));
            Assert.That(result.StdOut, Is.Not.Empty);
            Assert.That(result.StdErr, Is.Empty);

            // Check if returned version is SemVer 2.0 compliant
            Assert.That(result.StdOut[0], Does.Match(Utils.VersionTests.SemVerRegex));

            // Returned version must be the same as the avrogen tool assembly's version
            Assert.That(result.StdOut[0], Is.EqualTo(typeof(AvroGenTool).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion));

            // Returned version must be the same as the avro library assembly's version
            Assert.That(result.StdOut[0], Is.EqualTo(typeof(Schema).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion));
        }

        [TestCase("-p")]
        [TestCase("-s")]
        [TestCase("-p", "whatever.avpr")]
        [TestCase("-p", "whatever.avpr")]
        [TestCase("-s", "whatever.avsc")]
        [TestCase("whatever.avsc")]
        [TestCase("whatever.avsc", ".")]
        [TestCase(".")]
        [TestCase("-s", "whatever.avsc", "--namespace")]
        [TestCase("-s", "whatever.avsc", "--namespace", "org.apache")]
        [TestCase("-s", "whatever.avsc", "--namespace", "org.apache:")]
        [TestCase("-s", "whatever.avsc", ".", "whatever")]
        public void CommandLineInvalidArgs(params string[] args)
        {
            AvroGenToolResult result = AvroGenHelper.RunAvroGenTool(args);

            Assert.That(result.ExitCode, Is.EqualTo(1));
            Assert.That(result.StdOut, Is.Not.Empty);
            Assert.That(result.StdErr, Is.Not.Empty);
        }

        [Theory]
        public void CommandLineHelpContainsSkipDirectoriesParameter()
        {
            AvroGenToolResult result = AvroGenHelper.RunAvroGenTool("-h");

            Assert.That(result.ExitCode, Is.EqualTo(0));
            Assert.IsTrue(result.StdOut.Any(s => s.Contains("--skip-directories")));
        }
    }
}
