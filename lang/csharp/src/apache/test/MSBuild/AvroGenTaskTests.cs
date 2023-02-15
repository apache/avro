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
using NUnit.Framework;
using Microsoft.Build.Framework;
using Moq;
using Avro.msbuild;

namespace Avro.Test.MSBuild
{
    [TestFixture]
    public class AvroGenTaskTests
    {
        public static string CreateEmptyTemporaryFolder(out string uniqueId, string path = null)
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

        private bool RunAvroGenTask(
            IEnumerable<string> schemaFiles,
            IEnumerable<string> protocolFiles,
            string namespaceMapping,
            bool skipDirectories,
            IEnumerable<string> expectedGeneratedFiles,
            out IEnumerable<BuildMessageEventArgs> taskMessages,
            out IEnumerable<BuildWarningEventArgs> taskWarnings,
            out IEnumerable<BuildErrorEventArgs> taskErrors)
        {
            string outputDir = CreateEmptyTemporaryFolder(out var _);

            try
            {
                var buildEngine = new Mock<IBuildEngine>();
                var errors = new List<BuildErrorEventArgs>();
                var messages = new List<BuildMessageEventArgs>();
                var warnings = new List<BuildWarningEventArgs>();

                taskErrors = errors;
                taskWarnings = warnings;
                taskMessages = messages;

                buildEngine.Setup(x => x.LogErrorEvent(It.IsAny<BuildErrorEventArgs>())).Callback<BuildErrorEventArgs>(e => errors.Add(e));
                buildEngine.Setup(x => x.LogMessageEvent(It.IsAny<BuildMessageEventArgs>())).Callback<BuildMessageEventArgs>(m => messages.Add(m));
                buildEngine.Setup(x => x.LogWarningEvent(It.IsAny<BuildWarningEventArgs>())).Callback<BuildWarningEventArgs>(w => warnings.Add(w));

                List<Mock<ITaskItem>> schemas = new List<Mock<ITaskItem>>();
                List<Mock<ITaskItem>> protocols = new List<Mock<ITaskItem>>();

                foreach (var schemaFile in schemaFiles)
                {
                    Mock<ITaskItem> schema = new Mock<ITaskItem>();
                    schema.Setup(x => x.ItemSpec).Returns(schemaFile);
                    schemas.Add(schema);
                }

                foreach (var protocolFile in protocolFiles)
                {
                    Mock<ITaskItem> protocol = new Mock<ITaskItem>();
                    protocol.Setup(x => x.ItemSpec).Returns(protocolFile);
                    protocols.Add(protocol);
                }

                Mock<ITaskItem> outDir = new Mock<ITaskItem>();
                outDir.Setup(x => x.ItemSpec).Returns(outputDir);

                var avroGenTask = new AvroGenTask()
                {
                    BuildEngine = buildEngine.Object,
                    SchemaFiles = schemas.Select(x => x.Object).ToArray(),
                    ProtocolFiles = protocols.Select(x => x.Object).ToArray(),
                    OutDir = outDir.Object,
                    NamespaceMapping = namespaceMapping,
                    SkipDirectories = skipDirectories
                };

                bool taskResult = avroGenTask.Execute();
                var generatedTaskItems = avroGenTask.GeneratedFiles;
                // Check generated files
                foreach(var generatedFile in expectedGeneratedFiles)
                {
                    var generatedFileInfo = new FileInfo(Path.Combine(outputDir, generatedFile));
                    Assert.That(generatedFileInfo, Does.Exist);
                    // Check task output vs. expected
                    Assert.That(generatedTaskItems.Any(item => item.ItemSpec == generatedFileInfo.FullName));
                }

                return taskResult;
            }
            finally
            {
                Directory.Delete(outputDir, true);
            }
        }

        [Test]
        public void AvroGenTask_Schema()
        {
            bool result = RunAvroGenTask(
                new List<string>() {
                    "../../../../benchmark/schema/small.avsc"
                },
                Enumerable.Empty<string>(),
                null,
                false,
                new List<string>() {
                    "./org/apache/avro/benchmark/small/test.cs"
                },
                out var taskMessages,
                out var taskWarnings,
                out var taskErrors);

            Assert.IsTrue(result);
            Assert.AreEqual(1, taskMessages.Count());
            Assert.AreEqual(0, taskWarnings.Count());
            Assert.AreEqual(0, taskErrors.Count());
        }

        [Test]
        public void AvroGenTask_Schemas()
        {
            bool result = RunAvroGenTask(
                new List<string>() {
                    "../../../../benchmark/schema/big.avsc",
                    "../../../../benchmark/schema/small.avsc"
                },
                Enumerable.Empty<string>(),
                null,
                false,
                new List<string>() {
                    "./org/apache/avro/benchmark/big/mailing_address.cs",
                    "./org/apache/avro/benchmark/big/userInfo.cs",
                    "./org/apache/avro/benchmark/small/test.cs"
                },
                out var taskMessages,
                out var taskWarnings,
                out var taskErrors);

            Assert.IsTrue(result);
            Assert.AreEqual(3, taskMessages.Count());
            Assert.AreEqual(0, taskWarnings.Count());
            Assert.AreEqual(0, taskErrors.Count());
        }

        [Test]
        public void AvroGenTask_Schemas_NamespaceMapping()
        {
            bool result = RunAvroGenTask(
                new List<string>() {
                    "../../../../benchmark/schema/big.avsc",
                    "../../../../benchmark/schema/small.avsc"
                },
                Enumerable.Empty<string>(),
                "org.apache.avro.benchmark.big:big,org.apache.avro.benchmark.small:small",
                false,
                new List<string>() {
                    "./big/mailing_address.cs",
                    "./big/userInfo.cs",
                    "./small/test.cs"
                },
                out var taskMessages,
                out var taskWarnings,
                out var taskErrors);

            Assert.IsTrue(result);
            Assert.AreEqual(3, taskMessages.Count());
            Assert.AreEqual(0, taskWarnings.Count());
            Assert.AreEqual(0, taskErrors.Count());
        }

        [Test]
        public void AvroGenTask_Schemas_SkipDirectories()
        {
            bool result = RunAvroGenTask(
                new List<string>() {
                    "../../../../benchmark/schema/big.avsc",
                    "../../../../benchmark/schema/small.avsc"
                },
                Enumerable.Empty<string>(),
                null,
                true,
                new List<string>() {
                    "./mailing_address.cs",
                    "./userInfo.cs",
                    "./test.cs"
                },
                out var taskMessages,
                out var taskWarnings,
                out var taskErrors);

            Assert.IsTrue(result);
            Assert.AreEqual(3, taskMessages.Count());
            Assert.AreEqual(0, taskWarnings.Count());
            Assert.AreEqual(0, taskErrors.Count());
        }
    }
}
