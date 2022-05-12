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

using System.Reflection;
using NUnit.Framework;

namespace Avro.Test.Utils
{
    public class VersionTests
    {
        // SemVer2.0 Regular Expression
        public static string SemVerRegex = @"^((([0-9]+)\.([0-9]+)\.([0-9]+)(?:-([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?)(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?)$";
        
        [Test]
        public void VersionTest()
        {
            // Avro library's assembly
            Assembly assembly = typeof(Schema).Assembly;

            // Note: InformationalVersion contains prerelease tag if available (e.g. 1.x.y-beta.z)
            string libraryVersion = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion;

            // Check version is SmeVer 2.0 compliant
            Assert.That(libraryVersion, Does.Match(SemVerRegex));
        }

        [Test]
        public void MandatoryAttributesTest()
        {
            // Avro library's assembly
            Assembly assembly = typeof(Schema).Assembly;

            Assert.That(assembly.GetCustomAttribute<AssemblyCompanyAttribute>(), Is.Not.Null);
            Assert.That(assembly.GetCustomAttribute<AssemblyDescriptionAttribute>(), Is.Not.Null);
            Assert.That(assembly.GetCustomAttribute<AssemblyFileVersionAttribute>(), Is.Not.Null);
            Assert.That(assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>(), Is.Not.Null);
            Assert.That(assembly.GetCustomAttribute<AssemblyProductAttribute>(), Is.Not.Null);
        }
    }
}
