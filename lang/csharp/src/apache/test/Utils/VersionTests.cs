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
using System.Reflection;
using System.Text.RegularExpressions;
using NUnit.Framework;

namespace Avro.Test.Utils
{
    public class VersionTests
    {
        // SemVer2.0 regex
        public static string SemVerRegex = @"^((([0-9]+)\.([0-9]+)\.([0-9]+)(?:-([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?)(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?)$";
        
        [Test]
        public void VersionTest()
        {
            // Avro library's assembly
            Assembly assembly = typeof(Schema).Assembly;

            // Note: InformationalVersion contains pre-release tag if available (e.g. 1.x.y-beta.z)
            string libraryVersion = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion;

            Regex regex = new Regex(SemVerRegex);

            // Check version is SmeVer 2.0 compliant
            Match match = regex.Match(libraryVersion);

            Assert.That(match.Success, Is.True);

            // Parse major.minor.patch values
            int major = int.Parse(match.Groups[3].Value);
            int minor = int.Parse(match.Groups[4].Value);
            int patch = int.Parse(match.Groups[5].Value);

            // If the following tests fail, setting FileVersion csproj property has been changed (common.props)
            // Currently FileVersion is defined as "major.minor.patch.0"
            Version fileVersion = Version.Parse(assembly.GetCustomAttribute<AssemblyFileVersionAttribute>().Version);
            Assert.That(fileVersion, Is.EqualTo(new Version(major, minor, patch, 0)));
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
