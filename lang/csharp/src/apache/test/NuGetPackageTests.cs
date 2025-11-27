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
using System.IO.Compression;
using System.Linq;
using System.Xml.Linq;
using NUnit.Framework;

namespace Avro.Test.Utils
{
    [TestFixture]
    public class NuGetPackageTests
    {
        private static readonly string[] PackageIds = new[]
        {
            "Apache.Avro",
            "Apache.Avro.Tools",
            "Apache.Avro.File.Snappy",
            "Apache.Avro.File.BZip2",
            "Apache.Avro.File.XZ",
            "Apache.Avro.File.Zstandard"
        };

        [TestCaseSource(nameof(PackageIds))]
        public void PackageContainsSpdxLicenseExpression(string packageId)
        {
            var nupkgPath = FindPackageInBuildOutput(packageId);
            if (nupkgPath == null)
            {
                Assert.Inconclusive($"Package {packageId} not found. Run 'dotnet pack --configuration Release' first.");
                return;
            }

            var nuspecXml = ExtractNuspecFromPackage(nupkgPath);
            
            // Get the namespace from the root element
            var ns = nuspecXml.Root?.Name.Namespace ?? XNamespace.None;
            var licenseElement = nuspecXml.Root?.Element(ns + "metadata")?.Element(ns + "license");
            
            Assert.That(licenseElement, Is.Not.Null, 
                $"Package {packageId} does not contain a license element");
            
            var licenseType = licenseElement?.Attribute("type")?.Value;
            Assert.That(licenseType, Is.EqualTo("expression"), 
                $"Package {packageId} license type should be 'expression', but was '{licenseType}'");
            
            var licenseValue = licenseElement?.Value;
            Assert.That(licenseValue, Is.EqualTo("Apache-2.0"), 
                $"Package {packageId} should have SPDX license expression 'Apache-2.0', but was '{licenseValue}'");
        }

        [TestCaseSource(nameof(PackageIds))]
        public void PackageContainsLicenseFile(string packageId)
        {
            var nupkgPath = FindPackageInBuildOutput(packageId);
            if (nupkgPath == null)
            {
                Assert.Inconclusive($"Package {packageId} not found. Run 'dotnet pack --configuration Release' first.");
                return;
            }

            using (var archive = ZipFile.OpenRead(nupkgPath))
            {
                var licenseEntry = archive.Entries.FirstOrDefault(e => 
                    e.FullName.Equals("LICENSE", StringComparison.OrdinalIgnoreCase));
                
                Assert.That(licenseEntry, Is.Not.Null, 
                    $"Package {packageId} does not contain LICENSE file");
                
                Assert.That(licenseEntry.Length, Is.GreaterThan(0), 
                    $"Package {packageId} LICENSE file is empty");
            }
        }

        [TestCaseSource(nameof(PackageIds))]
        public void PackageLicenseFileContainsApacheLicense(string packageId)
        {
            var nupkgPath = FindPackageInBuildOutput(packageId);
            if (nupkgPath == null)
            {
                Assert.Inconclusive($"Package {packageId} not found. Run 'dotnet pack --configuration Release' first.");
                return;
            }

            using (var archive = ZipFile.OpenRead(nupkgPath))
            {
                var licenseEntry = archive.Entries.FirstOrDefault(e => 
                    e.FullName.Equals("LICENSE", StringComparison.OrdinalIgnoreCase));
                
                Assert.That(licenseEntry, Is.Not.Null);

                using (var stream = licenseEntry.Open())
                using (var reader = new StreamReader(stream))
                {
                    var content = reader.ReadToEnd();
                    Assert.That(content, Does.Contain("Apache License"), 
                        $"Package {packageId} LICENSE file does not contain Apache License text");
                    Assert.That(content, Does.Contain("Version 2.0"), 
                        $"Package {packageId} LICENSE file does not specify Version 2.0");
                }
            }
        }

        private string FindPackageInBuildOutput(string packageId)
        {
            // Find the lang/csharp root (4 levels up from test binary directory: bin/Release/net8.0 -> test -> apache -> src -> csharp)
            var testDir = TestContext.CurrentContext.TestDirectory;
            var csharpRoot = Path.GetFullPath(Path.Combine(testDir, "..", "..", "..", "..", ".."));
            
            // Search for the package in Release output directories
            var pattern = $"{packageId}.*.nupkg";
            var files = Directory.GetFiles(csharpRoot, pattern, SearchOption.AllDirectories)
                .Where(f => f.Contains($"{Path.DirectorySeparatorChar}Release{Path.DirectorySeparatorChar}"))
                .OrderByDescending(f => System.IO.File.GetLastWriteTime(f))
                .ToArray();
            
            return files.Length > 0 ? files[0] : null;
        }

        private XDocument ExtractNuspecFromPackage(string nupkgPath)
        {
            using (var archive = ZipFile.OpenRead(nupkgPath))
            {
                var nuspecEntry = archive.Entries.FirstOrDefault(e => 
                    e.FullName.EndsWith(".nuspec", StringComparison.OrdinalIgnoreCase));
                
                if (nuspecEntry == null)
                {
                    Assert.Fail($"No .nuspec file found in package: {nupkgPath}");
                }

                using (var stream = nuspecEntry.Open())
                {
                    return XDocument.Load(stream);
                }
            }
        }
    }
}
