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
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis.CSharp;
using NUnit.Framework;

namespace Avro.Test.CodeGen
{
    [TestFixture]
    class CodeGenTests
    {

        [Test]
        public void TestGetNullableTypeException()
        {
            Assert.Throws<ArgumentNullException>(() => Avro.CodeGen.GetNullableType(null));
        }

        [Test]
        public void TestReservedKeywords()
        {
            // https://github.com/dotnet/roslyn/blob/main/src/Compilers/CSharp/Portable/Syntax/SyntaxKindFacts.cs

            // Check if all items in CodeGenUtil.Instance.ReservedKeywords are keywords
            foreach (string keyword in CodeGenUtil.Instance.ReservedKeywords)
            {
                Assert.That(SyntaxFacts.GetKeywordKind(keyword) != SyntaxKind.None, Is.True);
            }

            // Check if all Roslyn defined keywords are in CodeGenUtil.Instance.ReservedKeywords
            foreach (SyntaxKind keywordKind in SyntaxFacts.GetReservedKeywordKinds())
            {
                Assert.That(CodeGenUtil.Instance.ReservedKeywords, Does.Contain(SyntaxFacts.GetText(keywordKind)));
            }

            // If this test fails, CodeGenUtil.ReservedKeywords list must be updated.
            // This might happen if newer version of C# language defines new reserved keywords.
        }

        [TestCase("a", "a")]
        [TestCase("a.b", "a.b")]
        [TestCase("a.b.c", "a.b.c")]
        [TestCase("int", "@int")]
        [TestCase("a.long.b", "a.@long.b")]
        [TestCase("int.b.c", "@int.b.c")]
        [TestCase("a.b.int", "a.b.@int")]
        [TestCase("int.long.while", "@int.@long.@while")] // Reserved keywords
        [TestCase("a.value.partial", "a.value.partial")] // Contextual keywords
        [TestCase("a.value.b.int.c.while.longpartial", "a.value.b.@int.c.@while.longpartial")] // Reserved and contextual keywords
        public void TestMangleUnMangle(string input, string mangled)
        {
            // Mangle
            Assert.That(CodeGenUtil.Instance.Mangle(input), Is.EqualTo(mangled));
            // Unmangle
            Assert.That(CodeGenUtil.Instance.UnMangle(mangled), Is.EqualTo(input));
        }

        [TestFixture]
        public class CodeGenTestClass : Avro.CodeGen
        {
            [Test]
            public void TestGenerateNamesException()
            {
                Protocol protocol = null;
                Assert.Throws<ArgumentNullException>(() => this.GenerateNames(protocol));
            }


            [Test]
            public void GetTypesShouldReturnTypes()
            {
                AddSchema(@"
{
  ""name"": ""PlanetEnum"",
  ""namespace"": ""Space.Models"",
  ""type"": ""enum"",
  ""symbols"": [
    ""Earth"",
    ""Mars"",
    ""Jupiter"",
    ""Saturn"",
    ""Uranus"",
    ""Neptune""
  ]
}
");
                GenerateCode();
                var types = GetTypes();
                Assert.That(types.Count, Is.EqualTo(1));
                bool hasPlanetEnumCode = types.TryGetValue("PlanetEnum", out string planetEnumCode);
                Assert.That(hasPlanetEnumCode);
                Assert.That(Regex.Matches(planetEnumCode, "public enum PlanetEnum").Count, Is.EqualTo(1));
            }

            [Test]
            public void RecordGetAndPutSwitchesShouldNotEmitUnreachableStatement()
            {
                AddSchema(@"
{
  ""name"": ""Sample"",
  ""namespace"": ""Avro.Test.CodeGen.UnreachableCodeRegression"",
  ""type"": ""record"",
  ""fields"": [
    { ""name"": ""value"", ""type"": ""string"" }
  ]
}
");
                GenerateCode();
                var types = GetTypes();
                bool hasSampleCode = types.TryGetValue("Sample", out string sampleCode);
                Assert.That(hasSampleCode);

                // The exhaustive switch in Get()/Put() must not be followed by a stray ";" -
                // that empty statement is unreachable (every case returns or throws) and
                // trips IDE-only unreachable-code analysis (e.g. ReSharper) even though Roslyn
                // does not flag it.
                Assert.That(Regex.IsMatch(sampleCode, @"\}\s*;\s*\}\s*public virtual void Put"), Is.False);
                Assert.That(Regex.IsMatch(sampleCode, @"in Put\(\)""\);\s*\}\s*;"), Is.False);

                // CodeDom writes CodeSnippetStatement at column 0, so the snippet carries its own indent.
                Assert.That(Regex.Matches(sampleCode, @"(?m)^\t\t\tswitch \(fieldPos\)\r?$").Count, Is.EqualTo(2));
                Assert.That(Regex.Matches(sampleCode, @"(?m)^\s*switch \(fieldPos\)").Count, Is.EqualTo(2));
            }

            [Test]
            public void ProtocolRequestSwitchShouldBeIndentedAndNotFollowedByEmptyStatement()
            {
                AddProtocol(@"
{
  ""protocol"": ""Greeter"",
  ""namespace"": ""Avro.Test.CodeGen.ProtocolSwitchRegression"",
  ""types"": [],
  ""messages"": {
    ""hello"": {
      ""request"": [ { ""name"": ""greeting"", ""type"": ""string"" } ],
      ""response"": ""string""
    }
  }
}
");
                GenerateCode();
                var types = GetTypes();
                bool hasGreeterCode = types.TryGetValue("Greeter", out string greeterCode);
                Assert.That(hasGreeterCode);

                Assert.That(Regex.Matches(greeterCode, @"(?m)^\t\t\tswitch\(messageName\)\r?$").Count, Is.EqualTo(1));
                Assert.That(Regex.IsMatch(greeterCode, @"break;\s*\}\s*;"), Is.False);
            }

            [Test]
            public void ProtocolWithoutMessagesShouldGenerateEmptyRequestBody()
            {
                AddProtocol(@"
{
  ""protocol"": ""Silent"",
  ""namespace"": ""Avro.Test.CodeGen.EmptyProtocolRegression"",
  ""types"": [],
  ""messages"": {}
}
");
                GenerateCode();
                var types = GetTypes();
                bool hasSilentCode = types.TryGetValue("Silent", out string silentCode);
                Assert.That(hasSilentCode);

                Assert.That(Regex.IsMatch(silentCode, @"object callback\)\s*\{\s*\}"), Is.True);
            }

            [Test]
            public void EnumWithKeywordSymbolsShouldHavePrefixedSymbols()
            {
                AddSchema(@"{
    ""type"": ""enum"",
    ""symbols"": [
        ""string"",
        ""integer"",
        ""float"",
        ""boolean"",
        ""list"",
        ""dict"",
        ""regex""
    ],
    ""name"": ""type"",
    ""namespace"": ""com.example""
}");
                GenerateCode();
                var types = GetTypes();
                Assert.That(types.Count, Is.EqualTo(1));
                bool hasTypeCode = types.TryGetValue("type", out string typeCode);
                Assert.That(hasTypeCode);
                Assert.That(Regex.Matches(typeCode, "public enum type").Count, Is.EqualTo(1));
                Assert.That(Regex.Matches(typeCode, "@string,").Count, Is.EqualTo(1));
                Assert.That(Regex.Matches(typeCode, "@float,").Count, Is.EqualTo(1));
            }
        }
    }
}
