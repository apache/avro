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
        [TestCase("a.value.b.int.c.while.longpartial", "a.value.b.@int.c.@while.longpartial")] // Rseserved and contextual keywords
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
        }
    }
}
