/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using Microsoft.CodeAnalysis.CSharp;
using Avro.SchemaGen;
using NUnit.Framework;

namespace Avro.Test
{


    [TestFixture]
    public class TestSchemaGen
    {
        [TestCase]
        public void TestBasicParser()
        {
            var tree = CSharpSyntaxTree.ParseText(@"
            using System.Collections.Generic;
            /// <summary>
            /// This is my class
            /// </summary>
            namespace test.this.a.b
            {
                public class MyClass
                {
                    private int notVisible {get;set;}
                    /// <summary>
                    /// This is my property
                    /// </summary>
                    /// <value>Value of my property</value>
                    public int MyProperty {get; set;}
                    public MyEnum E {get;set;}
                    [AvroField(""Anothername"", myConverter)]
                    public MyOtherClass Other {get;set;}
                    public List<MyEnum> MyList {get; set;}
                    public bool? MyOptional {get;set;}
                    public Int32 MyInt32 {get;set;}
                    public Boolean MyBoolean {get;set;}
                    public string MyString {get;set;}
                    public String MyString2 {get;set;}
                    public byte[] MyBytes{get;set;}
                }
                public enum MyEnum
                {
                    A, B, C
                }

                public class MyOtherClass
                {
                    public void MyMethod(int n)
                    {
                    }
                }
            }
        ");

            var builder = new SchemaBuilder(null, null, null);
            builder.Visit(tree.GetRoot());
            var j = builder.GetProtocolTypes();
            var s = j.ToString();
        }
    }
}
