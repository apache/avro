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
using System.IO;
using System.Collections.Generic;
using NUnit.Framework;
using Avro;
using Avro.Generic;
using Avro.IO;
using System.CodeDom;
using System.CodeDom.Compiler;
using Microsoft.CSharp;
using Avro.Specific;
using System.Reflection;

namespace Avro.Test
{
    [TestFixture]
    class SpecificTests
    {
        [TestCase(@"{
  ""protocol"" : ""MyProtocol"",
  ""namespace"" : ""com.foo"",
  ""types"" : [ 
   {
	""type"" : ""record"",
	""name"" : ""A"",
	""fields"" : [ { ""name"" : ""f1"", ""type"" : ""long"" } ]
   },
   {
	""type"" : ""enum"",
	""name"" : ""MyEnum"",
	""symbols"" : [ ""A"", ""B"", ""C"" ]
   },
   {
   ""type"": ""fixed"", 
   ""size"": 16, 
   ""name"": ""MyFixed""
   },
   {
	""type"" : ""record"",
	""name"" : ""Z"",
	""fields"" : 
			[ 	
				{ ""name"" : ""myUInt"", ""type"" : [ ""int"", ""null"" ] },
				{ ""name"" : ""myULong"", ""type"" : [ ""long"", ""null"" ] },
				{ ""name"" : ""myUBool"", ""type"" : [ ""boolean"", ""null"" ] },
				{ ""name"" : ""myUDouble"", ""type"" : [ ""double"", ""null"" ] },
				{ ""name"" : ""myUFloat"", ""type"" : [ ""float"", ""null"" ] },
				{ ""name"" : ""myUBytes"", ""type"" : [ ""bytes"", ""null"" ] },
				{ ""name"" : ""myUString"", ""type"" : [ ""string"", ""null"" ] },
				
				{ ""name"" : ""myInt"", ""type"" : ""int"" },
				{ ""name"" : ""myLong"", ""type"" : ""long"" },
				{ ""name"" : ""myBool"", ""type"" : ""boolean"" },
				{ ""name"" : ""myDouble"", ""type"" : ""double"" },
				{ ""name"" : ""myFloat"", ""type"" : ""float"" },
				{ ""name"" : ""myBytes"", ""type"" : ""bytes"" },
				{ ""name"" : ""myString"", ""type"" : ""string"" },
				{ ""name"" : ""myNull"", ""type"" : ""null"" },

				{ ""name"" : ""myFixed"", ""type"" : ""MyFixed"" },								
				{ ""name"" : ""myA"", ""type"" : ""A"" },
				{ ""name"" : ""myE"", ""type"" : ""MyEnum"" },
				{ ""name"" : ""myArray"", ""type"" : { ""type"" : ""array"", ""items"" : ""bytes"" } },
				{ ""name"" : ""myArray2"", ""type"" : { ""type"" : ""array"", ""items"" : { ""type"" : ""record"", ""name"" : ""newRec"", ""fields"" : [ { ""name"" : ""f1"", ""type"" : ""long""} ] } } },
				{ ""name"" : ""myMap"", ""type"" : { ""type"" : ""map"", ""values"" : ""string"" } },
				{ ""name"" : ""myMap2"", ""type"" : { ""type"" : ""map"", ""values"" : ""newRec"" } },
				{ ""name"" : ""myObject"", ""type"" : [ ""MyEnum"", ""A"", ""null"" ] },
                { ""name"" : ""myArray3"", ""type"" : { ""type"" : ""array"", ""items"" : { ""type"" : ""array"", ""items"" : [ ""double"", ""string"", ""null"" ] } } }
			]
   } 
   ]
}"
, new object[] {3, // index of the schema to serialize
  "com.foo.Z",  // name of the schema to serialize
@"Console.WriteLine(""Constructing com.foo.Z..."");", // Empty Constructor.
@"
  Console.WriteLine(""Populating com.foo.Z..."");
  string bytes = ""bytes sample text"";
  System.Text.UTF8Encoding encoding = new System.Text.UTF8Encoding();

  myUInt=1; 
  myULong=2; 
  myUBool=true; 
  myUDouble=(double)3; 
  myUFloat=(float)4.5; 
  myUBytes = encoding.GetBytes(bytes);
  myUString=""Hello""; 

  myInt=1; 
  myLong=2; 
  myBool=true; 
  myDouble=(double)3; 
  myFloat=(float)4.5; 
  myBytes=encoding.GetBytes(bytes);
  myString=""Hello"";
  myNull=null;

  string fixedstr = ""My fixed record0"";
  myFixed=new MyFixed(); myFixed.Value = encoding.GetBytes(fixedstr);
  myA=new A(); myA.f1 = 10;
  myE=com.foo.MyEnum.C;

  myArray=new List<byte[]>();
  myArray.Add(encoding.GetBytes(""a""));

  myArray2 = new List<com.foo.newRec>();
  com.foo.newRec rec = new com.foo.newRec();
  rec.f1 = 50;
  myArray2.Add(rec);

  myMap = new Dictionary<string, string>();
  myMap.Add(""key"", ""value"");
  myMap2 = new Dictionary<string, com.foo.newRec>();
  com.foo.newRec newrec = new com.foo.newRec();
  newrec.f1 = 1200;
  myMap2.Add(""A"", newrec);
  myObject = com.foo.MyEnum.B;

  IList<System.Object> o1 = new List<System.Object>();

    o1.Add((double)1123123121);
    o1.Add((double)2);
    o1.Add(null);
    o1.Add(""fred"");

    IList<System.Object> o2 = new List<System.Object>();

    o2.Add((double)1);
    o2.Add((double)32531);
    o2.Add((double)4);
    o2.Add((double)555);
    o2.Add((double)0);

    myArray3 = new List<IList<System.Object>>();
    myArray3.Add(o1);
    myArray3.Add(o2);

"}
)]
        public static void TestSpecific(string str, object[] result)
        {
            Protocol protocol = Protocol.Parse(str);
            var codegen = new CodeGen();
            codegen.AddProtocol(protocol);
            var compileUnit = codegen.GenerateCode();

            // add a constructor to the main class using the passed assignment statements
            CodeTypeDeclaration ctd = compileUnit.Namespaces[0].Types[(int)result[0]];
            CodeConstructor constructor = new CodeConstructor();
            constructor.Attributes = MemberAttributes.Public;
            CodeSnippetExpression snippet = new CodeSnippetExpression((string)result[2]);
            constructor.Statements.Add(snippet);
            ctd.Members.Add(constructor);

            // add a function to the main class to populate the data
            // This has been moved from constructor, as it was causing some tests to pass that shouldn't when referencing a blank object on READ.

            CodeMemberMethod method = new CodeMemberMethod();
            method.Attributes = MemberAttributes.Public;
            method.Name = "Populate";
            CodeSnippetExpression snippet2 = new CodeSnippetExpression((string)result[3]);
            method.Statements.Add(snippet2);
            ctd.Members.Add(method);



            // compile
            var comparam = new CompilerParameters(new string[] { "mscorlib.dll" });
            comparam.ReferencedAssemblies.Add("System.dll");
            comparam.ReferencedAssemblies.Add("System.Core.dll");
            comparam.ReferencedAssemblies.Add(Type.GetType("Mono.Runtime") != null ? "Mono.CSharp.dll" : "Microsoft.CSharp.dll");
            comparam.ReferencedAssemblies.Add("Avro.dll");
            comparam.GenerateInMemory = true;
            var ccp = new Microsoft.CSharp.CSharpCodeProvider();
            var units = new CodeCompileUnit[] { compileUnit };
            var compres = ccp.CompileAssemblyFromDom(comparam, units);
            if (compres == null || compres.Errors.Count > 0)
            {
                for (int i = 0; i < compres.Errors.Count; i++)
                    Console.WriteLine(compres.Errors[i]);
            }
            if (null != compres)
                Assert.IsTrue(compres.Errors.Count == 0);

            // create record
            ISpecificRecord rec = compres.CompiledAssembly.CreateInstance((string)result[1]) as ISpecificRecord;

            // Call populate to put some data in it.
            Type recType = rec.GetType(); ;
            MethodInfo methodInfo = recType.GetMethod("Populate");
            methodInfo.Invoke(rec, null);

            var x1 = compres.CompiledAssembly.FullName;
            

            Assert.IsFalse(rec == null);

            // serialize
            var stream = new MemoryStream();
            var binEncoder = new BinaryEncoder(stream);
            var writer = new SpecificDefaultWriter(rec.Schema);
            writer.Write(rec.Schema, rec, binEncoder);

            // deserialize
            stream.Position = 0;
            var decoder = new BinaryDecoder(stream);
            var reader = new SpecificDefaultReader(rec.Schema, rec.Schema);
            var rec2 = (ISpecificRecord)reader.Read(null, rec.Schema, rec.Schema, decoder);
            Assert.IsFalse(rec2 == null);
        }
    }
}
