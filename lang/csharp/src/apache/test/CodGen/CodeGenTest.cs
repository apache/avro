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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.CodeDom.Compiler;
using Microsoft.CSharp;
using NUnit.Framework;
using Avro.Specific;

namespace Avro.Test
{
    [TestFixture]
 
    class CodeGenTest
    {
        [TestCase(@"{
""type"" : ""record"",
""name"" : ""ClassKeywords"",
""namespace"" : ""com.base"",
""fields"" : 
		[ 	
			{ ""name"" : ""int"", ""type"" : ""int"" },
			{ ""name"" : ""base"", ""type"" : ""long"" },
			{ ""name"" : ""event"", ""type"" : ""boolean"" },
			{ ""name"" : ""foreach"", ""type"" : ""double"" },
			{ ""name"" : ""bool"", ""type"" : ""float"" },
			{ ""name"" : ""internal"", ""type"" : ""bytes"" },
			{ ""name"" : ""while"", ""type"" : ""string"" },
			{ ""name"" : ""return"", ""type"" : ""null"" },
			{ ""name"" : ""enum"", ""type"" : { ""type"" : ""enum"", ""name"" : ""class"", ""symbols"" : [ ""A"", ""B"" ] } },
			{ ""name"" : ""string"", ""type"" : { ""type"": ""fixed"", ""size"": 16, ""name"": ""static"" } }
		]
}
", new object[] {"com.base.ClassKeywords", typeof(int), typeof(long), typeof(bool), typeof(double), typeof(float), typeof(byte[]), typeof(string),typeof(object),"com.base.class", "com.base.static"})]
        [TestCase(@"{
""type"" : ""record"",
""name"" : ""SchemaObject"",
""namespace"" : ""schematest"",
""fields"" : 
	[ 	
		{ ""name"" : ""myobject"", ""type"" : 
			[ 
				""null"", 
				{""type"" : ""array"", ""items"" : [ ""null"", 
											{ ""type"" : ""enum"", ""name"" : ""MyEnum"", ""symbols"" : [ ""A"", ""B"" ] },
											{ ""type"": ""fixed"", ""size"": 16, ""name"": ""MyFixed"" } 
											]
				}
			]
		}
	]
}
", new object[] { "schematest.SchemaObject", typeof(IList<object>) })]
        public static void TestCodeGen(string str, object[] result)
        {
            Schema schema = Schema.Parse(str);

            CompilerResults compres = GenerateSchema(schema);

            // instantiate object
            ISpecificRecord rec = compres.CompiledAssembly.CreateInstance((string)result[0]) as ISpecificRecord;
            Assert.IsNotNull(rec);

            // test type of each fields
            for (int i = 1; i < result.Length; ++i)
            {
                object field = rec.Get(i - 1);
                Type stype;
                if (result[i].GetType() == typeof(string))
                {
                    object obj = compres.CompiledAssembly.CreateInstance((string)result[i]);
                    Assert.IsNotNull(obj);
                    stype = obj.GetType();
                }
                else
                    stype = (Type)result[i];
                if (!stype.IsValueType)
                    Assert.IsNull(field);   // can't test reference type, it will be null
                else
                    Assert.AreEqual(stype, field.GetType());
            }
        }

        [Test]
        public void CanCodeGenTraceProtocol()
        {
            var traceProtocol = File.ReadAllText("../../../../../share/schemas/org/apache/avro/ipc/trace/avroTrace.avpr");
            Protocol protocol = Protocol.Parse(traceProtocol);
            var compilerResults = GenerateProtocol(protocol);

            // instantiate object
            var types = compilerResults.CompiledAssembly.GetTypes().Select(t => t.FullName);
            Assert.That(4, Is.EqualTo(types.Count()));
            Assert.That(types.Contains("org.apache.avro.ipc.trace.ID"), "Should have contained ID type");
            Assert.That(types.Contains("org.apache.avro.ipc.trace.Span"), "Should have contained Span type");
            Assert.That(types.Contains("org.apache.avro.ipc.trace.SpanEvent"), "Should have contained SpanEvent type");
            Assert.That(types.Contains("org.apache.avro.ipc.trace.TimestampedEvent"), "Should have contained TimestampedEvent type");
        }

        private static CompilerResults GenerateSchema(Schema schema)
        {
            var codegen = new CodeGen();
            codegen.AddSchema(schema);
            return GenerateAssembly(codegen);
        }

        private static CompilerResults GenerateProtocol(Protocol protocol)
        {
            var codegen = new CodeGen();
            codegen.AddProtocol(protocol);
            return GenerateAssembly(codegen);            
        }

        private static CompilerResults GenerateAssembly(CodeGen schema)
        {
            var compileUnit = schema.GenerateCode();

            var comparam = new CompilerParameters(new string[] { "mscorlib.dll" });
            comparam.ReferencedAssemblies.Add("System.dll");
            comparam.ReferencedAssemblies.Add("System.Core.dll");
            comparam.ReferencedAssemblies.Add(Type.GetType("Mono.Runtime") != null ? "Mono.CSharp.dll" : "Microsoft.CSharp.dll");
            comparam.ReferencedAssemblies.Add("Avro.dll");
            comparam.GenerateInMemory = true;
            var ccp = new CSharpCodeProvider();
            var units = new[] { compileUnit };
            var compres = ccp.CompileAssemblyFromDom(comparam, units);
            if (compres.Errors.Count > 0)
            {
                for (int i = 0; i < compres.Errors.Count; i++)
                    Console.WriteLine(compres.Errors[i]);
            }
            Assert.AreEqual(0, compres.Errors.Count);
            return compres;
        }
    }
}
