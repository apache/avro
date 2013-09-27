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
using System.Collections;
using System.IO;
using System.Linq;
using NUnit.Framework;
using Avro.IO;
using System.CodeDom;
using System.CodeDom.Compiler;
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
  myObject = myA;

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
        public void TestSpecific(string str, object[] result)
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
            var stream = serialize(rec.Schema, rec);

            // deserialize
            var rec2 = deserialize<ISpecificRecord>(stream, rec.Schema, rec.Schema);
            Assert.IsFalse(rec2 == null);
            AssertSpecificRecordEqual(rec, rec2);
        }

        [TestCase]
        public void TestEnumResolution()
        {
            Schema writerSchema = Schema.Parse("{\"type\":\"record\",\"name\":\"EnumRecord\",\"namespace\":\"Avro.Test\"," + 
                                        "\"fields\":[{\"name\":\"enumType\",\"type\": { \"type\": \"enum\", \"name\": \"EnumType\", \"symbols\": [\"FIRST\", \"SECOND\"]} }]}");

            var testRecord = new EnumRecord();

            Schema readerSchema = testRecord.Schema;
            testRecord.enumType = EnumType.SECOND;

            // serialize
            var stream = serialize(writerSchema, testRecord);

            // deserialize
            var rec2 = deserialize<EnumRecord>(stream, writerSchema, readerSchema);
            Assert.AreEqual( EnumType.SECOND, rec2.enumType );
        }

        private static S deserialize<S>(Stream ms, Schema ws, Schema rs) where S : class, ISpecificRecord
        {
            long initialPos = ms.Position;
            var r = new SpecificReader<S>(ws, rs);
            Decoder d = new BinaryDecoder(ms);
            S output = r.Read(null, d);
            Assert.AreEqual(ms.Length, ms.Position); // Ensure we have read everything.
            checkAlternateDeserializers(output, ms, initialPos, ws, rs);
            return output;
        }

        private static void checkAlternateDeserializers<S>(S expected, Stream input, long startPos, Schema ws, Schema rs) where S : class, ISpecificRecord
        {
            input.Position = startPos;
            var reader = new SpecificDatumReader<S>(ws, rs);
            Decoder d = new BinaryDecoder(input);
            S output = reader.Read(null, d);
            Assert.AreEqual(input.Length, input.Position); // Ensure we have read everything.
            AssertSpecificRecordEqual(expected, output);
        }

        private static Stream serialize<T>(Schema ws, T actual)
        {
            var ms = new MemoryStream();
            Encoder e = new BinaryEncoder(ms);
            var w = new SpecificWriter<T>(ws);
            w.Write(actual, e);
            ms.Flush();
            ms.Position = 0;
            checkAlternateSerializers(ms.ToArray(), actual, ws);
            return ms;
        }

        private static void checkAlternateSerializers<T>(byte[] expected, T value, Schema ws)
        {
            var ms = new MemoryStream();
            var writer = new SpecificDatumWriter<T>(ws);
            var e = new BinaryEncoder(ms);
            writer.Write(value, e);
            var output = ms.ToArray();
            
            Assert.AreEqual(expected.Length, output.Length);
            Assert.True(expected.SequenceEqual(output));
        }

        private static void AssertSpecificRecordEqual(ISpecificRecord rec1, ISpecificRecord rec2)
        {
            var recordSchema = (RecordSchema) rec1.Schema;
            for (int i = 0; i < recordSchema.Count; i++)
            {
                var rec1Val = rec1.Get(i);
                var rec2Val = rec2.Get(i);
                if (rec1Val is ISpecificRecord)
                {
                    AssertSpecificRecordEqual((ISpecificRecord)rec1Val, (ISpecificRecord)rec2Val);
                }
                else if (rec1Val is IList)
                {
                    var rec1List = (IList) rec1Val;
                    if( rec1List.Count > 0 && rec1List[0] is ISpecificRecord)
                    {
                        var rec2List = (IList) rec2Val;
                        Assert.AreEqual(rec1List.Count, rec2List.Count);
                        for (int j = 0; j < rec1List.Count; j++)
                        {
                            AssertSpecificRecordEqual((ISpecificRecord)rec1List[j], (ISpecificRecord)rec2List[j]);
                        }
                    }
                    else
                    {
                        Assert.AreEqual(rec1Val, rec2Val);
                    }
                }
                else if (rec1Val is IDictionary)
                {
                    var rec1Dict = (IDictionary) rec1Val;
                    var rec2Dict = (IDictionary) rec2Val;
                    Assert.AreEqual(rec2Dict.Count, rec2Dict.Count);
                    foreach (var key in rec1Dict.Keys)
                    {
                        var val1 = rec1Dict[key];
                        var val2 = rec2Dict[key];
                        if (val1 is ISpecificRecord)
                        {
                            AssertSpecificRecordEqual((ISpecificRecord)val1, (ISpecificRecord)val2);
                        }
                        else
                        {
                            Assert.AreEqual(val1, val2);
                        }
                    }
                }
                else
                {
                    Assert.AreEqual(rec1Val, rec2Val);
                }
            }
        }
    }

    enum EnumType
    {
        THIRD,
        FIRST,
        SECOND
    }

    class EnumRecord : ISpecificRecord
    {
        public EnumType enumType { get; set; }
        public Schema Schema
        {
            get
            {
                return Schema.Parse("{\"type\":\"record\",\"name\":\"EnumRecord\",\"namespace\":\"Avro.Test\"," + 
                                        "\"fields\":[{\"name\":\"enumType\",\"type\": { \"type\": \"enum\", \"name\":" +
                                        " \"EnumType\", \"symbols\": [\"THIRD\", \"FIRST\", \"SECOND\"]} }]}");
            }
        }

        public object Get(int fieldPos)
        {
            return enumType;
        }

        public void Put(int fieldPos, object fieldValue)
        {
            enumType = (EnumType)fieldValue;
        }
    }
}