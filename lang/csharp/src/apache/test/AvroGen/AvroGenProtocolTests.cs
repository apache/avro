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
using System.Collections.Generic;
using NUnit.Framework;

namespace Avro.Test.AvroGen
{
    [TestFixture]

    class AvroGenProtocolTests
    {
        private const string _baseball = @"
{
  ""protocol"" : ""Baseball"",
  ""namespace"" : ""avro.examples.baseball"",
  ""doc"" : ""Licensed to the Apache Software Foundation (ASF) under one\nor more contributor license agreements.  See the NOTICE file\ndistributed with this work for additional information\nregarding copyright ownership.  The ASF licenses this file\nto you under the Apache License, Version 2.0 (the\n\""License\""); you may not use this file except in compliance\nwith the License.  You may obtain a copy of the License at\n\n    https://www.apache.org/licenses/LICENSE-2.0\n\nUnless required by applicable law or agreed to in writing, software\ndistributed under the License is distributed on an \""AS IS\"" BASIS,\nWITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\nSee the License for the specific language governing permissions and\nlimitations under the License."",
  ""types"" : [ {
    ""type"" : ""enum"",
    ""name"" : ""Position"",
    ""symbols"" : [ ""P"", ""C"", ""B1"", ""B2"", ""B3"", ""SS"", ""LF"", ""CF"", ""RF"", ""DH"" ]
  }, {
    ""type"" : ""record"",
    ""name"" : ""Player"",
    ""fields"" : [ {
      ""name"" : ""number"",
      ""type"" : ""int""
    }, {
      ""name"" : ""first_name"",
      ""type"" : ""string""
    }, {
      ""name"" : ""last_name"",
      ""type"" : ""string""
    }, {
      ""name"" : ""position"",
      ""type"" : {
        ""type"" : ""array"",
        ""items"" : ""Position""
      }
    } ]
  } ],
  ""messages"" : {
  }
}
";
        private const string _comments = @"
{
  ""protocol"" : ""Comments"",
  ""namespace"" : ""testing"",
  ""types"" : [ {
    ""type"" : ""enum"",
    ""name"" : ""DocumentedEnum"",
    ""doc"" : ""Documented Enum"",
    ""symbols"" : [ ""A"", ""B"", ""C"" ],
    ""default"" : ""A""
  }, {
    ""type"" : ""enum"",
    ""name"" : ""UndocumentedEnum"",
    ""symbols"" : [ ""D"", ""E"" ]
  }, {
    ""type"" : ""fixed"",
    ""name"" : ""DocumentedFixed"",
    ""doc"" : ""Documented Fixed Type"",
    ""size"" : 16
  }, {
    ""type"" : ""fixed"",
    ""name"" : ""UndocumentedFixed"",
    ""size"" : 16
  }, {
    ""type"" : ""error"",
    ""name"" : ""DocumentedError"",
    ""doc"" : ""Documented Error"",
    ""fields"" : [ {
      ""name"" : ""reason"",
      ""type"" : ""string"",
      ""doc"" : ""Documented Reason Field""
    }, {
      ""name"" : ""explanation"",
      ""type"" : ""string"",
      ""doc"" : ""Default Doc Explanation Field""
    } ]
  }, {
    ""type"" : ""record"",
    ""name"" : ""UndocumentedRecord"",
    ""fields"" : [ {
      ""name"" : ""description"",
      ""type"" : ""string""
    } ]
  } ],
  ""messages"" : {
    ""documentedMethod"" : {
      ""doc"" : ""Documented Method"",
      ""request"" : [ {
        ""name"" : ""message"",
        ""type"" : ""string"",
        ""doc"" : ""Documented Parameter""
      }, {
        ""name"" : ""defMsg"",
        ""type"" : ""string"",
        ""doc"" : ""Default Documented Parameter""
      } ],
      ""response"" : ""null"",
      ""errors"" : [ ""DocumentedError"" ]
    },
    ""undocumentedMethod"" : {
      ""request"" : [ {
        ""name"" : ""message"",
        ""type"" : ""string""
      } ],
      ""response"" : ""null""
    }
  }
}
";

        private const string _interop = @"
{
  ""protocol"" : ""InteropProtocol"",
  ""namespace"" : ""org.apache.avro.interop"",
  ""doc"" : ""Licensed to the Apache Software Foundation (ASF) under one\nor more contributor license agreements.  See the NOTICE file\ndistributed with this work for additional information\nregarding copyright ownership.  The ASF licenses this file\nto you under the Apache License, Version 2.0 (the\n\""License\""); you may not use this file except in compliance\nwith the License.  You may obtain a copy of the License at\n\n    https://www.apache.org/licenses/LICENSE-2.0\n\nUnless required by applicable law or agreed to in writing, software\ndistributed under the License is distributed on an \""AS IS\"" BASIS,\nWITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\nSee the License for the specific language governing permissions and\nlimitations under the License."",
  ""types"" : [ {
    ""type"" : ""record"",
    ""name"" : ""Label"",
    ""fields"" : [ {
      ""name"" : ""label"",
      ""type"" : ""string""
    } ]
  }, {
    ""type"" : ""enum"",
    ""name"" : ""Kind"",
    ""symbols"" : [ ""A"", ""B"", ""C"" ]
  }, {
    ""type"" : ""fixed"",
    ""name"" : ""MD5"",
    ""size"" : 16
  }, {
    ""type"" : ""record"",
    ""name"" : ""Node"",
    ""fields"" : [ {
      ""name"" : ""label"",
      ""type"" : ""string""
    }, {
      ""name"" : ""children"",
      ""type"" : {
        ""type"" : ""array"",
        ""items"" : ""Node""
      },
      ""default"" : [ ]
    } ]
  }, {
    ""type"" : ""record"",
    ""name"" : ""Interop"",
    ""fields"" : [ {
      ""name"" : ""intField"",
      ""type"" : ""int"",
      ""default"" : 1
    }, {
      ""name"" : ""longField"",
      ""type"" : ""long"",
      ""default"" : -1
    }, {
      ""name"" : ""stringField"",
      ""type"" : ""string""
    }, {
      ""name"" : ""boolField"",
      ""type"" : ""boolean"",
      ""default"" : false
    }, {
      ""name"" : ""floatField"",
      ""type"" : ""float"",
      ""default"" : 0.0
    }, {
      ""name"" : ""doubleField"",
      ""type"" : ""double"",
      ""default"" : -1.0E12
    }, {
      ""name"" : ""nullField"",
      ""type"" : ""null""
    }, {
      ""name"" : ""arrayField"",
      ""type"" : {
        ""type"" : ""array"",
        ""items"" : ""double""
      },
      ""default"" : [ ]
    }, {
      ""name"" : ""mapField"",
      ""type"" : {
        ""type"" : ""map"",
        ""values"" : ""Label""
      }
    }, {
      ""name"" : ""unionField"",
      ""type"" : [ ""boolean"", ""double"", {
        ""type"" : ""array"",
        ""items"" : ""bytes""
      } ]
    }, {
      ""name"" : ""enumField"",
      ""type"" : ""Kind""
    }, {
      ""name"" : ""fixedField"",
      ""type"" : ""MD5""
    }, {
      ""name"" : ""recordField"",
      ""type"" : ""Node""
    } ]
  } ],
  ""messages"" : { }
}
";
        private const string _namespaces = @"
{
  ""protocol"" : ""TestNamespace"",
  ""namespace"" : ""avro.test.protocol"",
  ""doc"" : ""Licensed to the Apache Software Foundation (ASF) under one\nor more contributor license agreements.  See the NOTICE file\ndistributed with this work for additional information\nregarding copyright ownership.  The ASF licenses this file\nto you under the Apache License, Version 2.0 (the\n\""License\""); you may not use this file except in compliance\nwith the License.  You may obtain a copy of the License at\n\n    https://www.apache.org/licenses/LICENSE-2.0\n\nUnless required by applicable law or agreed to in writing, software\ndistributed under the License is distributed on an \""AS IS\"" BASIS,\nWITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\nSee the License for the specific language governing permissions and\nlimitations under the License."",
  ""types"" : [ {
    ""type"" : ""fixed"",
    ""name"" : ""FixedInOtherNamespace"",
    ""namespace"" : ""avro.test.fixed"",
    ""size"" : 16
  }, {
    ""type"" : ""fixed"",
    ""name"" : ""FixedInThisNamespace"",
    ""size"" : 16
  }, {
    ""type"" : ""record"",
    ""name"" : ""RecordInOtherNamespace"",
    ""namespace"" : ""avro.test.record"",
    ""fields"" : [ ]
  }, {
    ""type"" : ""error"",
    ""name"" : ""ErrorInOtherNamespace"",
    ""namespace"" : ""avro.test.error"",
    ""fields"" : [ ]
  }, {
    ""type"" : ""enum"",
    ""name"" : ""EnumInOtherNamespace"",
    ""namespace"" : ""avro.test.enum"",
    ""symbols"" : [ ""FOO"" ]
  }, {
    ""type"" : ""record"",
    ""name"" : ""RefersToOthers"",
    ""fields"" : [ {
      ""name"" : ""someFixed"",
      ""type"" : ""avro.test.fixed.FixedInOtherNamespace""
    }, {
      ""name"" : ""someRecord"",
      ""type"" : ""avro.test.record.RecordInOtherNamespace""
    }, {
      ""name"" : ""someError"",
      ""type"" : ""avro.test.error.ErrorInOtherNamespace""
    }, {
      ""name"" : ""someEnum"",
      ""type"" : ""avro.test.enum.EnumInOtherNamespace""
    }, {
      ""name"" : ""thisFixed"",
      ""type"" : ""FixedInThisNamespace""
    } ]
  } ],
  ""messages"" : {
  }
}
";
        private const string _forwardRef = @"
{
  ""protocol"": ""Import"",
  ""namespace"": ""org.foo"",
  ""types"": [
    {
      ""type"": ""record"",
      ""name"": ""ANameValue"",
      ""fields"": [
        { ""name"":""name"", ""type"": ""string"", ""doc"":""the name"" },
        { ""name"": ""value"", ""type"": ""string"", ""doc"": ""the value"" },
        { ""name"": ""type"", ""type"": { ""type"": ""enum"", ""name"":""ValueType"", ""symbols"": [""JSON"",""BASE64BIN"",""PLAIN""] }, ""default"": ""PLAIN"" }
      ]
    }
  ],
  ""messages"":  { }
}
";
        private const string _unicode = @"
{
  ""protocol"" : ""Протоколы"",
  ""namespace"" : ""org.avro.test"",
  ""doc"" : ""This is a test that UTF8 functions correctly.\nこのテストでは、UTF - 8で正しく機能している。\n这是一个测试，UTF - 8的正常运行。"",
  ""types"" : [ {
    ""type"" : ""record"",
    ""name"" : ""Структура"",
    ""fields"" : [ {
      ""name"" : ""Строковый"",
      ""type"" : ""string""
    }, {
      ""name"" : ""文字列"",
      ""type"" : ""string""
    } ]
  } ],
  ""messages"" : {
  }
}
";

        private const string _myProtocol = @"
{
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
}";

        [TestCase(
            _baseball,
            new string[]
            {
                "avro.examples.baseball.Baseball",
                "avro.examples.baseball.BaseballCallback",
                "avro.examples.baseball.Player",
                "avro.examples.baseball.Position"
            },
            new string[]
            {
                "avro/examples/baseball/Baseball.cs",
                "avro/examples/baseball/BaseballCallback.cs",
                "avro/examples/baseball/Player.cs",
                "avro/examples/baseball/Position.cs"
            })]
        [TestCase(
            _comments,
            new string[]
            {
                "testing.Comments",
                "testing.CommentsCallback",
                "testing.DocumentedEnum",
                "testing.DocumentedError",
                "testing.DocumentedFixed",
                "testing.UndocumentedEnum",
                "testing.UndocumentedFixed",
                "testing.UndocumentedRecord"
            },
            new string[]
            {
                "testing/Comments.cs",
                "testing/CommentsCallback.cs",
                "testing/DocumentedEnum.cs",
                "testing/DocumentedError.cs",
                "testing/DocumentedFixed.cs",
                "testing/UndocumentedEnum.cs",
                "testing/UndocumentedFixed.cs",
                "testing/UndocumentedRecord.cs"
            })]
        [TestCase(
            _interop,
            new string[]
            {
                "org.apache.avro.interop.Label",
                "org.apache.avro.interop.Interop",
                "org.apache.avro.interop.InteropProtocol",
                "org.apache.avro.interop.InteropProtocolCallback",
                "org.apache.avro.interop.Kind",
                "org.apache.avro.interop.MD5",
                "org.apache.avro.interop.Node",
            },
            new string[]
            {
                "org/apache/avro/interop/Label.cs",
                "org/apache/avro/interop/Interop.cs",
                "org/apache/avro/interop/InteropProtocol.cs",
                "org/apache/avro/interop/InteropProtocolCallback.cs",
                "org/apache/avro/interop/Kind.cs",
                "org/apache/avro/interop/MD5.cs",
                "org/apache/avro/interop/Node.cs",
            })]
        [TestCase(
            _namespaces,
            new string[]
            {
                "avro.test.enum.EnumInOtherNamespace",
                "avro.test.error.ErrorInOtherNamespace",
                "avro.test.fixed.FixedInOtherNamespace",
                "avro.test.protocol.FixedInThisNamespace",
                "avro.test.protocol.RefersToOthers",
                "avro.test.protocol.TestNamespace",
                "avro.test.protocol.TestNamespaceCallback",
                "avro.test.record.RecordInOtherNamespace"
            },
            new string[]
            {
                "avro/test/enum/EnumInOtherNamespace.cs",
                "avro/test/error/ErrorInOtherNamespace.cs",
                "avro/test/fixed/FixedInOtherNamespace.cs",
                "avro/test/protocol/FixedInThisNamespace.cs",
                "avro/test/protocol/RefersToOthers.cs",
                "avro/test/protocol/TestNamespace.cs",
                "avro/test/protocol/TestNamespaceCallback.cs",
                "avro/test/record/RecordInOtherNamespace.cs"
            })]
        [TestCase(
            _forwardRef,
            new string[]
            {
                "org.foo.ANameValue",
                "org.foo.Import",
                "org.foo.ImportCallback",
                "org.foo.ValueType"
            },
            new string[]
            {
                "org/foo/ANameValue.cs",
                "org/foo/Import.cs",
                "org/foo/ImportCallback.cs",
                "org/foo/ValueType.cs"
            })]
        [TestCase(
            _unicode,
            new string[]
            {
                "org.avro.test.Протоколы",
                "org.avro.test.ПротоколыCallback",
                "org.avro.test.Структура"
            },
            new string[]
            {
                "org/avro/test/Протоколы.cs",
                "org/avro/test/ПротоколыCallback.cs",
                "org/avro/test/Структура.cs"
            })]
        [TestCase(
            _myProtocol,
            new string[]
            {
                "com.foo.A",
                "com.foo.MyEnum",
                "com.foo.MyFixed",
                "com.foo.MyProtocol",
                "com.foo.MyProtocolCallback",
                "com.foo.newRec",
                "com.foo.Z"
            },
            new string[]
            {
                "com/foo/A.cs",
                "com/foo/MyEnum.cs",
                "com/foo/MyFixed.cs",
                "com/foo/MyProtocol.cs",
                "com/foo/MyProtocolCallback.cs",
                "com/foo/newRec.cs",
                "com/foo/Z.cs"
            })]
        public void GenerateProtocol(string protocol, IEnumerable<string> typeNamesToCheck, IEnumerable<string> generatedFilesToCheck)
        {
            AvroGenHelper.TestProtocol(protocol, typeNamesToCheck, generatedFilesToCheck: generatedFilesToCheck);
        }
    }
}
