/*
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
using System.Data.SqlTypes;
using System.Globalization;
using System.Net.Sockets;
using System.Numerics;
using Avro.Util;
using NUnit.Framework;
using NUnit.Framework.Constraints;

namespace Avro.test.Util
{
    /// <summary>
    /// This tests added to confirm standalone operation of new type UnknownLogicalType that implements LogicalType
    /// </summary>
    [TestFixture]
    class UnknownLogicalTypeTests
    {
        [TestCase(typeof(System.String), "", "{\"type\": \"string\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Boolean), true, "{\"type\": \"boolean\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Int32), Int32.MinValue, "{\"type\": \"int\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Int64), Int64.MinValue, "{\"type\": \"long\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Single), Single.MinValue, "{\"type\": \"float\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Double), Double.MinValue, "{\"type\": \"double\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Byte[]), new byte[] { }, "{\"type\": \"bytes\", \"logicalType\": \"unknown\"}")]
        public void TestConvertToBaseValue_IsTrue(Type baseType, object logicalValue, string schemaText)
        {
            var schema = (LogicalSchema)Schema.Parse(schemaText);

            var logicalType = new UnknownLogicalType(schema);

            var baseValue = logicalType.ConvertToBaseValue(logicalValue, schema);

            Assert.AreEqual(baseValue, Convert.ChangeType(logicalValue, baseType));
        }

        [TestCase(typeof(System.Byte[]), "", "{\"type\": \"string\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Double), true, "{\"type\": \"boolean\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Single), Int32.MinValue, "{\"type\": \"int\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Boolean), Int64.MinValue, "{\"type\": \"long\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Int32), Single.MinValue, "{\"type\": \"float\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Int64), Double.MinValue, "{\"type\": \"double\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.String), new byte[] { }, "{\"type\": \"bytes\", \"logicalType\": \"unknown\"}")]
        public void TestConvertToBaseValue_IsFalse(Type baseType, object logicalValue, string schemaText)
        {
            var schema = (LogicalSchema)Schema.Parse(schemaText);

            var logicalType = new UnknownLogicalType(schema);

            var baseValue = logicalType.ConvertToBaseValue(logicalValue, schema);

            Assert.AreNotEqual(baseValue.GetType(), baseType);
        }

        [TestCase(typeof(System.String), "", "{\"type\": \"string\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Boolean), true, "{\"type\": \"boolean\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Int32), Int32.MinValue, "{\"type\": \"int\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Int64), Int64.MinValue, "{\"type\": \"long\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Single), Single.MinValue, "{\"type\": \"float\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Double), Double.MinValue, "{\"type\": \"double\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Byte[]), new byte[] { }, "{\"type\": \"bytes\", \"logicalType\": \"unknown\"}")]
        public void TestConvertToLogicalValue_IsTrue(Type baseType, object logicalValue, string schemaText)
        {
            var schema = (LogicalSchema)Schema.Parse(schemaText);

            var logicalType = new UnknownLogicalType(schema);

            var baseValue = logicalType.ConvertToLogicalValue(logicalValue, schema);

            Assert.AreEqual(baseValue, Convert.ChangeType(logicalValue, baseType));
        }

        [TestCase(typeof(System.Byte[]), "", "{\"type\": \"string\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Double), true, "{\"type\": \"boolean\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Single), Int32.MinValue, "{\"type\": \"int\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Boolean), Int64.MinValue, "{\"type\": \"long\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Int32), Single.MinValue, "{\"type\": \"float\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Int64), Double.MinValue, "{\"type\": \"double\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.String), new byte[] { }, "{\"type\": \"bytes\", \"logicalType\": \"unknown\"}")]
        public void TestConvertToLogicalValue_IsFalse(Type baseType, object logicalValue, string schemaText)
        {
            var schema = (LogicalSchema)Schema.Parse(schemaText);

            var logicalType = new UnknownLogicalType(schema);

            var baseValue = logicalType.ConvertToLogicalValue(logicalValue, schema);

            Assert.AreNotEqual(baseValue.GetType(), baseType);
        }

        [TestCase(typeof(System.String), false, "{\"type\": \"string\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Boolean), false, "{\"type\": \"boolean\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Int32), false, "{\"type\": \"int\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Int64), false, "{\"type\": \"long\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Single), false, "{\"type\": \"float\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Double), false, "{\"type\": \"double\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Byte[]), false, "{\"type\": \"bytes\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.String), true, "{\"type\": \"string\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Boolean?), true, "{\"type\": \"boolean\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Int32?), true, "{\"type\": \"int\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Int64?), true, "{\"type\": \"long\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Single?), true, "{\"type\": \"float\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Double?), true, "{\"type\": \"double\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Byte?[]), true, "{\"type\": \"bytes\", \"logicalType\": \"unknown\"}")]
        public void TestGetCSharpType_IsTrue(Type type, bool isNullable, string schemaText)
        {
            var schema = (LogicalSchema)Schema.Parse(schemaText);

            var logicalType = new UnknownLogicalType(schema);

            Assert.AreEqual(logicalType.GetCSharpType(isNullable), type);
        }

        //[TestCase(typeof(System.String), true, "{\"type\": \"string\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Boolean), true, "{\"type\": \"boolean\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Int32), true, "{\"type\": \"int\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Int64), true, "{\"type\": \"long\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Single), true, "{\"type\": \"float\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Double), true, "{\"type\": \"double\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Byte[]), true, "{\"type\": \"bytes\", \"logicalType\": \"unknown\"}")]
        //[TestCase(typeof(System.String), false, "{\"type\": \"string\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Boolean?), false, "{\"type\": \"boolean\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Int32?), false, "{\"type\": \"int\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Int64?), false, "{\"type\": \"long\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Single?), false, "{\"type\": \"float\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Double?), false, "{\"type\": \"double\", \"logicalType\": \"unknown\"}")]
        [TestCase(typeof(System.Byte?[]), false, "{\"type\": \"bytes\", \"logicalType\": \"unknown\"}")]
        public void TestGetCSharpType_IsFalse(Type type, bool isNullable, string schemaText)
        {
            var schema = (LogicalSchema)Schema.Parse(schemaText);

            var logicalType = new UnknownLogicalType(schema);

            Assert.AreNotEqual(logicalType.GetCSharpType(isNullable), type);
        }

        [TestCase("", "{\"type\": \"string\", \"logicalType\": \"unknown\"}")]
        [TestCase(true, "{\"type\": \"boolean\", \"logicalType\": \"unknown\"}")]
        [TestCase(Int32.MinValue, "{\"type\": \"int\", \"logicalType\": \"unknown\"}")]
        [TestCase(Int64.MinValue, "{\"type\": \"long\", \"logicalType\": \"unknown\"}")]
        [TestCase(Single.MinValue, "{\"type\": \"float\", \"logicalType\": \"unknown\"}")]
        [TestCase(Double.MinValue, "{\"type\": \"double\", \"logicalType\": \"unknown\"}")]
        [TestCase(new byte[] { } , "{\"type\": \"bytes\", \"logicalType\": \"unknown\"}")]
        public void TestIsInstanceOfLogicalType_IsTrue(object logicalValue, string schemaText)
        {
            var schema = (LogicalSchema)Schema.Parse(schemaText);

            var logicalType = new UnknownLogicalType(schema);

            Assert.IsTrue(logicalType.IsInstanceOfLogicalType(logicalValue));
        }

        [TestCase(Int32.MinValue, "{\"type\": \"string\", \"logicalType\": \"unknown\"}")]
        [TestCase(new byte[] { }, "{\"type\": \"boolean\", \"logicalType\": \"unknown\"}")]
        [TestCase(Int64.MinValue, "{\"type\": \"int\", \"logicalType\": \"unknown\"}")]
        [TestCase(Single.MinValue, "{\"type\": \"long\", \"logicalType\": \"unknown\"}")]
        [TestCase(Double.MinValue, "{\"type\": \"float\", \"logicalType\": \"unknown\"}")]
        [TestCase(new byte[] { }, "{\"type\": \"double\", \"logicalType\": \"unknown\"}")]
        [TestCase("", "{\"type\": \"bytes\", \"logicalType\": \"unknown\"}")]
        public void TestIsInstanceOfLogicalType_IsFalse(object logicalValue, string schemaText)
        {
            var schema = (LogicalSchema)Schema.Parse(schemaText);

            var logicalType = new UnknownLogicalType(schema);

            Assert.IsFalse(logicalType.IsInstanceOfLogicalType(logicalValue));
        }

        // See also a new test in Avro.Tests.File in TestSpecificDataSource using unknowLogicalTypeSchema
    }
}
