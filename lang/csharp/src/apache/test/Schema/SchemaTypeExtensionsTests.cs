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

using NUnit.Framework;

namespace Avro.test
{
    [TestFixture]
    public class SchemaTypeExtensionsTests
    {
        [TestCase("Null", Schema.Type.Null)]
        [TestCase("null", Schema.Type.Null)]
        [TestCase("Boolean", Schema.Type.Boolean)]
        [TestCase("boolean", Schema.Type.Boolean)]
        [TestCase("Int", Schema.Type.Int)]
        [TestCase("int", Schema.Type.Int)]
        [TestCase("Long", Schema.Type.Long)]
        [TestCase("long", Schema.Type.Long)]
        [TestCase("Float", Schema.Type.Float)]
        [TestCase("float", Schema.Type.Float)]
        [TestCase("Double", Schema.Type.Double)]
        [TestCase("double", Schema.Type.Double)]
        [TestCase("Bytes", Schema.Type.Bytes)]
        [TestCase("bytes", Schema.Type.Bytes)]
        [TestCase("String", Schema.Type.String)]
        [TestCase("string", Schema.Type.String)]
        [TestCase("Record", Schema.Type.Record)]
        [TestCase("record", Schema.Type.Record)]
        [TestCase("Enumeration", Schema.Type.Enumeration)]
        [TestCase("enumeration", Schema.Type.Enumeration)]
        [TestCase("Array", Schema.Type.Array)]
        [TestCase("array", Schema.Type.Array)]
        [TestCase("Map", Schema.Type.Map)]
        [TestCase("map", Schema.Type.Map)]
        [TestCase("Union", Schema.Type.Union)]
        [TestCase("union", Schema.Type.Union)]
        [TestCase("Fixed", Schema.Type.Fixed)]
        [TestCase("fixed", Schema.Type.Fixed)]
        [TestCase("Error", Schema.Type.Error)]
        [TestCase("error", Schema.Type.Error)]
        [TestCase("Logical", Schema.Type.Logical)]
        [TestCase("logical", Schema.Type.Logical)]
        [TestCase("InvalidValue", null)]
        [TestCase(null, null)]
        public void ToSchemaTypeTest(string value, object expectedResult)
        {
            if (expectedResult is Schema.Type expectedType)
            {
                Assert.AreEqual(value.ToSchemaType(), expectedType);
            }
            else
            {
                Assert.AreEqual(value.ToSchemaType(), expectedResult);
            }
        }

        [TestCase("\"Null\"", Schema.Type.Null)]
        [TestCase("\"null\"", Schema.Type.Null)]
        [TestCase("\"\"", null)]
        public void ToSchemaTypeRemoveQuotesTest(string value, object expectedResult)
        {
            if (expectedResult is Schema.Type expectedType)
            {
                Assert.AreEqual(value.ToSchemaType(true), expectedType);
            }
            else
            {
                Assert.AreEqual(value.ToSchemaType(true), expectedResult);
            }
        }
    }
}
