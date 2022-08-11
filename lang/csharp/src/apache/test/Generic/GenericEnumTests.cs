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

using Avro.Generic;
using NUnit.Framework;

namespace Avro.test.Generic
{
    [TestFixture]
    public class GenericEnumTests
    {
        private const string baseSchema = "{\"type\": \"enum\", \"name\": \"Test\", \"symbols\": " +
            "[\"Unknown\", \"A\", \"B\"], \"default\": \"Unknown\" }";

        [Test]
        public void TestEquals()
        {
            GenericEnum genericEnum = GetBaseGenericEnum();
            GenericEnum genericEnum2 = GetBaseGenericEnum();

            Assert.IsTrue(genericEnum.Equals(genericEnum2));
        }

        [Test]
        public void TestEqualsNotEqual()
        {
            GenericEnum genericEnum = GetBaseGenericEnum();
            GenericEnum genericEnum2 = new GenericEnum(Schema.Parse(baseSchema) as EnumSchema, "B");

            Assert.IsFalse(genericEnum.Equals(genericEnum2));
        }

        [Test]
        public void TestEqualsObject()
        {
            GenericEnum genericEnum = GetBaseGenericEnum();
            object genericEnum2 = genericEnum;

            Assert.IsTrue(genericEnum.Equals(genericEnum2));
        }

        [Test]
        public void TestEqualsObjectNullObject()
        {
            GenericEnum genericEnum = GetBaseGenericEnum();

            Assert.IsFalse(genericEnum.Equals(null));
        }

        private GenericEnum GetBaseGenericEnum()
        {
            GenericEnum genericEnum = new GenericEnum(Schema.Parse(baseSchema) as EnumSchema, "A");

            return genericEnum;
        }
    }
}
