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
    public class GenericFixedTests
    {
        private const string baseSchema = "{\"type\": \"fixed\", \"size\": 2, \"name\": \"f\"}";

        [Test]
        public void TestEquals()
        {
            GenericFixed genericFixed = GetBaseGenericFixed();
            GenericFixed genericFixed2 = GetBaseGenericFixed();

            Assert.IsTrue(genericFixed.Equals(genericFixed2));
        }

        [Test]
        public void TestEqualsNotEqual()
        {
            GenericFixed genericFixed = GetBaseGenericFixed();
            GenericFixed genericFixed2 = new TestGenericFixed((FixedSchema)Schema.Parse(baseSchema), new byte[] { 1, 3 });

            Assert.IsFalse(genericFixed.Equals(genericFixed2));
        }

        [Test]
        public void TestEqualsObject()
        {
            GenericFixed genericFixed = GetBaseGenericFixed();
            object genericFixed2 = genericFixed;

            Assert.IsTrue(genericFixed.Equals(genericFixed2));
        }

        [Test]
        public void TestEqualsObjectNullObject()
        {
            GenericFixed genericFixed = GetBaseGenericFixed();

            Assert.IsFalse(genericFixed.Equals(null));
        }

        [Test]
        public void TestInheritanceEquals()
        {
            GenericFixed genericFixed = GetBaseGenericFixed();
            TestGenericFixed testGenericEnum = new TestGenericFixed((FixedSchema)Schema.Parse(baseSchema), new byte[] { 1, 2 });

            Assert.False(genericFixed.Equals(testGenericEnum));
            Assert.False(testGenericEnum.Equals(genericFixed));
        }

        private GenericFixed GetBaseGenericFixed()
        {
            GenericFixed genericFixed = new GenericFixed((FixedSchema)Schema.Parse(baseSchema), new byte[] { 1, 2 });

            return genericFixed;
        }

        public class TestGenericFixed : GenericFixed
        {
            public TestGenericFixed(FixedSchema schema) : base(schema)
            {
            }

            public TestGenericFixed(FixedSchema schema, byte[] value) : base(schema, value)
            {
            }

            protected TestGenericFixed(uint size) : base(size)
            {
            }

            public override bool Equals(object obj)
            {
                return obj.GetType() == typeof(TestGenericFixed);
            }

            public override int GetHashCode() => base.GetHashCode();
        }
    }
}
