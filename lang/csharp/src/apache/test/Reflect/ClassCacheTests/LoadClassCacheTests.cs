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

using Avro.Reflect;
using NUnit.Framework;

namespace Avro.test.Reflect.ClassCacheTests
{
    public class LoadClassCacheTests
    {
        [TestCase]
        public void WhenLoadTypeWithComplexTypeProperty_LoadTypeAndPropertyType()
        {
            //Arrange
            var classCache = new ClassCache();

            //Act
            classCache.LoadClassCache(typeof(TestClass1), TestClass1.RootSchema);
            var type = classCache.GetClass(TestClass1.RootSchema);
            var propertyType = classCache.GetClass(TestClass1.ComplexTypeSchema);

            //Assert
            Assert.NotNull(type);
            Assert.AreEqual(typeof(TestClass1), type.GetClassType());
            Assert.NotNull(propertyType);
            Assert.AreEqual(typeof(ComplexType1), propertyType.GetClassType());
        }

        [TestCase]
        public void WhenLoadTwoTypesWithSameNameButDifferentComplexTypeProperties_LoadFourTypes()
        {
            //Arrange
            var classCache = new ClassCache();

            //Act
            classCache.LoadClassCache(typeof(TestClass1), TestClass1.RootSchema);
            var type1 = classCache.GetClass(TestClass1.RootSchema);
            var propertyType1 = classCache.GetClass(TestClass1.ComplexTypeSchema);
            classCache.LoadClassCache(typeof(TestClass2), TestClass2.RootSchema);
            var type2 = classCache.GetClass(TestClass2.RootSchema);
            var propertyType2 = classCache.GetClass(TestClass2.ComplexTypeSchema);

            //Assert
            Assert.NotNull(type1);
            Assert.AreEqual(typeof(TestClass1), type1.GetClassType());
            Assert.NotNull(propertyType1);
            Assert.AreEqual(typeof(ComplexType1), propertyType1.GetClassType());
            Assert.NotNull(type2);
            Assert.AreEqual(typeof(TestClass2), type2.GetClassType());
            Assert.NotNull(propertyType2);
            Assert.AreEqual(typeof(ComplexType2), propertyType2.GetClassType());
        }
    }

    public class TestClass1
    {
        public static RecordSchema RootSchema = (RecordSchema)Schema.Parse(@"
{
    ""type"" : ""record"",
    ""name"" : ""TestClass1"",
    ""namespace"" : ""Avro.test.Reflect.ClassCacheTests"",
    ""fields"" :
        [
            {
                ""name"" : ""ComplexType"",
                ""type"" :
                    {
                        ""type"" : ""record"",
                        ""name"" : ""ComplexType1"",
                        ""fields"" :
                            [
                                {
                                    ""name"" : ""IntProperty"",
                                    ""type"" :""int""
                                }
                            ]
                    }
            }
        ]
}
");
        public static RecordSchema ComplexTypeSchema = (RecordSchema)RootSchema.Fields[0].Schema;

        public ComplexType1 ComplexType { get; set; }
    }

    public class TestClass2
    {
        public static RecordSchema RootSchema = (RecordSchema)Schema.Parse(@"
{
    ""type"" : ""record"",
    ""name"" : ""TestClass2"",
    ""namespace"" : ""Avro.test.Reflect.ClassCacheTests"",
    ""fields"" :
        [
            {
                ""name"" : ""ComplexType"",
                ""type"" :
                    {
                        ""type"" : ""record"",
                        ""name"" : ""ComplexType2"",
                        ""fields"" :
                            [
                                {
                                    ""name"" : ""StringProperty"",
                                    ""type"" : ""string""
                                }
                            ]
                    }
            }
        ]
}
");
        public static RecordSchema ComplexTypeSchema = (RecordSchema)RootSchema.Fields[0].Schema;
        public ComplexType2 ComplexType { get; set; }
    }

    public class ComplexType1
    {
        public int IntProperty { get; set; }
    }

    public class ComplexType2
    {
        public string StringProperty { get; set; }
    }
}
