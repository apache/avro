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

using NUnit.Framework;

namespace Avro.test
{
    [TestFixture]
    class AvroDecimalTest
    {
        //Use strings as parameters as otherwise doubles will be used intermediately by C# and scale will be lost in this process
        [TestCase("1")]
        [TestCase("1000")]
        [TestCase("10.10")]
        [TestCase("0")]
        [TestCase("0.1")]
        [TestCase("0.01")]
        [TestCase("-1")]
        [TestCase("-1000")]
        [TestCase("-10.10")]
        [TestCase("-0.1")]
        [TestCase("-0.01")]
        public void TestAvroDecimalToString(decimal value)
        {
            var valueString = value.ToString();

            var avroDecimal = new AvroDecimal(value);
            var avroDecimalString = avroDecimal.ToString();

            Assert.AreEqual(valueString, avroDecimalString);
        }

        [Test]
        public void TestHighPrecisionAvroDecimalToString()
        {
            var value = 4.1748330066797328106875724512m; // High precision decimal value
            var valueString = value.ToString();

            var avroDecimal = new AvroDecimal(value);
            var avroDecimalString = avroDecimal.ToString();

            Assert.AreEqual(valueString, avroDecimalString);

            value = -4.1748330066797328106875724512m; // High precision decimal value
            valueString = value.ToString();

            avroDecimal = new AvroDecimal(value);
            avroDecimalString = avroDecimal.ToString();

            Assert.AreEqual(valueString, avroDecimalString);
        }

        //Use strings as parameters as otherwise doubles will be used intermediately by C# and scale will be lost in this process
        [TestCase("0", "0", 0)]
        [TestCase("1", "0", 1)]
        [TestCase("0", "1", -1)]
        [TestCase("1.0", "1.0", 0)]
        [TestCase("1.0", "1", 0)]
        [TestCase("1", "1.0", 0)]
        [TestCase("1.0", "0", 1)]
        [TestCase("0", "1.0", -1)]
        [TestCase("-0.5", "-1.0", 1)]
        [TestCase("-1.0", "-0.5", -1)]
        [TestCase("0.1", "0.01", 1)]
        [TestCase("0.01", "0.1", -1)]
        [TestCase("-0.1", "-0.01", -1)]
        [TestCase("-0.01", "-0.1", 1)]
        public void TestAvroDecimalCompareTo(decimal left, decimal right, int expectedResult)
        {
            var leftAvroDecimal = new AvroDecimal(left);
            var rightAvroDecimal = new AvroDecimal(right);

            int actualResult = leftAvroDecimal.CompareTo(rightAvroDecimal);
            Assert.AreEqual(expectedResult, actualResult);
        }
    }
}
