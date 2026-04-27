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

using System.Globalization;
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
        public void TestAvroDecimalToString(string value)
        {
            var valueDecimal = decimal.Parse(value, CultureInfo.InvariantCulture);
            var valueString = valueDecimal.ToString();

            var avroDecimal = new AvroDecimal(valueDecimal);
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
        [TestCase("0", "0", ExpectedResult = 0)]
        [TestCase("1", "0", ExpectedResult = 1)]
        [TestCase("0", "1", ExpectedResult = -1)]
        [TestCase("1.0", "1.0", ExpectedResult = 0)]
        [TestCase("1.0", "1", ExpectedResult = 0)]
        [TestCase("1", "1.0", ExpectedResult = 0)]
        [TestCase("1.0", "0", ExpectedResult = 1)]
        [TestCase("0", "1.0", ExpectedResult = -1)]
        [TestCase("-0.5", "-1.0", ExpectedResult = 1)]
        [TestCase("-1.0", "-0.5", ExpectedResult = -1)]
        [TestCase("0.1", "0.01", ExpectedResult = 1)]
        [TestCase("0.01", "0.1", ExpectedResult = -1)]
        [TestCase("-0.1", "-0.01", ExpectedResult = -1)]
        [TestCase("-0.01", "-0.1", ExpectedResult = 1)]
        public int TestAvroDecimalCompareTo(string left, string right)
        {
            var leftDecimal = decimal.Parse(left, CultureInfo.InvariantCulture);
            var rightDecimal = decimal.Parse(right, CultureInfo.InvariantCulture);
            var leftAvroDecimal = new AvroDecimal(leftDecimal);
            var rightAvroDecimal = new AvroDecimal(rightDecimal);

            return leftAvroDecimal.CompareTo(rightAvroDecimal);
        }
    }
}
