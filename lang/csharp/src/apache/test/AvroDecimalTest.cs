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

using System;
using System.Collections.Generic;

namespace Avro.test
{
    [TestFixture]
    class AvroDecimalTest
    {
        public static IEnumerable<decimal> DecimalTestCases = new[]
        {
            1m,
            1000m,
            10.10m,
            0m,
            0.1m,
            0.01m,
            -1m,
            -1000m,
            -10.10m,
            -9.9999m,
            9.9999999999m,
            -0.1m,
            -0.01m,
        };

        public static IEnumerable<int> ScaleTestCases = new[]
        {
            0,
            1,
            2,
            10,
            5
        };

        [TestCaseSource(nameof(DecimalTestCases))]
        public void TestAvroDecimalToString(decimal value)
        {
            var valueString = value.ToString();

            var avroDecimal = new AvroDecimal(value);
            var avroDecimalString = avroDecimal.ToString();

            Assert.AreEqual(valueString, avroDecimalString);
        }

        [Test]
        public void TestAvroDecimalReScaling(
            [ValueSource(nameof(DecimalTestCases))] decimal value,
            [ValueSource(nameof(ScaleTestCases))] int scale)
        {
            var before = new AvroDecimal(value);

            // Determine if an overflow would happen by just counting the decimal places in the string representation
            var split = value.ToString("0.##########").Split('.');
            var decimalPlaces = split.Length == 1 ? 0 : split[1].Length;
            var wouldOverflow = decimalPlaces > scale;

            if (wouldOverflow)
            {
                Assert.That(() => before.ReScale(scale), Throws.Exception.InstanceOf<OverflowException>());
            }
            else
            {
                var after = before.ReScale(scale);
                Assert.AreEqual((decimal)before, (decimal)after);
            }
        }
    }
}
