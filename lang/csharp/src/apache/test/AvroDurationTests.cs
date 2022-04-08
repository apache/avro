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

namespace Avro.Test
{
    [TestFixture]
    class AvroDurationTests
    {
        [TestCase(0, 0, 0)]
        [TestCase(0, 0, 1)]
        [TestCase(10, 0, 0)]
        public void TestAvroDurationEqual(int months, int days, int milliseconds)
        {
            AvroDuration duration1 = new AvroDuration(months, days, milliseconds);
            AvroDuration duration2 = new AvroDuration(months, days, milliseconds);

            Assert.AreEqual(months, duration1.Months);
            Assert.AreEqual(days, duration1.Days);
            Assert.AreEqual(milliseconds, duration1.Milliseconds);

            Assert.AreEqual(months, duration2.Months);
            Assert.AreEqual(days, duration2.Days);
            Assert.AreEqual(milliseconds, duration2.Milliseconds);

            Assert.AreEqual(duration1, duration2);
        }

        [TestCase(0, 0, 0, 0, 0, 1)]
        [TestCase(0, 0, 1, 0, 1, 0)]
        [TestCase(10, 0, 0, 10, 0, 1)]
        public void TestAvroDurationNotEqual(int months, int days, int milliseconds, int monthsOther, int daysOther, int millisecondsOther)
        {
            AvroDuration duration = new AvroDuration(months, days, milliseconds);
            AvroDuration durationOther = new AvroDuration(monthsOther, daysOther, millisecondsOther);

            Assert.AreNotEqual(duration, durationOther);
        }

        [TestCase(0, 0, 0, 0, 0, 1, ExpectedResult = -1)]
        [TestCase(0, 0, 1, 0, 1, 0, ExpectedResult = -1)]
        [TestCase(10, 0, 0, 10, 0, 1, ExpectedResult = -1)]
        [TestCase(0, 0, 0, 0, 0, 0, ExpectedResult = 0)]
        [TestCase(0, 0, 1, 0, 0, 1, ExpectedResult = 0)]
        [TestCase(0, 0, 2, 0, 0, 1, ExpectedResult = 1)]
        [TestCase(0, 1, 1, 0, 1, 0, ExpectedResult = 1)]
        [TestCase(1, 1, 0, 0, 1, 0, ExpectedResult = 1)]
        [TestCase(10, 0, 1, 10, 0, 0, ExpectedResult = 1)]
        public int TestAvroDurationCompareTo(int months, int days, int milliseconds, int monthsOther, int daysOther, int millisecondsOther)
        {
            return new AvroDuration(months, days, milliseconds).CompareTo(new AvroDuration(monthsOther, daysOther, millisecondsOther));
        }
    }
}
