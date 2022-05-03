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
using System.Globalization;
using System.Numerics;
using Avro.Util;
using NUnit.Framework;

namespace Avro.Test
{
    [TestFixture]
    class LogicalTypeTests
    {
        [TestCase("0", 0, new byte[] { 0 })]
        [TestCase("1.01", 2, new byte[] { 101 })]
        [TestCase("123456789123456789.56", 2, new byte[] { 0, 171, 84, 169, 143, 129, 101, 36, 108 })]
        [TestCase("1234", 0, new byte[] { 4, 210 })]
        [TestCase("1234.5", 1, new byte[] { 48, 57 })]
        [TestCase("1234.56", 2, new byte[] { 1, 226, 64 })]
        [TestCase("-0", 0, new byte[] { 0 })]
        [TestCase("-1.01", 2, new byte[] { 155 })]
        [TestCase("-123456789123456789.56", 2, new byte[] { 255, 84, 171, 86, 112, 126, 154, 219, 148 })]
        [TestCase("-1234", 0, new byte[] { 251, 46 })]
        [TestCase("-1234.5", 1, new byte[] { 207, 199 })]
        [TestCase("-1234.56", 2, new byte[] { 254, 29, 192 })]
        // This tests ensures that changes to Decimal.ConvertToBaseValue and ConvertToLogicalValue can be validated (bytes)
        public void TestDecimalConvert(string s, int scale, byte[] converted)
        {
            var schema = (LogicalSchema)Schema.Parse(@$"{{""type"": ""bytes"", ""logicalType"": ""decimal"", ""precision"": 4, ""scale"": {scale}}}");

            var avroDecimal = new Avro.Util.Decimal();
            // CultureInfo.InvariantCulture ensures that "." is always accepted as the decimal point
            var decimalVal = (AvroDecimal)decimal.Parse(s, CultureInfo.InvariantCulture);

            // TestDecimal tests ConvertToLogicalValue(ConvertToBaseValue(...)) which might hide symmetrical breaking changes in both functions
            // The following 2 tests are checking the conversions separately

            // Validate Decimal.ConvertToBaseValue
            Assert.AreEqual(converted, avroDecimal.ConvertToBaseValue(decimalVal, schema));

            // Validate Decimal.ConvertToLogicalValue
            Assert.AreEqual(decimalVal, (AvroDecimal)avroDecimal.ConvertToLogicalValue(converted, schema));
        }

        [Test]
        public void TestDecimal(
            [Values(
                "1234.56",
                "-1234.56",
                "123456789123456789.56",
                "-123456789123456789.56",
                "000000000000000001.01",
                "-000000000000000001.01"
            )] string s, 
            [Values(
                "\"bytes\"",
                "{\"type\": \"fixed\", \"size\": 16, \"name\": \"n\"}"
            )] string baseType)
        {
            var schema = (LogicalSchema)Schema.Parse($"{{\"type\": {baseType}, \"logicalType\": \"decimal\", \"precision\": 4, \"scale\": 2 }}");

            var avroDecimal = new Avro.Util.Decimal();
            // CultureInfo.InvariantCulture ensures that "." is always accepted as the decimal point
            var decimalVal = (AvroDecimal)decimal.Parse(s, CultureInfo.InvariantCulture);

            var convertedDecimalVal = (AvroDecimal)avroDecimal.ConvertToLogicalValue(avroDecimal.ConvertToBaseValue(decimalVal, schema), schema);

            Assert.AreEqual(decimalVal, convertedDecimalVal);
        }

        [Test]
        public void TestDecimalScale(
            [Values(
                "0",
                "1",
                "-1",
                "1234567891234567890123456789",
                "-1234567891234567890123456789",
                "0000000000000000000000000001",
                "-0000000000000000000000000001"
            )] string s, 
            [Values(1, 2, 3, 4, 5, 6, 7, 8)] int scale,
            [Values(
                "\"bytes\"",
                "{\"type\": \"fixed\", \"size\": 16, \"name\": \"n\"}"
            )] string baseType)
        {
            var schema = (LogicalSchema)Schema.Parse($"{{\"type\": {baseType}, \"logicalType\": \"decimal\", \"precision\": 8, \"scale\": {scale} }}");

            var avroDecimal = new Avro.Util.Decimal();
            var decimalVal = new AvroDecimal(BigInteger.Parse(s), scale);

            var convertedDecimalVal = (AvroDecimal)avroDecimal.ConvertToLogicalValue(avroDecimal.ConvertToBaseValue(decimalVal, schema), schema);

            Assert.AreEqual(decimalVal, convertedDecimalVal);
        }

        [TestCase("\"bytes\"")]
        [TestCase("{\"type\": \"fixed\", \"size\": 16, \"name\": \"n\"}")]
        public void TestDecimalMinMax(string baseType)
        {
            var schema = (LogicalSchema)Schema.Parse($"{{\"type\": {baseType}, \"logicalType\": \"decimal\", \"precision\": 4, \"scale\": 0 }}");

            var avroDecimal = new Avro.Util.Decimal();

            foreach (var decimalVal in new AvroDecimal[] { decimal.MinValue, decimal.MaxValue })
            {
                var convertedDecimalVal = (AvroDecimal)avroDecimal.ConvertToLogicalValue(avroDecimal.ConvertToBaseValue(decimalVal, schema), schema);

                Assert.AreEqual(decimalVal, convertedDecimalVal);
            }
        }

        [TestCase("\"bytes\"")]
        [TestCase("{\"type\": \"fixed\", \"size\": 16, \"name\": \"n\"}")]
        public void TestDecimalOutOfRangeException(string baseType)
        {
            var schema = (LogicalSchema)Schema.Parse($"{{\"type\": {baseType}, \"logicalType\": \"decimal\", \"precision\": 4, \"scale\": 2 }}");

            var avroDecimal = new Avro.Util.Decimal();
            var decimalVal = (AvroDecimal)1234.567M; // scale of 3 should throw ArgumentOutOfRangeException

            Assert.Throws<ArgumentOutOfRangeException>(() => avroDecimal.ConvertToBaseValue(decimalVal, schema));
        }

        [TestCase("01/01/2019")]
        [TestCase("05/05/2019")]
        [TestCase("05/05/2019 00:00:00Z")]
        [TestCase("05/05/2019 01:00:00Z")]
        [TestCase("05/05/2019 01:00:00+01:00")]
        [TestCase("05/05/2019 01:00:00.1Z")]
        [TestCase("05/05/2019 01:00:00.01Z")]
        [TestCase("05/05/2019 01:00:00.001Z")]
        [TestCase("05/05/2019 01:00:00.0001Z")]
        [TestCase("05/05/2019 01:00:00.00001Z")]
        [TestCase("05/05/2019 01:00:00.000001Z")]
        [TestCase("05/05/2019 01:00:00.0000001Z")]
        [TestCase("05/05/2019 01:00:00.00000001Z")]
        public void TestDate(string s)
        {
            var schema = (LogicalSchema)Schema.Parse("{\"type\": \"int\", \"logicalType\": \"date\"}");

            var date = DateTime.Parse(s, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.RoundtripKind);

            if (date.Kind != DateTimeKind.Utc)
            {
                date = DateTime.Parse(s, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.AssumeLocal);
            }

            var avroDate = new Date();

            var convertedDate = (DateTime)avroDate.ConvertToLogicalValue(avroDate.ConvertToBaseValue(date, schema), schema);

            Assert.AreEqual(new TimeSpan(0, 0, 0), convertedDate.TimeOfDay); // the time should always be 00:00:00
            Assert.AreEqual(date.Date, convertedDate.Date);
        }

        [TestCase("01/01/2019 14:20:00Z", "01/01/2019 14:20:00Z")]
        [TestCase("01/01/2019 14:20:00", "01/01/2019 14:20:00Z")]
        [TestCase("05/05/2019 14:20:00Z", "05/05/2019 14:20:00Z")]
        [TestCase("05/05/2019 14:20:00+01:00", "05/05/2019 13:20:00Z")]
        [TestCase("05/05/2019 00:00:00Z", "05/05/2019 00:00:00Z")]
        [TestCase("05/05/2019 00:00:00+01:00", "05/04/2019 23:00:00Z")] // adjusted to UTC
        [TestCase("01/01/2019 14:20:00.1Z", "01/01/2019 14:20:00.1Z")]
        [TestCase("01/01/2019 14:20:00.01Z", "01/01/2019 14:20:00.01Z")]
        [TestCase("01/01/2019 14:20:00.001Z", "01/01/2019 14:20:00.001Z")]
        [TestCase("01/01/2019 14:20:00.0001Z", "01/01/2019 14:20:00Z")]
        [TestCase("01/01/2019 14:20:00.0009Z", "01/01/2019 14:20:00Z")] // there is no rounding up
        [TestCase("01/01/2019 14:20:00.0019Z", "01/01/2019 14:20:00.001Z")] // there is no rounding up
        public void TestTimestampMillisecond(string s, string e)
        {
            var schema = (LogicalSchema)Schema.Parse("{\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}");

            var date = DateTime.Parse(s, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.RoundtripKind);

            if (date.Kind != DateTimeKind.Utc)
            {
                date = DateTime.Parse(s, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.AssumeUniversal);
            }

            var expectedDate = DateTime.Parse(e, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.RoundtripKind);

            var avroTimestampMilli = new TimestampMillisecond();
            var convertedDate = (DateTime)avroTimestampMilli.ConvertToLogicalValue(avroTimestampMilli.ConvertToBaseValue(date, schema), schema);
            Assert.AreEqual(expectedDate, convertedDate);
            Assert.AreEqual(DateTimeKind.Utc, convertedDate.Kind);
        }

        [TestCase("01/01/2019 14:20:00Z", "01/01/2019 14:20:00Z")]
        [TestCase("01/01/2019 14:20:00", "01/01/2019 14:20:00Z")]
        [TestCase("05/05/2019 14:20:00Z", "05/05/2019 14:20:00Z")]
        [TestCase("05/05/2019 14:20:00+01:00", "05/05/2019 13:20:00Z")]
        [TestCase("05/05/2019 00:00:00Z", "05/05/2019 00:00:00Z")]
        [TestCase("05/05/2019 00:00:00+01:00", "05/04/2019 23:00:00Z")] // adjusted to UTC
        [TestCase("01/01/2019 14:20:00.1Z", "01/01/2019 14:20:00.1Z")]
        [TestCase("01/01/2019 14:20:00.01Z", "01/01/2019 14:20:00.01Z")]
        [TestCase("01/01/2019 14:20:00.001Z", "01/01/2019 14:20:00.001Z")]
        [TestCase("01/01/2019 14:20:00.0001Z", "01/01/2019 14:20:00.0001Z")]
        [TestCase("01/01/2019 14:20:00.00001Z", "01/01/2019 14:20:00.00001Z")]
        [TestCase("01/01/2019 14:20:00.000001Z", "01/01/2019 14:20:00.000001Z")]
        [TestCase("01/01/2019 14:20:00.0000001Z", "01/01/2019 14:20:00Z")]
        [TestCase("01/01/2019 14:20:00.0000009Z", "01/01/2019 14:20:00Z")] // there is no rounding up
        [TestCase("01/01/2019 14:20:00.0000019Z", "01/01/2019 14:20:00.000001Z")] // there is no rounding up
        public void TestTimestampMicrosecond(string s, string e)
        {
            var schema = (LogicalSchema)Schema.Parse("{\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}");

            var date = DateTime.Parse(s, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.RoundtripKind);

            if (date.Kind != DateTimeKind.Utc)
            {
                date = DateTime.Parse(s, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.AssumeUniversal);
            }

            var expectedDate = DateTime.Parse(e, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.RoundtripKind);

            var avroTimestampMicro = new TimestampMicrosecond();
            var convertedDate = (DateTime)avroTimestampMicro.ConvertToLogicalValue(avroTimestampMicro.ConvertToBaseValue(date, schema), schema);
            Assert.AreEqual(expectedDate, convertedDate);
            Assert.AreEqual(DateTimeKind.Utc, convertedDate.Kind);
        }

        [TestCase("01/01/2019 14:20:00", "01/01/2019 14:20:00")]
        [TestCase("05/05/2019 14:20:00", "05/05/2019 14:20:00")]
        [TestCase("05/05/2019 00:00:00", "05/05/2019 00:00:00")]
        [TestCase("01/01/2019 14:20:00.1", "01/01/2019 14:20:00.1")]
        [TestCase("01/01/2019 14:20:00.01", "01/01/2019 14:20:00.01")]
        [TestCase("01/01/2019 14:20:00.001", "01/01/2019 14:20:00.001")]
        [TestCase("01/01/2019 14:20:00.0001", "01/01/2019 14:20:00")]
        [TestCase("01/01/2019 14:20:00.0009", "01/01/2019 14:20:00")] // there is no rounding up
        [TestCase("01/01/2019 14:20:00.0019", "01/01/2019 14:20:00.001")] // there is no rounding up
        [TestCase("01/01/2019 14:20:00Z", "01/01/2019 14:20:00Z")] // UTC timestamps, but will check will in local TZ
        [TestCase("01/01/2019 14:20:00.1Z", "01/01/2019 14:20:00.1Z")]
        [TestCase("01/01/2019 14:20:00.01Z", "01/01/2019 14:20:00.01Z")]
        [TestCase("01/01/2019 14:20:00.001Z", "01/01/2019 14:20:00.001Z")]
        public void TestLocalTimestampMillisecond(string s, string e)
        {
            var schema = (LogicalSchema)Schema.Parse("{\"type\": \"long\", \"logicalType\": \"local-timestamp-millis\"}");

            var date = DateTime.Parse(s, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.RoundtripKind);

            if (date.Kind != DateTimeKind.Utc)
            {
                date = DateTime.Parse(s, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.AssumeLocal);
            }

            var expectedDate = DateTime.Parse(e, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.RoundtripKind);

            if (expectedDate.Kind != DateTimeKind.Utc)
            {
                expectedDate = DateTime.Parse(e, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.AssumeLocal);
            }

            expectedDate = expectedDate.ToLocalTime();

            var avroLocalTimestampMilli = new LocalTimestampMillisecond();
            var convertedDate = (DateTime)avroLocalTimestampMilli.ConvertToLogicalValue(avroLocalTimestampMilli.ConvertToBaseValue(date, schema), schema);
            Assert.AreEqual(expectedDate, convertedDate);
            Assert.AreEqual(DateTimeKind.Local, convertedDate.Kind);
        }

        [TestCase("01/01/2019 14:20:00", "01/01/2019 14:20:00")]
        [TestCase("05/05/2019 14:20:00", "05/05/2019 14:20:00")]
        [TestCase("05/05/2019 00:00:00", "05/05/2019 00:00:00")]
        [TestCase("01/01/2019 14:20:00.1", "01/01/2019 14:20:00.1")]
        [TestCase("01/01/2019 14:20:00.01", "01/01/2019 14:20:00.01")]
        [TestCase("01/01/2019 14:20:00.001", "01/01/2019 14:20:00.001")]
        [TestCase("01/01/2019 14:20:00.0001", "01/01/2019 14:20:00.0001")]
        [TestCase("01/01/2019 14:20:00.00001", "01/01/2019 14:20:00.00001")]
        [TestCase("01/01/2019 14:20:00.000001", "01/01/2019 14:20:00.000001")]
        [TestCase("01/01/2019 14:20:00.0000001", "01/01/2019 14:20:00")]
        [TestCase("01/01/2019 14:20:00.0000009", "01/01/2019 14:20:00")] // there is no rounding up
        [TestCase("01/01/2019 14:20:00.0000019", "01/01/2019 14:20:00.000001")] // there is no rounding up
        [TestCase("01/01/2019 14:20:00Z", "01/01/2019 14:20:00Z")] // UTC timestamps, but will check will in local TZ
        [TestCase("01/01/2019 14:20:00.1Z", "01/01/2019 14:20:00.1Z")]
        [TestCase("01/01/2019 14:20:00.01Z", "01/01/2019 14:20:00.01Z")]
        [TestCase("01/01/2019 14:20:00.001Z", "01/01/2019 14:20:00.001Z")]
        [TestCase("01/01/2019 14:20:00.0001Z", "01/01/2019 14:20:00.0001Z")]
        [TestCase("01/01/2019 14:20:00.00001Z", "01/01/2019 14:20:00.00001Z")]
        [TestCase("01/01/2019 14:20:00.000001Z", "01/01/2019 14:20:00.000001Z")]
        public void TestLocalTimestampMicrosecond(string s, string e)
        {
            var schema = (LogicalSchema)Schema.Parse("{\"type\": \"long\", \"logicalType\": \"local-timestamp-micros\"}");

            var date = DateTime.Parse(s, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.RoundtripKind);

            if (date.Kind != DateTimeKind.Utc)
            {
                date = DateTime.Parse(s, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.AssumeLocal);
            }

            var expectedDate = DateTime.Parse(e, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.RoundtripKind);

            if (expectedDate.Kind != DateTimeKind.Utc)
            {
                expectedDate = DateTime.Parse(e, CultureInfo.GetCultureInfo("en-US").DateTimeFormat, DateTimeStyles.AssumeLocal);
            }

            expectedDate = expectedDate.ToLocalTime();
            
            var avroLocalTimestampMicro = new LocalTimestampMicrosecond();
            var convertedDate = (DateTime)avroLocalTimestampMicro.ConvertToLogicalValue(avroLocalTimestampMicro.ConvertToBaseValue(date, schema), schema);
            Assert.AreEqual(expectedDate, convertedDate);
            Assert.AreEqual(DateTimeKind.Local, convertedDate.Kind);
        }

        [TestCase("01:20:10", "01:20:10", false)]
        [TestCase("23:00:00", "23:00:00", false)]
        [TestCase("23:59:00", "23:59:00", false)]
        [TestCase("23:59:59", "23:59:59", false)]
        [TestCase("01:20:10.1", "01:20:10.1", false)]
        [TestCase("01:20:10.01", "01:20:10.01", false)]
        [TestCase("01:20:10.001", "01:20:10.001", false)]
        [TestCase("01:20:10.0001", "01:20:10", false)]
        [TestCase("01:20:10.0009", "01:20:10", false)] // there is no rounding up
        [TestCase("01:20:10.0019", "01:20:10.001", false)] // there is no rounding up
        [TestCase("23:59:59.999", "23:59:59.999", false)]
        [TestCase("01:00:00:00", null, true)]
        [TestCase("-00:00:00.001", null, true)]
        [TestCase("-00:00:00.000001", null, true)]
        [TestCase("-00:00:00.0000001", null, true)]
        [TestCase("-00:01", null, true)]
        [TestCase("-999999.00:00:00", null, true)]
        public void TestTimeMillisecond(string s, string e, bool expectRangeError)
        {
            var timeMilliSchema = (LogicalSchema)Schema.Parse("{\"type\": \"int\", \"logicalType\": \"time-millis\"}");

            var time = TimeSpan.Parse(s);
            
            var avroTimeMilli = new TimeMillisecond();

            if (expectRangeError)
            {
                Assert.Throws<ArgumentOutOfRangeException>(() =>
                {
                    avroTimeMilli.ConvertToLogicalValue(avroTimeMilli.ConvertToBaseValue(time, timeMilliSchema), timeMilliSchema);
                });
            }
            else
            {
                var expectedTime = TimeSpan.Parse(e);

                var convertedTime = (TimeSpan)avroTimeMilli.ConvertToLogicalValue(avroTimeMilli.ConvertToBaseValue(time, timeMilliSchema), timeMilliSchema);
                Assert.AreEqual(expectedTime, convertedTime);
            }
        }

        [TestCase("01:20:10", "01:20:10", false)]
        [TestCase("23:00:00", "23:00:00", false)]
        [TestCase("23:59:00", "23:59:00", false)]
        [TestCase("23:59:59", "23:59:59", false)]
        [TestCase("01:20:10.1", "01:20:10.1", false)]
        [TestCase("01:20:10.01", "01:20:10.01", false)]
        [TestCase("01:20:10.001", "01:20:10.001", false)]
        [TestCase("01:20:10.0001", "01:20:10.0001", false)]
        [TestCase("01:20:10.00001", "01:20:10.00001", false)]
        [TestCase("01:20:10.000001", "01:20:10.000001", false)]
        [TestCase("01:20:10.0000001", "01:20:10", false)]
        [TestCase("01:20:10.0000009", "01:20:10", false)]
        [TestCase("23:59:59.999999", "23:59:59.999999", false)]
        [TestCase("01:00:00:00", null, true)]
        [TestCase("-00:00:00.001", null, true)]
        [TestCase("-00:00:00.000001", null, true)]
        [TestCase("-00:00:00.0000001", null, true)]
        [TestCase("-00:01", null, true)]
        [TestCase("-999999.00:00:00", null, true)]
        public void TestTimeMicrosecond(string s, string e, bool expectRangeError)
        {
            var timeMicroSchema = (LogicalSchema)Schema.Parse("{\"type\": \"long\", \"logicalType\": \"time-micros\"}");

            var time = TimeSpan.Parse(s);
            
            var avroTimeMicro = new TimeMicrosecond();

            if (expectRangeError)
            {
                Assert.Throws<ArgumentOutOfRangeException>(() =>
                {
                    avroTimeMicro.ConvertToLogicalValue(avroTimeMicro.ConvertToBaseValue(time, timeMicroSchema), timeMicroSchema);
                });
            }
            else
            {
                var expectedTime = TimeSpan.Parse(e);

                var convertedTime = (TimeSpan)avroTimeMicro.ConvertToLogicalValue(avroTimeMicro.ConvertToBaseValue(time, timeMicroSchema), timeMicroSchema);
                Assert.AreEqual(expectedTime, convertedTime);

            }
        }

        [TestCase("633a6cf0-52cb-43aa-b00a-658510720958")]
        public void TestUuid(string guidString)
        {
            var schema = (LogicalSchema)Schema.Parse("{\"type\": \"string\", \"logicalType\": \"uuid\" }");

            var guid = new Guid(guidString);

            var avroUuid = new Uuid();

            Assert.True(avroUuid.IsInstanceOfLogicalType(guid));

            var converted = (Guid) avroUuid.ConvertToLogicalValue(avroUuid.ConvertToBaseValue(guid, schema), schema);
            Assert.AreEqual(guid, converted);
        }
    }
}
