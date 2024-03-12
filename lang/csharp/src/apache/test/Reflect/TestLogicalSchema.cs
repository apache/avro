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

using System;
using System.IO;
using Avro.IO;
using Avro.Reflect;
using NUnit.Framework;

namespace Avro.test.Reflect
{
    public class TestLogicalSchema
    {
        [TestCase]
        public void WriteAndReadObjectsWithLogicalSchemaFields_WithNullValues()
        {
            //Arrange
            var obj = new TestObject
            {
                AvroDecimalNullableProperty = null,
                AvroDecimalProperty = 13.42m,
                GuidNullableProperty = null,
                GuidProperty = Guid.NewGuid(),
                DateNullableProperty = null,
                DateProperty = new DateTime(2022, 05, 26, 14, 57, 24, 123),
                DateTimeMicrosecondNullableProperty = null,
                DateTimeMicrosecondProperty = DateTime.UtcNow,
                DateTimeMillisecondNullableProperty = null,
                DateTimeMillisecondProperty = DateTime.UtcNow,
                TimeSpanMicrosecondNullableProperty = null,
                TimeSpanMicrosecondProperty = new TimeSpan(23, 59, 59),
                TimeSpanMillisecondNullableProperty = null,
                TimeSpanMillisecondProperty = new TimeSpan(23, 59, 59),
            };

            var schema = Schema.Parse(SchemaJson);
            var writer = new ReflectWriter<TestObject>(schema);
            var reader = new ReflectReader<TestObject>(schema, schema);
            var writeStream = new MemoryStream();
            var writeBinaryEncoder = new BinaryEncoder(writeStream);

            //Act
            writer.Write(obj, writeBinaryEncoder);
            var data = writeStream.ToArray();

            var readStream = new MemoryStream(data);
            var result = reader.Read(null, new BinaryDecoder(readStream));

            //Assert
            Assert.NotNull(result);

            Assert.IsNull(result.AvroDecimalNullableProperty);
            Assert.AreEqual(obj.AvroDecimalProperty, result.AvroDecimalProperty);

            Assert.IsNull(result.GuidNullableProperty);
            Assert.AreEqual(obj.GuidProperty, result.GuidProperty);

            Assert.IsNull(obj.DateNullableProperty);
            Assert.AreEqual(obj.DateProperty.Date, result.DateProperty);

            Assert.IsNull(result.DateTimeMicrosecondNullableProperty);
            Assert.AreEqual((obj.DateTimeMicrosecondProperty.Ticks / 10 ) * 10, result.DateTimeMicrosecondProperty.Ticks);

            Assert.IsNull(result.DateTimeMillisecondNullableProperty);
            Assert.AreEqual((obj.DateTimeMillisecondProperty.Ticks / 10000) * 10000, result.DateTimeMillisecondProperty.Ticks);

            Assert.IsNull(result.TimeSpanMicrosecondNullableProperty);
            Assert.AreEqual(obj.TimeSpanMicrosecondProperty, result.TimeSpanMicrosecondProperty);

            Assert.IsNull(result.TimeSpanMillisecondNullableProperty);
            Assert.AreEqual(obj.TimeSpanMillisecondProperty, result.TimeSpanMillisecondProperty);
        }

        [TestCase]
        public void WriteAndReadObjectsWithLogicalSchemaFields_WithoutNullValues()
        {
            //Arrange
            var obj = new TestObject
            {
                AvroDecimalNullableProperty = 136.42m,
                AvroDecimalProperty = 13.42m,
                GuidNullableProperty = Guid.NewGuid(),
                GuidProperty = Guid.NewGuid(),
                DateNullableProperty = new DateTime(2022, 05, 26, 14, 57, 24, 123),
                DateProperty = new DateTime(2022, 05, 26, 14, 57, 24, 123),
                DateTimeMicrosecondNullableProperty = DateTime.UtcNow,
                DateTimeMicrosecondProperty = DateTime.UtcNow,
                DateTimeMillisecondNullableProperty = DateTime.UtcNow,
                DateTimeMillisecondProperty = DateTime.UtcNow,
                TimeSpanMicrosecondNullableProperty = new TimeSpan(23, 59, 59),
                TimeSpanMicrosecondProperty = new TimeSpan(23, 59, 59),
                TimeSpanMillisecondNullableProperty = new TimeSpan(23, 59, 59),
                TimeSpanMillisecondProperty = new TimeSpan(23, 59, 59),
            };

            var schema = Schema.Parse(SchemaJson);
            var writer = new ReflectWriter<TestObject>(schema);
            var reader = new ReflectReader<TestObject>(schema, schema);
            var writeStream = new MemoryStream();
            var writeBinaryEncoder = new BinaryEncoder(writeStream);

            //Act
            writer.Write(obj, writeBinaryEncoder);
            var data = writeStream.ToArray();

            var readStream = new MemoryStream(data);
            var result = reader.Read(null, new BinaryDecoder(readStream));

            //Assert
            Assert.NotNull(result);

            Assert.NotNull(result.AvroDecimalNullableProperty);
            Assert.AreEqual(obj.AvroDecimalNullableProperty, result.AvroDecimalNullableProperty);
            Assert.AreEqual(obj.AvroDecimalProperty, result.AvroDecimalProperty);

            Assert.NotNull(result.GuidNullableProperty);
            Assert.AreEqual(obj.GuidNullableProperty, result.GuidNullableProperty);
            Assert.AreEqual(obj.GuidProperty, result.GuidProperty);

            Assert.NotNull(result.DateProperty);
            Assert.AreEqual(obj.DateNullableProperty?.Date, result.DateProperty);
            Assert.AreEqual(obj.DateProperty.Date, result.DateProperty);

            Assert.NotNull(result.DateTimeMicrosecondNullableProperty);
            Assert.AreEqual((obj.DateTimeMicrosecondNullableProperty?.Ticks / 10) * 10, result.DateTimeMicrosecondNullableProperty?.Ticks);
            Assert.AreEqual((obj.DateTimeMicrosecondProperty.Ticks / 10) * 10, result.DateTimeMicrosecondProperty.Ticks);

            Assert.NotNull(result.DateTimeMillisecondNullableProperty);
            Assert.AreEqual((obj.DateTimeMillisecondNullableProperty?.Ticks / 10000) * 10000, result.DateTimeMillisecondNullableProperty?.Ticks);
            Assert.AreEqual((obj.DateTimeMillisecondProperty.Ticks / 10000) * 10000, result.DateTimeMillisecondProperty.Ticks);

            Assert.NotNull(result.TimeSpanMicrosecondNullableProperty);
            Assert.AreEqual(obj.TimeSpanMicrosecondNullableProperty, result.TimeSpanMicrosecondNullableProperty);
            Assert.AreEqual(obj.TimeSpanMicrosecondProperty, result.TimeSpanMicrosecondProperty);

            Assert.NotNull(result.TimeSpanMillisecondNullableProperty);
            Assert.AreEqual(obj.TimeSpanMillisecondNullableProperty, result.TimeSpanMillisecondNullableProperty);
            Assert.AreEqual(obj.TimeSpanMillisecondProperty, result.TimeSpanMillisecondProperty);
        }

        private const string SchemaJson = @"
{
  ""type"" : ""record"",
  ""namespace"" : ""Avro.test.Reflect.Converters"",
  ""name"" : ""TestObject"",
  ""fields"" : [
    { ""name"" : ""AvroDecimalNullableProperty"" , ""type"" : [""null"", { ""type"": ""bytes"", ""logicalType"": ""decimal"", ""precision"": 6, ""scale"": 2 }] },
    { ""name"" : ""AvroDecimalProperty"" , ""type"" : { ""type"": ""bytes"", ""logicalType"": ""decimal"", ""precision"": 6, ""scale"": 2 } },
    { ""name"" : ""GuidNullableProperty"" , ""type"" : [""null"", { ""type"": ""string"", ""logicalType"": ""uuid""}] },
    { ""name"" : ""GuidProperty"" , ""type"" : { ""type"": ""string"", ""logicalType"": ""uuid""} },
    { ""name"" : ""DateNullableProperty"" , ""type"" : [""null"", { ""type"": ""int"", ""logicalType"": ""date""}] },
    { ""name"" : ""DateProperty"" , ""type"" : { ""type"": ""int"", ""logicalType"": ""date""} },
    { ""name"" : ""DateTimeMicrosecondNullableProperty"" , ""type"" : [""null"", { ""type"": ""long"", ""logicalType"": ""timestamp-micros""}] },
    { ""name"" : ""DateTimeMicrosecondProperty"" , ""type"" : { ""type"": ""long"", ""logicalType"": ""timestamp-micros""} },
    { ""name"" : ""DateTimeMillisecondNullableProperty"" , ""type"" : [""null"", { ""type"": ""long"", ""logicalType"": ""timestamp-millis""}] },
    { ""name"" : ""DateTimeMillisecondProperty"" , ""type"" : { ""type"": ""long"", ""logicalType"": ""timestamp-millis""} },
    { ""name"" : ""TimeSpanMicrosecondNullableProperty"" , ""type"" : [""null"", { ""type"": ""long"", ""logicalType"": ""time-micros""}] },
    { ""name"" : ""TimeSpanMicrosecondProperty"" , ""type"" : { ""type"": ""long"", ""logicalType"": ""time-micros""} },
    { ""name"" : ""TimeSpanMillisecondNullableProperty"" , ""type"" : [""null"", { ""type"": ""int"", ""logicalType"": ""time-millis""}] },
    { ""name"" : ""TimeSpanMillisecondProperty"" , ""type"" : { ""type"": ""int"", ""logicalType"": ""time-millis""} }
  ]
}
";

        public class TestObject
        {
            public AvroDecimal? AvroDecimalNullableProperty { get; set; }
            public AvroDecimal AvroDecimalProperty { get; set; }
            public Guid? GuidNullableProperty { get; set; }
            public Guid GuidProperty { get; set; }
            public DateTime? DateNullableProperty { get; set; }
            public DateTime DateProperty { get; set; }
            public DateTime? DateTimeMicrosecondNullableProperty { get; set; }
            public DateTime DateTimeMicrosecondProperty { get; set; }
            public DateTime? DateTimeMillisecondNullableProperty { get; set; }
            public DateTime DateTimeMillisecondProperty { get; set; }
            public TimeSpan? TimeSpanMicrosecondNullableProperty { get; set; }
            public TimeSpan TimeSpanMicrosecondProperty { get; set; }
            public TimeSpan? TimeSpanMillisecondNullableProperty { get; set; }
            public TimeSpan TimeSpanMillisecondProperty { get; set; }
        }
    }
}
