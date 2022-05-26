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
            Assert.AreEqual(obj.AvroDecimalNullableProperty, result.AvroDecimalNullableProperty);
            Assert.AreEqual(obj.AvroDecimalProperty, result.AvroDecimalProperty);
            Assert.AreEqual(obj.GuidNullableProperty, result.GuidNullableProperty);
            Assert.AreEqual(obj.GuidProperty, result.GuidProperty);
            Assert.IsNull(obj.DateNullableProperty);
            Assert.AreEqual(obj.DateProperty.Date, result.DateProperty);
            Assert.AreEqual(obj.DateTimeMicrosecondNullableProperty, obj.DateTimeMicrosecondNullableProperty);
            Assert.AreEqual(obj.DateTimeMicrosecondProperty, obj.DateTimeMicrosecondProperty);
            Assert.IsNull(result.DateTimeMillisecondNullableProperty);
            Assert.AreEqual((obj.DateTimeMillisecondProperty.Ticks / 10000) * 10000, result.DateTimeMillisecondProperty.Ticks);
            Assert.AreEqual(obj.TimeSpanMicrosecondNullableProperty, result.TimeSpanMicrosecondNullableProperty);
            Assert.AreEqual(obj.TimeSpanMicrosecondProperty, result.TimeSpanMicrosecondProperty);
            Assert.AreEqual(obj.TimeSpanMillisecondNullableProperty, result.TimeSpanMillisecondNullableProperty);
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
            Assert.AreEqual(obj.AvroDecimalNullableProperty, result.AvroDecimalNullableProperty);
            Assert.AreEqual(obj.AvroDecimalProperty, result.AvroDecimalProperty);
            Assert.AreEqual(obj.GuidNullableProperty, result.GuidNullableProperty);
            Assert.AreEqual(obj.GuidProperty, result.GuidProperty);
            Assert.AreEqual(obj.DateNullableProperty?.Date, result.DateProperty);
            Assert.AreEqual(obj.DateProperty.Date, result.DateProperty);
            Assert.AreEqual(obj.DateTimeMicrosecondNullableProperty, obj.DateTimeMicrosecondNullableProperty);
            Assert.AreEqual(obj.DateTimeMicrosecondProperty, obj.DateTimeMicrosecondProperty);
            Assert.AreEqual((obj.TimeSpanMicrosecondNullableProperty?.Ticks / 10000) * 10000, result.TimeSpanMicrosecondNullableProperty?.Ticks);
            Assert.AreEqual((obj.DateTimeMillisecondProperty.Ticks / 10000) * 10000, result.DateTimeMillisecondProperty.Ticks);
            Assert.AreEqual(obj.TimeSpanMicrosecondNullableProperty, result.TimeSpanMicrosecondNullableProperty);
            Assert.AreEqual(obj.TimeSpanMicrosecondProperty, result.TimeSpanMicrosecondProperty);
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
