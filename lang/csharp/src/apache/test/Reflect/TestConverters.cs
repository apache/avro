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
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Avro.IO;
using Avro.Reflect;
using NUnit.Framework;

namespace Avro.Test
{
    public enum MessageTypes
    {
        None,
        Verbose,
        Info,
        Warning,
        Error
    }

    public class LogMessage
    {
        private Dictionary<string, string> _tags = new Dictionary<string, string>();

        public string IP { get; set; }

        [AvroField("Message")]
        public string message { get; set; }

        [AvroField(typeof(DateTimeOffsetToLongConverter))]
        public DateTimeOffset TimeStamp { get; set; }

        public Dictionary<string, string> Tags { get => _tags; set => _tags = value; }

        public MessageTypes Severity { get; set; }
    }

    public interface ITimeStamp
    {
        DateTimeOffset? TimeStamp { get; set; }
    }

    public class NullableWithNoDefinedConverter : ITimeStamp
    {
        public DateTimeOffset? TimeStamp { get; set; }
    }

    public class NullableWithDefinedConverter : ITimeStamp
    {
        [AvroField(typeof(DateTimeOffsetToLongConverter))]
        public DateTimeOffset? TimeStamp { get; set; }
    }

    public class StringToDecimalConverter : TypedFieldConverter<string, decimal>
    {
        public override decimal From(string o, Schema s)
        {
            return decimal.Parse(o);
        }

        public override string To(decimal o, Schema s)
        {
            return o.ToString(CultureInfo.InvariantCulture);
        }
    }

    public class NullableDecimal
    {
        public decimal? Amount { get; set; }
    }

    public class NonNullableDecimal
    {
        public decimal Amount { get; set; }
    }

    public class NullableDecimalWithConverter
    {
        [AvroField(typeof(StringToDecimalConverter))]
        public decimal? Amount { get; set; }
    }

    public class NonNullableDecimalWithConverter
    {
        [AvroField(typeof(StringToDecimalConverter))]
        public decimal Amount { get; set; }
    }

    [TestFixture]
    public class TestConverters
    {
        private const string _logMessageSchemaV1 = @"
        {
            ""namespace"": ""MessageTypes"",
            ""type"": ""record"",
            ""doc"": ""A simple log message type as used by this blog post."",
            ""name"": ""LogMessage"",
            ""fields"": [
                { ""name"": ""IP"", ""type"": ""string"" },
                { ""name"": ""Message"", ""type"": ""string"" },
                { ""name"": ""TimeStamp"", ""type"": ""long"" },
                { ""name"": ""Tags"",""type"":
                    { ""type"": ""map"",
                        ""values"": ""string""},
                        ""default"": {}},
                { ""name"": ""Severity"",
                ""type"": { ""namespace"": ""MessageTypes"",
                    ""type"": ""enum"",
                    ""doc"": ""Enumerates the set of allowable log levels."",
                    ""name"": ""LogLevel"",
                    ""symbols"": [""None"", ""Verbose"", ""Info"", ""Warning"", ""Error""]}}
            ]
        }";

        private const string _nullableConverterV1 = @"
        {
            ""namespace"": ""MessageTypes"",
            ""type"": ""record"",
            ""name"": ""NullableConverter"",
            ""fields"": [
                { ""name"": ""TimeStamp"", ""type"": [
                    ""null"",
                    ""long""
                    ]
                }
             ]
         }";

        private List<IAvroFieldConverter> _converters;

        [SetUp]
        public void SetUp()
        {
            _converters = ClassCache.ClearDefaultConverters();
        }

        [TearDown]
        public void TearDown()
        {
            ClassCache.ClearDefaultConverters();
            foreach (var converter in _converters)
            {
                ClassCache.AddDefaultConverter(converter);
            }
        }

        [TestCase]
        public void Serialize()
        {
            var schema = Schema.Parse(_logMessageSchemaV1);
            var avroWriter = new ReflectWriter<LogMessage>(schema);
            var avroReader = new ReflectReader<LogMessage>(schema, schema);

            byte[] serialized;

            var logMessage = new LogMessage()
            {
                IP = "10.20.30.40",
                message = "Log entry",
                Severity = MessageTypes.Error,
                TimeStamp = new DateTimeOffset(2002, 1, 21, 8, 15, 54, TimeSpan.FromHours(-4))
            };

            using (var stream = new MemoryStream(256))
            {
                avroWriter.Write(logMessage, new BinaryEncoder(stream));
                serialized = stream.ToArray();
            }

            LogMessage deserialized = null;
            using (var stream = new MemoryStream(serialized))
            {
                deserialized = avroReader.Read(default(LogMessage), new BinaryDecoder(stream));
            }
            Assert.IsNotNull(deserialized);
            Assert.AreEqual(logMessage.IP, deserialized.IP);
            Assert.AreEqual(logMessage.message, deserialized.message);
            Assert.AreEqual(logMessage.Severity, deserialized.Severity);
            Assert.AreEqual(logMessage.TimeStamp, deserialized.TimeStamp);
        }

        [TestCase(true)]
        [TestCase(false)]
        public void SerializeNullableWithNullableConverter(bool useNull)
        {
            SerializeTest<NullableWithDefinedConverter>(_nullableConverterV1, useNull);
        }

        [TestCase(true)]
        [TestCase(false)]
        public void SerializeNullableDefaultWithClass(bool useNull)
        {
            ClassCache.AddDefaultConverter(new DateTimeOffsetToLongConverter());
            SerializeTest<NullableWithNoDefinedConverter>(_nullableConverterV1, useNull);
        }

        [TestCase(true)]
        [TestCase(false)]
        public void SerializeNullableDefaultWithDelegate(bool useNull)
        {
            var converter = new DateTimeOffsetToLongConverter();
            ClassCache.AddDefaultConverter<long, DateTimeOffset>(
                (l, s) => (DateTimeOffset) converter.FromAvroType(l, s),
                (d, s) => (long)converter.ToAvroType(d, s));

            SerializeTest<NullableWithNoDefinedConverter>(_nullableConverterV1, useNull);
        }

        [TestCase(true)]
        [TestCase(false)]
        public void SerializeNullableDefaultWithNullableDelegate(bool useNull)
        {
            var converter = new DateTimeOffsetToLongConverter();

            ClassCache.AddDefaultConverter<long, DateTimeOffset?>(
                (l, s) => (DateTimeOffset?) converter.FromAvroType(l, s),
                (d, s) => d == null ? (long) 0 : (long) converter.ToAvroType(d, s));

            SerializeTest<NullableWithNoDefinedConverter>(_nullableConverterV1, useNull);
        }

        [TestCase(true)]
        [TestCase(false)]
        public void SerializeNullableDefaultWithDoubleNullableDelegate(bool useNull)
        {
            var converter = new DateTimeOffsetToLongConverter();

            ClassCache.AddDefaultConverter<long?, DateTimeOffset?>(
                (l, s) => l == null ? null : (DateTimeOffset?) converter.FromAvroType(l, s),
                (d, s) => d == null ? null : (long?) converter.ToAvroType(d, s));

            SerializeTest<NullableWithNoDefinedConverter>(_nullableConverterV1, useNull);
        }

        private const string _nullableDecimalV1 = @"
        {
            ""namespace"": ""MessageTypes"",
            ""type"": ""record"",
            ""name"": ""NullableDecimal"",
            ""fields"": [
                { ""name"": ""Amount"", ""type"": [
                    ""null"",
                    ""string""
                    ]
                }
             ]
         }";

        private const string _nonNullableDecimalV1 = @"
        {
            ""namespace"": ""MessageTypes"",
            ""type"": ""record"",
            ""name"": ""NonNullableDecimal"",
            ""fields"": [
                { ""name"": ""Amount"", ""type"": ""string"" }
             ]
         }";

        [TestCase(true)]
        [TestCase(false)]
        public void SerializeNullableStringToDecimalConverter(bool useNull)
        {
            ClassCache.AddDefaultConverter(new StringToDecimalConverter());

            SerializeTest<NullableDecimal>(_nullableDecimalV1, useNull,
                (d, n) => d.Amount = n ? (decimal?) null : 14.34M,
                (l, r) => Assert.AreEqual(l.Amount, r.Amount));
        }

        [TestCase(true)]
        [TestCase(false)]
        public void SerializeNullableStringToDecimalConverterWithAttribute(bool useNull)
        {
            SerializeTest<NullableDecimalWithConverter>(_nullableDecimalV1, useNull,
                (d, n) => d.Amount = n ? (decimal?) null : 14.34M,
                (l, r) => Assert.AreEqual(l.Amount, r.Amount));
        }

        // [TestCase(true)]
        [TestCase(false)]
        public void SerializeNonNullableStringToDecimalConverterWithAttribute(bool useNull)
        {
            SerializeTest<NonNullableDecimalWithConverter>(_nullableDecimalV1, useNull,
                (d, n) => d.Amount = n ? throw new NotSupportedException("cannot test null") : 14.34M,
                (l, r) => Assert.AreEqual(l.Amount, r.Amount));
        }

        // [TestCase(true)]
        [TestCase(false)]
        public void SerializeStringToDecimalConverter(bool useNull)
        {
            ClassCache.AddDefaultConverter(new StringToDecimalConverter());

            SerializeTest<NonNullableDecimal>(_nonNullableDecimalV1, useNull,
                (d, n) => d.Amount = n ? throw new NotSupportedException("cannot test null") : 14.34M,
                (l, r) => Assert.AreEqual(l.Amount, r.Amount));
        }

        private void SerializeTest<T>(string schemaJson, bool useNull)
            where T : class, ITimeStamp, new()
        {
            SerializeTest<T>(schemaJson, useNull,
                (m, n) => m.TimeStamp = n ? (DateTimeOffset?) null : new DateTimeOffset(2002, 1, 21, 8, 15, 54, TimeSpan.FromHours(-4)),
                (l, r) => Assert.AreEqual(l.TimeStamp, r.TimeStamp));
        }

        private void SerializeTest<T>(string schemaJson, bool useNull, Action<T, bool> setValue, Action<T, T> assert)
            where T : class, new()
        {
            var schema = Schema.Parse(schemaJson);
            var avroWriter = new ReflectWriter<T>(schema);
            var avroReader = new ReflectReader<T>(schema, schema);

            byte[] serialized;

            var message = new T();

            setValue(message, useNull);

            using (var stream = new MemoryStream(256))
            {
                avroWriter.Write(message, new BinaryEncoder(stream));
                serialized = stream.ToArray();
            }

            T deserialized = default(T);
            using (var stream = new MemoryStream(serialized))
            {
                deserialized = avroReader.Read(default(T), new BinaryDecoder(stream));
            }

            Assert.IsNotNull(deserialized);

            assert(message, deserialized);
        }
    }
}
