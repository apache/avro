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

    public class NullableWithNullableConverter : ITimeStamp
    {
        [AvroField(typeof(NullableConverter<DateTimeOffsetToLongConverter>))]
        public DateTimeOffset? TimeStamp { get; set; }
    }


    [TestFixture]
    public class TestLogMessage
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
            var converters = ClassCache.ClearDefaultConverters();
            try
            {
                SerializeTest<NullableWithNullableConverter>(_nullableConverterV1, useNull);
            }
            finally
            {
                // leave the cache the same as it was before we started
                foreach (var converter in converters)
                {
                    ClassCache.AddDefaultConverter(converter);
                }
            }
        }

        [TestCase(true)]
        [TestCase(false)]
        public void SerializeNullableDefaultWithClass(bool useNull)
        {
            var converters = ClassCache.ClearDefaultConverters();
            ClassCache.AddDefaultConverter(new DateTimeOffsetToLongConverter());
            try
            {
                SerializeTest<NullableWithNoDefinedConverter>(_nullableConverterV1, useNull);
            }
            finally
            {
                // leave the cache the same as it was before we started
                ClassCache.ClearDefaultConverters();
                foreach (var converter in converters)
                {
                    ClassCache.AddDefaultConverter(converter);
                }
            }
        }

        [TestCase(true)]
        [TestCase(false)]
        public void SerializeNullableDefaultWithDelegate(bool useNull)
        {
            var converters = ClassCache.ClearDefaultConverters();

            var converter = new DateTimeOffsetToLongConverter();

            ClassCache.AddDefaultConverter<long, DateTimeOffset>(
                (l, s) => (DateTimeOffset) converter.FromAvroType(l, s),
                (d, s) => (long)converter.ToAvroType(d, s));

            try
            {
                SerializeTest<NullableWithNoDefinedConverter>(_nullableConverterV1, useNull);
            }
            finally
            {
                // leave the cache the same as it was before we started
                ClassCache.ClearDefaultConverters();
                foreach (var c in converters)
                {
                    ClassCache.AddDefaultConverter(c);
                }
            }
        }

        [TestCase(true)]
        [TestCase(false)]
        public void SerializeNullableDefaultWithNullableDelegate(bool useNull)
        {
            var converters = ClassCache.ClearDefaultConverters();

            var converter = new DateTimeOffsetToLongConverter();

            ClassCache.AddDefaultConverter<long, DateTimeOffset?>(
                (l, s) => (DateTimeOffset?) converter.FromAvroType(l, s),
                (d, s) => d == null ? (long) 0 : (long) converter.ToAvroType(d, s));

            try
            {
                SerializeTest<NullableWithNoDefinedConverter>(_nullableConverterV1, useNull);
            }
            finally
            {
                // leave the cache the same as it was before we started
                ClassCache.ClearDefaultConverters();
                foreach (var c in converters)
                {
                    ClassCache.AddDefaultConverter(c);
                }
            }
        }

        [TestCase(true)]
        [TestCase(false)]
        public void SerializeNullableDefaultWithDoubleNullableDelegate(bool useNull)
        {
            var converters = ClassCache.ClearDefaultConverters();

            var converter = new DateTimeOffsetToLongConverter();

            ClassCache.AddDefaultConverter<long?, DateTimeOffset?>(
                (l, s) => l == null ? null : (DateTimeOffset?) converter.FromAvroType(l, s),
                (d, s) => d == null ? null : (long?) converter.ToAvroType(d, s));

            try
            {
                SerializeTest<NullableWithNoDefinedConverter>(_nullableConverterV1, useNull);
            }
            finally
            {
                // leave the cache the same as it was before we started
                ClassCache.ClearDefaultConverters();
                foreach (var c in converters)
                {
                    ClassCache.AddDefaultConverter(c);
                }
            }
        }

        private void SerializeTest<T>(string schemaJson, bool useNull)
            where T : class, ITimeStamp, new()
        {
            var schema = Schema.Parse(schemaJson);
            var avroWriter = new ReflectWriter<T>(schema);
            var avroReader = new ReflectReader<T>(schema, schema);

            byte[] serialized;

            var message = new T
            {
                TimeStamp = useNull
                    ? (DateTimeOffset?) null
                    : new DateTimeOffset(2002, 1, 21, 8, 15, 54, TimeSpan.FromHours(-4))
            };

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
            Assert.AreEqual(message.TimeStamp, deserialized.TimeStamp);
        }
    }
}
