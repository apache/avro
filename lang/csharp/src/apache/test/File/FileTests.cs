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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Avro.File;
using Avro.Generic;
using Avro.Specific;
using NUnit.Framework;

namespace Avro.Test.File
{
    [TestFixture]
    public class FileTests
    {
        const string specificSchema  = "{\"type\":\"record\",\"name\":\"Foo\",\"namespace\":\"Avro.Test.File\",\"fields\":"
                                     + "[{\"name\":\"name\",\"type\":[\"null\",\"string\"]},{\"name\":\"age\",\"type\":\"int\"}]}";

        private static IEnumerable<TestCaseData> TestSpecificDataSource()
        {
            foreach (Codec.Type codecType in Enum.GetValues(typeof(Codec.Type)))
            {
                yield return new TestCaseData(specificSchema, new object[]
                    {
                        new object[] { "John", 23 }
                    }, codecType).SetName("{m}(Case0,{2})");

                yield return new TestCaseData(specificSchema, new object[]
                    {
                        new object[] { "John", 23 },
                        new object[] { "Jane", 99 },
                        new object[] { "Jeff", 88 }
                    }, codecType).SetName("{m}(Case1,{2})");

                yield return new TestCaseData(specificSchema, new object[]
                    {
                        new object[] { "John", 23 },
                        new object[] { "Jane", 99 },
                        new object[] { "Jeff", 88 },
                        new object[] { "James", 13 },
                        new object[] { "June", 109 },
                        new object[] { "Lloyd", 18 },
                        new object[] {"Jenny", 3},
                        new object[] { "Bob", 9 },
                        new object[] { null, 48 }
                    }, codecType).SetName("{m}(Case2,{2})");

                yield return new TestCaseData(specificSchema, new object[]
                    {
                        new object[] { "John", 23},
                        new object[] { "Jane", 99 },
                        new object[] { "Jeff", 88 },
                        new object[] { "James", 13 },
                        new object[] { "June", 109 },
                        new object[] { "Lloyd", 18 },
                        new object[] { "Jamie", 53 },
                        new object[] { "Fanessa", 101 },
                        new object[] { "Kan", 18 },
                        new object[] { "Janey", 33 },
                        new object[] { "Deva", 102 },
                        new object[] { "Gavin", 28 },
                        new object[] { "Lochy", 113 },
                        new object[] { "Nickie", 10 },
                        new object[] { "Liddia", 38 },
                        new object[] { "Fred", 3 },
                        new object[] { "April", 17 },
                        new object[] { "Novac", 48 },
                        new object[] { "Idan", 33 },
                        new object[] { "Jolyon", 76 },
                        new object[] { "Ant", 68 },
                        new object[] { "Ernie", 43 },
                        new object[] { "Joel", 99 },
                        new object[] { "Dan", 78 },
                        new object[] { "Dave", 103 },
                        new object[] { "Hillary", 79 },
                        new object[] { "Grant", 88 },
                        new object[] { "JJ", 14 },
                        new object[] { "Bill", 90 },
                        new object[] { "Larry", 4 },
                        new object[] { "Jenny", 3 },
                        new object[] { "Bob", 9 },
                        new object[] { null, 48 }
                    }, codecType).SetName("{m}(Case3,{2})");
            }
        }

        /// <summary>
        /// Reading & writing of specific (custom) record objects
        /// </summary>
        /// <param name="recs"></param>
        /// <param name="codecType"></param>
        [TestCaseSource(nameof(TestSpecificDataSource))]
        public void TestSpecificData(string schemaStr, object[] recs, Codec.Type codecType)
        {
            // create and write out
            IList<Foo> records = MakeRecords(recs);

            foreach(var rwFactory in SpecificOptions<Foo>())
            {
                MemoryStream dataFileOutputStream = new MemoryStream();
                Schema schema = Schema.Parse(schemaStr);
                using (IFileWriter<Foo> dataFileWriter = rwFactory.CreateWriter(dataFileOutputStream, schema, Codec.CreateCodec(codecType)))
                {
                    foreach (Foo rec in records)
                        dataFileWriter.Append(rec);
                }

                MemoryStream dataFileInputStream = new MemoryStream(dataFileOutputStream.ToArray());

                // read back
                IList<Foo> readRecords = new List<Foo>();

                using (IFileReader<Foo> reader = rwFactory.CreateReader(dataFileInputStream, null))
                {
                    foreach (Foo rec in reader.NextEntries)
                        readRecords.Add(rec);
                }

                // compare objects via Json
                Assert.AreEqual(records.Count, readRecords.Count);
                for (int i = 0; i < records.Count; i++)
                {
                    Assert.AreEqual(records[i].ToString(), readRecords[i].ToString());
                }
            }
        }

        private static IEnumerable<TestCaseData> TestAppendSpecificDataSource()
        {
            foreach (Codec.Type codecType in Enum.GetValues(typeof(Codec.Type)))
            {
                yield return new TestCaseData(specificSchema,
                    new object[]
                    {
                        new object[] { "John", 23 }
                    },
                    new object[]
                    {
                        new object[] { "Jane", 21 }
                    }, codecType).SetName("{m}(Case0,{3})");

                yield return new TestCaseData(specificSchema,
                    new object[]
                    {
                        new object[] { "John", 23 },
                        new object[] { "Jane", 99 },
                        new object[] { "Jeff", 88 },
                        new object[] { "James", 13 },
                        new object[] { "June", 109 },
                        new object[] { "Lloyd", 18 },
                        new object[] { "Jenny", 3 },
                        new object[] { "Bob", 9 },
                        new object[] { null, 48 }
                    },
                    new object[]
                    {
                        new object[] { "Hillary", 79 },
                        new object[] { "Grant", 88 }
                    }, codecType).SetName("{m}(Case1,{3})");
            }
        }

        /// <summary>
        /// Test appending of specific (custom) record objects
        /// </summary>
        /// <param name="schemaStr">schema</param>
        /// <param name="recs">initial records</param>
        /// <param name="appendRecs">append records</param>
        /// <param name="codecType">initial compression codec type</param>
        [TestCaseSource(nameof(TestAppendSpecificDataSource))]
        public void TestAppendSpecificData(string schemaStr, object[] recs, object[] appendRecs, Codec.Type codecType)
        {
            IList<Foo> records = MakeRecords(recs);
            IList<Foo> appendRecords = MakeRecords(appendRecs);
            IList<Foo> allRecords = records.Concat(appendRecords).ToList();

            foreach (var rwFactory in SpecificOptions<Foo>())
            {
                // create and write out
                MemoryStream dataFileOutputStream = new MemoryStream();
                Schema schema = Schema.Parse(schemaStr);
                using (IFileWriter<Foo> dataFileWriter = rwFactory.CreateWriter(dataFileOutputStream, schema, Codec.CreateCodec(codecType)))
                {
                    foreach (Foo rec in records)
                        dataFileWriter.Append(rec);
                }

                // append records
                byte[] outputData = dataFileOutputStream.ToArray();
                MemoryStream dataFileAppendInputStream = new MemoryStream(dataFileOutputStream.ToArray());
                MemoryStream dataFileAppendStream = new MemoryStream(); // MemoryStream is not expandable
                dataFileAppendStream.Write(outputData, 0, outputData.Length);

                using (IFileWriter<Foo> appendFileWriter = rwFactory.CreateAppendWriter(dataFileAppendInputStream, dataFileAppendStream, schema))
                {
                    foreach (Foo rec in appendRecords)
                        appendFileWriter.Append(rec);
                }

                MemoryStream dataFileInputStream = new MemoryStream(dataFileAppendStream.ToArray());

                // read back
                IList<Foo> readRecords = new List<Foo>();

                using (IFileReader<Foo> reader = rwFactory.CreateReader(dataFileInputStream, null))
                {
                    foreach (Foo rec in reader.NextEntries)
                        readRecords.Add(rec);
                }

                // compare objects via Json
                Assert.AreEqual(allRecords.Count, readRecords.Count);
                for (int i = 0; i < allRecords.Count; i++)
                    Assert.AreEqual(allRecords[i].ToString(), readRecords[i].ToString());
            }
        }

        private static IEnumerable<TestCaseData> TestGenericDataSource()
        {
            foreach (Codec.Type codecType in Enum.GetValues(typeof(Codec.Type)))
            {
                yield return new TestCaseData(
                    "{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"null\"}]}", new object[] { "f1", null }, codecType)
                    .SetName("{m}(null,{2})");
                yield return new TestCaseData(
                    "{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"boolean\"}]}", new object[] { "f1", true }, codecType)
                    .SetName("{m}(true,{2})");
                yield return new TestCaseData(
                    "{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"boolean\"}]}", new object[] { "f1", false }, codecType)
                    .SetName("{m}(false,{2})"); ;
                yield return new TestCaseData(
                    "{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"int\"}]}", new object[] { "f1", 101 }, codecType)
                    .SetName("{m}(int,{2})"); ;
                yield return new TestCaseData(
                    "{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"long\"}]}", new object[] { "f1", 101L }, codecType)
                    .SetName("{m}(long,{2})"); ;
                yield return new TestCaseData(
                    "{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"float\"}]}", new object[] { "f1", 101.78f }, codecType)
                    .SetName("{m}(float,{2})"); ;
                yield return new TestCaseData(
                    "{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"double\"}]}", new object[] { "f1", 101.78 }, codecType)
                    .SetName("{m}(double,{2})"); ;
                yield return new TestCaseData(
                    "{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"string\"}]}", new object[] { "f1", "A" }, codecType)
                    .SetName("{m}(string,{2})"); ;
                yield return new TestCaseData(
                    "{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"bytes\"}]}", new object[] { "f1", new byte[] { 0, 1 } }, codecType)
                    .SetName("{m}(bytes,{2})"); ;
            }
        }

        /// <summary>
        /// Reading & writing of generic record objects
        /// </summary>
        /// <param name="schemaStr"></param>
        /// <param name="value"></param>
        /// <param name="codecType"></param>
        [TestCaseSource(nameof(TestGenericDataSource))]
        public void TestGenericData(string schemaStr, object[] value, Codec.Type codecType)
        {
            foreach (var rwFactory in GenericOptions<GenericRecord>())
            {
                // Create and write out
                MemoryStream dataFileOutputStream = new MemoryStream();
                using (var writer = rwFactory.CreateWriter(dataFileOutputStream, Schema.Parse(schemaStr), Codec.CreateCodec(codecType)))
                {
                    writer.Append(mkRecord(value, Schema.Parse(schemaStr) as RecordSchema));
                }

                MemoryStream dataFileInputStream = new MemoryStream(dataFileOutputStream.ToArray());

                // Read back
                IList<GenericRecord> readFoos = new List<GenericRecord>();
                using (IFileReader<GenericRecord> reader = rwFactory.CreateReader(dataFileInputStream,null))
                {
                    foreach (GenericRecord foo in reader.NextEntries)
                    {
                        readFoos.Add(foo);
                    }
                }

                Assert.IsTrue((readFoos != null && readFoos.Count > 0),
                               string.Format(@"Generic object: {0} did not serialise/deserialise correctly", readFoos));
            }
        }

        private static IEnumerable<TestCaseData> TestAppendGenericDataSource()
        {
            foreach (Codec.Type codecType in Enum.GetValues(typeof(Codec.Type)))
            {
                yield return new TestCaseData(
                    "{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"boolean\"}]}", new object[] { "f1", true }, new object[] { "f1", false }, codecType)
                    .SetName("{m}(bool,{3})");
                yield return new TestCaseData(
                    "{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"int\"}]}", new object[] { "f1", 1 }, new object[] { "f1", 2 }, codecType)
                    .SetName("{m}(int,{3})");
                yield return new TestCaseData(
                    "{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"string\"}]}", new object[] { "f1", "A" }, new object[] { "f1", "B" }, codecType)
                    .SetName("{m}(string,{3})");
            }
        }

        /// <summary>
        /// Test appending of generic record objects
        /// </summary>
        /// <param name="schemaStr">schema</param>
        /// <param name="recs">initial records</param>
        /// <param name="appendRecs">append records</param>
        /// <param name="codecType">innitial compression codec type</param>
        [TestCaseSource(nameof(TestAppendGenericDataSource))]
        public void TestAppendGenericData(string schemaStr, object[] recs, object[] appendRecs, Codec.Type codecType)
        {
            foreach (var rwFactory in GenericOptions<GenericRecord>())
            {
                // Create and write out
                MemoryStream dataFileOutputStream = new MemoryStream();
                using (var writer = rwFactory.CreateWriter(dataFileOutputStream, Schema.Parse(schemaStr), Codec.CreateCodec(codecType)))
                {
                    writer.Append(mkRecord(recs, Schema.Parse(schemaStr) as RecordSchema));
                }

                // append records
                byte[] outputData = dataFileOutputStream.ToArray();
                MemoryStream dataFileAppendInputStream = new MemoryStream(dataFileOutputStream.ToArray());
                MemoryStream dataFileAppendStream = new MemoryStream(); // MemoryStream is not expandable
                dataFileAppendStream.Write(outputData, 0, outputData.Length);

                using (var appendFileWriter = rwFactory.CreateAppendWriter(dataFileAppendInputStream, dataFileAppendStream, Schema.Parse(schemaStr)))
                {
                    appendFileWriter.Append(mkRecord(appendRecs, Schema.Parse(schemaStr) as RecordSchema));
                }

                MemoryStream dataFileInputStream = new MemoryStream(dataFileAppendStream.ToArray());

                // Read back
                IList<GenericRecord> readFoos = new List<GenericRecord>();
                using (IFileReader<GenericRecord> reader = rwFactory.CreateReader(dataFileInputStream, null))
                {
                    foreach (GenericRecord foo in reader.NextEntries)
                    {
                        readFoos.Add(foo);
                    }
                }

                Assert.NotNull(readFoos);
                Assert.AreEqual((recs.Length + appendRecs.Length) / 2, readFoos.Count,
                    $"Generic object: {readFoos} did not serialise/deserialise correctly");
            }
        }

        [Test]
        public void OpenAppendWriter_IncorrectInStream_Throws()
        {
            MemoryStream compressedStream = new MemoryStream();
            // using here a DeflateStream as it is a standard non-seekable stream, so if it works for this one,
            // it should also works with any standard non-seekable stream (ie: NetworkStreams)
            DeflateStream dataFileInputStream = new DeflateStream(compressedStream, CompressionMode.Compress);

            var action =  new TestDelegate(() => DataFileWriter<Foo>.OpenAppendWriter(null, dataFileInputStream, null));

            var ex = Assert.Throws(typeof(AvroRuntimeException), action);
        }

        [Test]
        public void OpenAppendWriter_IncorrectOutStream_Throws()
        {
            MemoryStream inStream = new MemoryStream();
            MemoryStream outStream = new MemoryStream();
            outStream.Close();

            var action = new TestDelegate(() => DataFileWriter<Foo>.OpenAppendWriter(null, inStream, outStream));

            Assert.Throws(typeof(AvroRuntimeException), action);
        }

        /// <summary>
        /// This test is a single test case of
        /// <see cref="TestGenericData(string, object[], Codec.Type)"/> but introduces a
        /// DeflateStream as it is a standard non-seekable Stream that has the same behavior as the
        /// NetworkStream, which we should handle.
        /// </summary>
        [TestCase("{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":[\"int\", \"long\"]}]}", new object[] { "f1", 100L }, Codec.Type.Null)]
        public void TestNonSeekableStream(string schemaStr, object[] value, Codec.Type codecType)
        {
            foreach (var rwFactory in GenericOptions<GenericRecord>())
            {
                // Create and write out
                MemoryStream compressedStream = new MemoryStream();
                // using here a DeflateStream as it is a standard non-seekable stream, so if it works for this one,
                // it should also works with any standard non-seekable stream (ie: NetworkStreams)
                DeflateStream dataFileOutputStream = new DeflateStream(compressedStream, CompressionMode.Compress);
                using (var writer = rwFactory.CreateWriter(dataFileOutputStream, Schema.Parse(schemaStr), Codec.CreateCodec(codecType)))
                {
                    writer.Append(mkRecord(value, Schema.Parse(schemaStr) as RecordSchema));

                    // The Sync method is not supported for non-seekable streams.
                    Assert.Throws<NotSupportedException>(() => writer.Sync());
                }

                DeflateStream dataFileInputStream = new DeflateStream(new MemoryStream(compressedStream.ToArray()), CompressionMode.Decompress);

                // Read back
                IList<GenericRecord> readFoos = new List<GenericRecord>();
                using (IFileReader<GenericRecord> reader = rwFactory.CreateReader(dataFileInputStream, null))
                {
                    foreach (GenericRecord foo in reader.NextEntries)
                    {
                        readFoos.Add(foo);
                    }

                    // These methods are not supported for non-seekable streams.
                    Assert.Throws<AvroRuntimeException>(() => reader.Seek(0));
                    Assert.Throws<AvroRuntimeException>(() => reader.PreviousSync());
                }

                Assert.IsTrue((readFoos != null && readFoos.Count > 0),
                               string.Format(@"Generic object: {0} did not serialise/deserialise correctly", readFoos));
            }
        }

        private static IEnumerable<TestCaseData> TestPrimitiveDataSource()
        {
            foreach (Codec.Type codecType in Enum.GetValues(typeof(Codec.Type)))
            {
                yield return new TestCaseData("[\"boolean\", \"null\"]", null, codecType);
                yield return new TestCaseData("[\"boolean\", \"null\"]", true, codecType);
                yield return new TestCaseData("[\"int\", \"long\"]", 100, codecType);
                yield return new TestCaseData("[\"int\", \"long\"]", 100L, codecType);
                yield return new TestCaseData("[\"float\", \"double\"]", 100.75, codecType);
                yield return new TestCaseData("[\"float\", \"double\"]", 23.67f, codecType);
                yield return new TestCaseData("{\"type\": \"boolean\"}", true, codecType);
                yield return new TestCaseData("{\"type\": \"boolean\"}", false, codecType);
                yield return new TestCaseData("{\"type\": \"string\"}", "John", codecType);
                yield return new TestCaseData("{\"type\": [\"null\",\"string\"]}", null, codecType);
                yield return new TestCaseData("{\"type\": \"int\"}", 1, codecType);
                yield return new TestCaseData("{\"type\": \"long\"}", 12312313123L, codecType);
                yield return new TestCaseData("{\"type\": \"float\"}", 0.0f, codecType);
                yield return new TestCaseData("{\"type\": \"double\"}", 0.0, codecType);
                yield return new TestCaseData("[{\"type\": \"array\", \"items\": \"float\"}, \"double\"]", new float[] { 23.67f, 22.78f }, codecType);
                yield return new TestCaseData("[{\"type\": \"array\", \"items\": \"float\"}, \"double\"]", 100.89, codecType);
                yield return new TestCaseData("[{\"type\": \"array\", \"items\": \"string\"}, \"string\"]", "a", codecType);
                yield return new TestCaseData("[{\"type\": \"array\", \"items\": \"string\"}, \"string\"]", new string[] { "a", "b" }, codecType);
                yield return new TestCaseData("[{\"type\": \"array\", \"items\": \"bytes\"}, \"bytes\"]", new byte[] { 1, 2, 3 }, codecType);
                yield return new TestCaseData("[{\"type\": \"array\", \"items\": \"bytes\"}, \"bytes\"]", new object[] { new byte[] { 1, 2 }, new byte[] { 3, 4 } }, codecType);
                yield return new TestCaseData("[{\"type\": \"enum\", \"symbols\": [\"s1\", \"s2\"], \"name\": \"e\"}, \"string\"]", "h1", codecType);
            }
        }

        /// <summary>
        /// Reading & writing of primitive objects
        /// </summary>
        /// <param name="schemaStr"></param>
        /// <param name="value"></param>
        /// <param name="codecType"></param>
        [TestCaseSource(nameof(TestPrimitiveDataSource))]
        public void TestPrimitiveData(string schemaStr, object value, Codec.Type codecType)
        {
            foreach(var rwFactory in GenericOptions<object>())
            {
                MemoryStream dataFileOutputStream = new MemoryStream();
                using (var writer = rwFactory.CreateWriter(dataFileOutputStream, Schema.Parse(schemaStr), Codec.CreateCodec(codecType)))
                {
                    writer.Append(value);
                }

                MemoryStream dataFileInputStream = new MemoryStream(dataFileOutputStream.ToArray());

                Assert.IsTrue(CheckPrimitive(dataFileInputStream, value, rwFactory.CreateReader),
                              string.Format("Error reading generic data for object: {0}", value));
            }
        }

        private static IEnumerable<TestCaseData> TestMetaDataSource()
        {
            foreach (Codec.Type codecType in Enum.GetValues(typeof(Codec.Type)))
            {
                foreach (bool useTypeGetter in new bool[] { true, false })
                {
                    yield return new TestCaseData("bytesTest", new byte[] { 1, 2, 3 }, codecType, useTypeGetter);
                    yield return new TestCaseData("stringTest", "testVal", codecType, useTypeGetter);
                    yield return new TestCaseData("longTest", 12312313123L, codecType, useTypeGetter);
                    yield return new TestCaseData("bytesTest", new byte[] { 1 }, codecType, useTypeGetter);
                    yield return new TestCaseData("longTest", -1211212L, codecType, useTypeGetter);
                }
            }
        }

        /// <summary>
        /// Reading & writing of header meta data
        /// </summary>
        /// <param name="value"></param>
        /// <param name="codecType"></param>
        /// <param name="useTypeGetter"></param>
        [TestCaseSource(nameof(TestMetaDataSource))]
        public void TestMetaData(string key, object value, Codec.Type codecType, bool useTypeGetter)
        {
            // create and write out
            object[] obj = new object[] { new object[] { "John", 23 } };
            IList<Foo> records = MakeRecords(obj);
            MemoryStream dataFileOutputStream = new MemoryStream();

            Schema schema = Schema.Parse(specificSchema);
            DatumWriter<Foo> writer = new SpecificWriter<Foo>(schema);
            using (IFileWriter<Foo> dataFileWriter = DataFileWriter<Foo>.OpenWriter(writer, dataFileOutputStream, Codec.CreateCodec(codecType)))
            {
                SetMetaData(dataFileWriter, key, value);
                foreach (Foo rec in records)
                    dataFileWriter.Append(rec);
            }

            MemoryStream dataFileInputStream = new MemoryStream(dataFileOutputStream.ToArray());

            // read back
            using (IFileReader<Foo> reader = DataFileReader<Foo>.OpenReader(dataFileInputStream))
            {
                Assert.IsTrue(ValidateMetaData(reader, key, value, useTypeGetter),
                              string.Format("Error validating header meta data for key: {0}, expected value: {1}", key, value));
            }
        }

        private static IEnumerable<TestCaseData> TestPartialReadSource()
        {
            foreach (Codec.Type codecType in Enum.GetValues(typeof(Codec.Type)))
            {
                yield return new TestCaseData(specificSchema, codecType, 0, 330).SetName("{m}({1},{2},{3})");
                yield return new TestCaseData(specificSchema, codecType, 1, 330).SetName("{m}({1},{2},{3})");
                yield return new TestCaseData(specificSchema, codecType, 135, 330).SetName("{m}({1},{2},{3})");
                yield return new TestCaseData(specificSchema, codecType, 194, 264).SetName("{m}({1},{2},{3})");
            }

            // This is only for Null codec
            yield return new TestCaseData(specificSchema, Codec.Type.Null, 888, 165).SetName("{m}({1},{2},{3})");
        }

        /// <summary>
        /// Partial reading of file / stream from
        /// position in stream
        /// </summary>
        /// <param name="schemaStr"></param>
        /// <param name="value"></param>
        /// <param name="codecType"></param>
        [TestCaseSource(nameof(TestPartialReadSource))]
        public void TestPartialRead(string schemaStr, Codec.Type codecType, int position, int expectedRecords)
        {
            // create and write out
            IList<Foo> records = MakeRecords(GetTestFooObject());

            MemoryStream dataFileOutputStream = new MemoryStream();

            Schema schema = Schema.Parse(schemaStr);
            DatumWriter<Foo> writer = new SpecificWriter<Foo>(schema);
            using (IFileWriter<Foo> dataFileWriter = DataFileWriter<Foo>.OpenWriter(writer, dataFileOutputStream, Codec.CreateCodec(codecType)))
            {
                for (int i = 0; i < 10; ++i)
                {
                    foreach (Foo foo in records)
                    {
                        dataFileWriter.Append(foo);
                    }

                    // write out block
                    if (i == 1 || i == 4)
                    {
                        dataFileWriter.Sync();
                    }
                }
            }

            MemoryStream dataFileInputStream = new MemoryStream(dataFileOutputStream.ToArray());

            // read back
            IList<Foo> readRecords = new List<Foo>();
            using (IFileReader<Foo> reader = DataFileReader<Foo>.OpenReader(dataFileInputStream))
            {
                // move to next block from position
                reader.Sync(position);

                // read records from synced position
                foreach (Foo rec in reader.NextEntries)
                    readRecords.Add(rec);
            }

            Assert.IsTrue((readRecords != null && readRecords.Count == expectedRecords),
                          string.Format("Error performing partial read after position: {0}", position));
        }

        /// <summary>
        /// Partial reading of file / stream from position in stream
        /// Tests reading from sync boundaries.
        /// </summary>
        /// <param name="schemaStr"></param>
        /// <param name="codecType"></param>
        [Test]
        public void TestPartialReadAll([Values(specificSchema)] string schemaStr, [Values] Codec.Type codecType)
        {
            // create and write out
            IList<Foo> records = MakeRecords(GetTestFooObject());

            MemoryStream dataFileOutputStream = new MemoryStream();

            Schema schema = Schema.Parse(schemaStr);
            DatumWriter<Foo> writer = new SpecificWriter<Foo>(schema);
            int numRecords = 0;
            List<SyncLog> syncLogs = new List<SyncLog>();
            using (IFileWriter<Foo> dataFileWriter = DataFileWriter<Foo>.OpenWriter(writer, dataFileOutputStream, Codec.CreateCodec(codecType)))
            {
                dataFileWriter.Flush();
                syncLogs.Add(new SyncLog { Position = dataFileOutputStream.Position - DataFileConstants.SyncSize + 1, RemainingRecords = numRecords });
                long lastPosition = dataFileOutputStream.Position;
                for (int i = 0; i < 10; ++i)
                {
                    foreach (Foo foo in records)
                    {
                        dataFileWriter.Append(foo);
                        if (dataFileOutputStream.Position != lastPosition)
                        {
                            syncLogs.Add(new SyncLog { Position = dataFileOutputStream.Position - DataFileConstants.SyncSize + 1, RemainingRecords = numRecords });
                            lastPosition = dataFileOutputStream.Position;
                        }
                        numRecords++;
                    }

                    // write out block
                    if (i == 1 || i == 4)
                    {
                        dataFileWriter.Sync();
                        syncLogs.Add(new SyncLog { Position = dataFileOutputStream.Position - DataFileConstants.SyncSize + 1, RemainingRecords = numRecords });
                        lastPosition = dataFileOutputStream.Position;
                    }
                }
                dataFileWriter.Flush();
                syncLogs.Add(new SyncLog { Position = dataFileOutputStream.Position, RemainingRecords = numRecords });
            }

            MemoryStream dataFileInputStream = new MemoryStream(dataFileOutputStream.ToArray());

            // read back
            using (IFileReader<Foo> reader = DataFileReader<Foo>.OpenReader(dataFileInputStream))
            {
                long curPosition = 0;

                foreach (SyncLog syncLog in syncLogs)
                {
                    int expectedRecords = numRecords - syncLog.RemainingRecords;
                    long nextSyncPoint = syncLog.Position;
                    AssertNumRecordsFromPosition( reader, curPosition, expectedRecords );
                    AssertNumRecordsFromPosition( reader, nextSyncPoint - 1, expectedRecords );
                    curPosition = nextSyncPoint;
                }
            }
        }

        /// <summary>
        /// Test leaveOpen flag
        /// </summary>
        /// <param name="schemaStr"></param>
        /// <param name="codecType"></param>
        /// <param name="leaveWriteOpen"></param>
        /// <param name="leaveReadOpen"></param>
        [Test]
        public void TestLeaveOpen([Values(specificSchema)] string schemaStr, [Values] Codec.Type codecType, [Values] bool leaveWriteOpen, [Values] bool leaveReadOpen)
        {
            // create and write out
            IList<Foo> records = MakeRecords(GetTestFooObject());

            byte[] inputBuffer;
            using (MemoryStream dataFileOutputStream = new MemoryStream())
            {
                Schema schema = Schema.Parse(schemaStr);
                DatumWriter<Foo> writer = new SpecificWriter<Foo>(schema);
                using (IFileWriter<Foo> dataFileWriter = DataFileWriter<Foo>.OpenWriter(writer, dataFileOutputStream, Codec.CreateCodec(codecType), leaveWriteOpen))
                {
                    dataFileWriter.Flush();
                }

                try
                {
                    // Check if stream is still valid and not closed
                    // If opened with leaveOpen=false, it should throw an exception
                    Assert.AreNotEqual(dataFileOutputStream.Length, 0);
                    dataFileOutputStream.Flush();

                    // If we get here we must have used leaveOpen=true
                    Assert.True(leaveWriteOpen);

                }
                catch(System.ObjectDisposedException)
                {
                    // If we get here we must have used leaveOpen=false
                    Assert.False(leaveWriteOpen);
                }

                inputBuffer = dataFileOutputStream.ToArray();
            }

            using (MemoryStream dataFileInputStream = new MemoryStream(inputBuffer))
            {
                // read back
                using (IFileReader<Foo> reader = DataFileReader<Foo>.OpenReader(dataFileInputStream, leaveReadOpen))
                {
                }

                try
                {
                    // Check if stream is still valid and not closed
                    // If opened with leaveOpen=false, it should throw an exception
                    Assert.AreNotEqual(dataFileInputStream.Length, 0);

                    // If we get here we must have used leaveOpen=true
                    Assert.True(leaveReadOpen);

                }
                catch(System.ObjectDisposedException)
                {
                    // If we get here we must have used leaveOpen=false
                    Assert.False(leaveReadOpen);
                }
            }
        }

        class SyncLog
        {
            public long Position { get; set; }
            public int RemainingRecords { get; set; }
        }

        private static void AssertNumRecordsFromPosition( IFileReader<Foo> reader, long position, int expectedRecords )
        {
            // move to next block from position
            reader.Sync( position );

            int readRecords = 0;
            // read records from synced position
            foreach( Foo rec in reader.NextEntries )
            {
                readRecords++;
            }
            Assert.AreEqual( expectedRecords, readRecords, "didn't read expected records from position " + position );
        }

        private static IEnumerable<TestCaseData> TestSyncAndSeekPositionsSource()
        {
            foreach (Codec.Type codecType in Enum.GetValues(typeof(Codec.Type)))
            {
                yield return new TestCaseData(specificSchema, codecType, 2, 0, 1).SetName("{m}({1},{2},{3},{4})");
                yield return new TestCaseData(specificSchema, codecType, 10, 1, 4).SetName("{m}({1},{2},{3},{4})");
                yield return new TestCaseData(specificSchema, codecType, 200, 111, 15).SetName("{m}({1},{2},{3},{4})");
                yield return new TestCaseData(specificSchema, codecType, 1000, 588, 998).SetName("{m}({1},{2},{3},{4})");
            }
        }

        /// <summary>
        /// Reading all sync positions and
        /// verifying them with subsequent seek
        /// positions
        /// </summary>
        [TestCaseSource(nameof(TestSyncAndSeekPositionsSource))]
        public void TestSyncAndSeekPositions(string schemaStr, Codec.Type codecType, int iterations, int firstSyncPosition, int secondSyncPosition)
        {
            // create and write out
            IList<Foo> records = MakeRecords(GetTestFooObject());
            MemoryStream dataFileOutputStream = new MemoryStream();

            Schema schema = Schema.Parse(schemaStr);
            DatumWriter<Foo> writer = new SpecificWriter<Foo>(schema);
            using (IFileWriter<Foo> dataFileWriter = DataFileWriter<Foo>.OpenWriter(writer, dataFileOutputStream, Codec.CreateCodec(codecType)))
            {
                for (int i = 0; i < iterations; ++i)
                {
                    foreach (Foo foo in records)
                        dataFileWriter.Append(foo);

                    // write out block
                    if (i == firstSyncPosition || i == secondSyncPosition)
                        dataFileWriter.Sync();
                }
            }

            MemoryStream dataFileInputStream = new MemoryStream(dataFileOutputStream.ToArray());

            // read syncs
            IList<long> syncs = new List<long>();
            using (IFileReader<Foo> reader = DataFileReader<Foo>.OpenReader(dataFileInputStream))
            {
                long previousSync = -1;

                foreach (Foo foo in reader.NextEntries)
                {
                    if (reader.PreviousSync() != previousSync
                     && reader.Tell() != reader.PreviousSync()) // EOF
                    {
                        previousSync = reader.PreviousSync();
                        syncs.Add(previousSync);
                    }
                }

                // verify syncs wth seeks
                reader.Sync(0); // first sync
                Assert.AreEqual(reader.PreviousSync(), syncs[0],
                              string.Format("Error syncing reader to position: {0}", syncs[0]));

                foreach (long sync in syncs) // the rest
                {
                    reader.Seek(sync);
                    Foo foo = reader.Next();
                    Assert.IsNotNull(foo, string.Format("Error seeking to sync position: {0}", sync));
                }
            }
        }

        [TestCase]
        public void TestDifferentReaderSchema()
        {
            RecordSchema writerSchema = Schema.Parse( "{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"string\"},"
                + "{\"name\":\"f2\", \"type\":\"string\"}]}" ) as RecordSchema;
            Schema readerSchema = Schema.Parse( "{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"string\"},"
                +"{\"name\":\"f3\", \"type\":\"string\", \"default\":\"test\"}]}" );

            foreach(var rwFactory in GenericOptions<GenericRecord>())
            {
                MemoryStream dataFileOutputStream = new MemoryStream();

                using (var writer = rwFactory.CreateWriter(dataFileOutputStream, writerSchema, Codec.CreateCodec(Codec.Type.Null)))
                {
                    writer.Append(mkRecord(new [] { "f1", "f1val", "f2", "f2val" }, writerSchema));
                }

                MemoryStream dataFileInputStream = new MemoryStream(dataFileOutputStream.ToArray());

                using (IFileReader<GenericRecord> reader = rwFactory.CreateReader(dataFileInputStream, readerSchema))
                {
                    GenericRecord result = reader.Next();
                    object ignore;
                    Assert.IsFalse(result.TryGetValue("f2", out ignore));
                    Assert.AreEqual("f1val", result["f1"]);
                    Assert.AreEqual("test", result["f3"]);
                }
            }
        }

        /// <summary>
        /// Reading and writing using optional codecs
        /// </summary>
        /// <param name="schemaStr"></param>
        /// <param name="recs"></param>
        [TestCase("zstd", true)]
        [TestCase("deflate", false)]
        [TestCase("null", false)]
        [TestCase("snappy", false)]
        [TestCase("bzip2", false)]
        [TestCase("xz", false)]
        [TestCase("zstandard", false)]
        public void TestOptionalCodecs(string codecToUse, bool expectResolverProvidedCodec)
        {
            var resolverProvidedCodec = false;

            var fakeCodec = new FakeZstdCodec();
            Codec codecResolver(string codecString)
            {
                if (codecString == "zstd")
                {
                    resolverProvidedCodec = true;
                    return fakeCodec;
                }

                return null;
            }

            Codec.RegisterResolver(codecResolver);

            RecordSchema schema = Schema.Parse( "{\"type\":\"record\", \"name\":\"n\", \"fields\":[{\"name\":\"f1\", \"type\":\"string\"},"
                + "{\"name\":\"f2\", \"type\":\"string\"}]}" ) as RecordSchema;

            foreach(var rwFactory in GenericOptions<GenericRecord>())
            {
                using (MemoryStream dataFileOutputStream = new MemoryStream())
                {
                    using (var writer = rwFactory.CreateWriter(dataFileOutputStream, schema, fakeCodec))
                    {
                        writer.Append(mkRecord(new [] { "f1", "f1val", "f2", "f2val" }, schema));
                    }

                    using (var dataFileInputStream = new MemoryStream(dataFileOutputStream.ToArray()))
                    using (IFileReader<GenericRecord> reader = rwFactory.CreateReader(dataFileInputStream, schema))
                    {
                        GenericRecord result = reader.Next();
                        Assert.AreEqual("f1val", result["f1"]);
                        Assert.AreEqual("f2val", result["f2"]);
                    }
                }
            }

            Assert.AreEqual(expectResolverProvidedCodec, resolverProvidedCodec);
        }

        [TestCase("")]
        [TestCase("blahblahblah")]
        public void UnknownCodecFromStringException(string codec)
        {
            Assert.Throws(typeof(AvroRuntimeException), () => Codec.CreateCodecFromString(codec));
        }

        [TestCase((Codec.Type)(-1))] // "Invalid" Codec.Type
        public void UnknownCodecFromType(Codec.Type codec)
        {
            Assert.Throws(typeof(AvroRuntimeException), () => Codec.CreateCodec(codec));
        }

        [TestCase("deflate")]
        [TestCase("null")]
        [TestCase(null)] // If codec is absent, it is assumed to be "null"
        [TestCase("snappy")]
        [TestCase("bzip2")]
        [TestCase("xz")]
        [TestCase("zstandard")]
        public void KnownCodecFromString(string codec)
        {
            Assert.NotNull(Codec.CreateCodecFromString(codec));
        }

        [Test]
        public void KnownCodecFromType([Values] Codec.Type codec)
        {
            Assert.NotNull(Codec.CreateCodec(codec));
        }

        private bool CheckPrimitive<T>(Stream input, T value, ReaderWriterSet<T>.ReaderFactory createReader)
        {
            IFileReader<T> reader = createReader(input, null);
            IList<T> readFoos = new List<T>();
            foreach (T foo in reader.NextEntries)
            {
                readFoos.Add(foo);
            }
            return (readFoos.Count > 0 &&
                CheckPrimitiveEquals(value, readFoos[0]));
        }

        private bool CheckPrimitiveEquals(object first, object second)
        {
            if (first is IList)
            {
                var firstList = (IList) first;
                var secondList = (IList) second;
                if (firstList.Count != secondList.Count)
                {
                    return false;
                }
                for (int i = 0; i < firstList.Count; i++)
                {
                    if (!CheckPrimitiveEquals(firstList[i], secondList[i]))
                    {
                        return false;
                    }
                }
                return true;
            }
            return (first == null && second == null) || (first.Equals(second));
        }

        private static GenericRecord mkRecord(object[] kv, RecordSchema s)
        {
            GenericRecord input = new GenericRecord(s);
            for (int i = 0; i < kv.Length; i += 2)
            {
                string fieldName = (string)kv[i];
                object fieldValue = kv[i + 1];
                Schema inner = s[fieldName].Schema;
                if (inner is EnumSchema)
                {
                    GenericEnum ge = new GenericEnum(inner as EnumSchema, (string)fieldValue);
                    fieldValue = ge;
                }
                else if (inner is FixedSchema)
                {
                    GenericFixed gf = new GenericFixed(inner as FixedSchema);
                    gf.Value = (byte[])fieldValue;
                    fieldValue = gf;
                }
                input.Add(fieldName, fieldValue);
            }
            return input;
        }

        private IList<Foo> MakeRecords(object[] recs)
        {
            IList<Foo> records = new List<Foo>();

            foreach (object obj in recs)
            {
                object[] inner = (object[])obj;
                Foo newFoo = new Foo { name = (String)inner[0], age = (int)inner[1] };
                records.Add(newFoo);
            }
            return records;
        }

        private bool ValidateMetaData<T>(IFileReader<T> reader,
                                         string key,
                                         object expected,
                                         bool useTypeGetter)
        {
            byte[] valueBytes = reader.GetMeta(key);

            if (expected is byte[])
            {
                Byte[] expectedBytes = new Byte[valueBytes.Length];
                expectedBytes = (byte[])expected;
                return Enumerable.SequenceEqual(expectedBytes, valueBytes);
            }
            else if (expected is long)
            {
                if (useTypeGetter)
                    return ((long)expected == reader.GetMetaLong(key));
                else
                    return ((long)expected == long.Parse(System.Text.Encoding.UTF8.GetString(valueBytes)));
            }
            else
            {
                if (useTypeGetter)
                    return ((string)expected == reader.GetMetaString(key));
                else
                    return ((string)expected == System.Text.Encoding.UTF8.GetString(valueBytes));
            }
        }

        private void SetMetaData(IFileWriter<Foo> dataFileWriter, string key, object value)
        {
            if (value is byte[])
                dataFileWriter.SetMeta(key, (byte[])value);
            else if (value is long)
                dataFileWriter.SetMeta(key, (long)value);
            else
                dataFileWriter.SetMeta(key, (string)value);
        }

        private object[] GetTestFooObject()
        {
            return new object[] { new object[] {"John", 23}, new object[] { "Jane", 99 }, new object[] { "Jeff", 88 },
                                  new object[] {"James", 13}, new object[] { "June", 109 }, new object[] { "Lloyd", 18 },
                                  new object[] {"Jamie", 53}, new object[] { "Fanessa", 101 }, new object[] { "Kan", 18 },
                                  new object[] {"Janey", 33}, new object[] { "Deva", 102 }, new object[] { "Gavin", 28 },
                                  new object[] {"Lochy", 113}, new object[] { "Nickie", 10 }, new object[] { "Liddia", 38 },
                                  new object[] {"Fred", 3}, new object[] { "April", 17 }, new object[] { "Novac", 48 },
                                  new object[] {"Idan", 33}, new object[] { "Jolyon", 76 }, new object[] { "Ant", 68 },
                                  new object[] {"Ernie", 43}, new object[] { "Joel", 99 }, new object[] { "Dan", 78 },
                                  new object[] {"Dave", 103}, new object[] { "Hillary", 79 }, new object[] { "Grant", 88 },
                                  new object[] {"JJ", 14}, new object[] { "Bill", 90 }, new object[] { "Larry", 4 },
                                  new object[] {"Jenny", 3}, new object[] { "Bob", 9 }, new object[] { null, 48 }};
        }

        private static IEnumerable<ReaderWriterSet<T>> SpecificOptions<T>()
        {
            yield return new ReaderWriterSet<T>
            {
                CreateReader = (stream, schema) => DataFileReader<T>.OpenReader(stream, schema),
                CreateWriter = (stream, schema, codec) =>
                    DataFileWriter<T>.OpenWriter(new SpecificWriter<T>(schema), stream, codec),
                CreateAppendWriter = (inStream, outStream, schema) =>
                    DataFileWriter<T>.OpenAppendWriter(new SpecificWriter<T>(schema), inStream, outStream)
            };

            yield return new ReaderWriterSet<T>
                             {
                                 CreateReader = (stream, schema) => DataFileReader<T>.OpenReader(stream, schema,
                                     (ws, rs) => new SpecificDatumReader<T>(ws, rs)),
                                 CreateWriter = (stream, schema, codec) =>
                                     DataFileWriter<T>.OpenWriter(new SpecificDatumWriter<T>(schema), stream, codec ),
                                 CreateAppendWriter = (inStream, outStream, schema) =>
                                     DataFileWriter<T>.OpenAppendWriter(new SpecificDatumWriter<T>(schema), inStream, outStream)
            };
        }

        private static IEnumerable<ReaderWriterSet<T>> GenericOptions<T>()
        {
            yield return new ReaderWriterSet<T>
                             {
                                 CreateReader = (stream, schema) => DataFileReader<T>.OpenReader(stream, schema),
                                 CreateWriter = (stream, schema, codec) =>
                                     DataFileWriter<T>.OpenWriter(new GenericWriter<T>(schema), stream, codec ),
                                 CreateAppendWriter = (inStream, outStream, schema) =>
                                     DataFileWriter<T>.OpenAppendWriter(new GenericWriter<T>(schema), inStream, outStream)
                             };

            yield return new ReaderWriterSet<T>
                             {
                                 CreateReader = (stream, schema) => DataFileReader<T>.OpenReader(stream, schema,
                                     (ws, rs) => new GenericDatumReader<T>(ws, rs)),
                                 CreateWriter = (stream, schema, codec) =>
                                     DataFileWriter<T>.OpenWriter(new GenericDatumWriter<T>(schema), stream, codec ),
                                 CreateAppendWriter = (inStream, outStream, schema) =>
                                    DataFileWriter<T>.OpenAppendWriter(new GenericDatumWriter<T>(schema), inStream, outStream)
                             };
        }

        class ReaderWriterSet<T>
        {
            public delegate IFileWriter<T> WriterFactory(Stream stream, Schema writerSchema, Codec codec);
            public delegate IFileReader<T> ReaderFactory(Stream stream, Schema readerSchema);
            public delegate IFileWriter<T> AppendFactory(Stream inStream, Stream outStream, Schema writerSchema);

            public WriterFactory CreateWriter { get; set; }
            public ReaderFactory CreateReader { get; set; }
            public AppendFactory CreateAppendWriter { get; set; }
        }
    }


    // Foo (Specific)
    public class Foo : ISpecificRecord
    {
        public string name { get; set; }
        public int age { get; set; }

        public Schema Schema
        {
            get
            {
                return Schema.Parse("{\"type\":\"record\",\"name\":\"Foo\",\"namespace\":\"Avro.Test.File\"," +
                                    "\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}");
            }
        }

        public object Get(int fieldPos)
        {
            switch (fieldPos)
            {
                case 0:
                    return name;
                case 1:
                    return age;
            }
            throw new Exception("Invalid index " + fieldPos);
        }

        public void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0:
                    name = (string)fieldValue;
                    break;
                case 1:
                    age = (int) fieldValue;
                    break;
                default:
                    throw new Exception("Invalid index " + fieldPos);
            }
        }

        public override string ToString()
        {
            return string.Format("Name: {0}, Age: {1}", name, age);
        }
    }

    class FakeZstdCodec : Codec
    {
        private DeflateCodec _codec = new DeflateCodec();
        public override byte[] Compress(byte[] uncompressedData)
        {
            return _codec.Compress(uncompressedData);
        }

        public override void Compress(MemoryStream inputStream, MemoryStream outputStream)
        {
            _codec.Compress(inputStream, outputStream);
        }

        public override byte[] Decompress(byte[] compressedData, int length)
        {
            return _codec.Decompress(compressedData, length);
        }

        public override bool Equals(object other)
        {
            if (other == null) return false;

            return this == other;
        }

        public override int GetHashCode()
        {
            return GetName().GetHashCode();
        }

        public override string GetName()
        {
            return "zstd";
        }
    }
}
