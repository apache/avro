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
using System.IO;
using System.Linq;
using NUnit.Framework;

namespace Avro.File.Snappy.Test
{
    public class Tests
    {
        private static readonly int[] _testLengths = new int[] { 0, 1000, 64 * 1024, 100000 };

        [TestCaseSource(nameof(_testLengths))]
        public void CompressDecompress(int length)
        {
            byte[] data = Enumerable.Range(0, length).Select(x => (byte)x).ToArray();

            SnappyCodec codec = new SnappyCodec();

            byte[] compressed = codec.Compress(data);
            byte[] uncompressed = codec.Decompress(compressed, compressed.Length);

            Assert.IsTrue(Enumerable.SequenceEqual(data, uncompressed));
        }

        [TestCaseSource(nameof(_testLengths))]
        public void CompressDecompressStream(int length)
        {
            byte[] data = Enumerable.Range(0, length).Select(x => (byte)x).ToArray();

            SnappyCodec codec = new SnappyCodec();

            using (MemoryStream inputStream = new MemoryStream(data))
            using (MemoryStream outputStream = new MemoryStream())
            {
                codec.Compress(inputStream, outputStream);

                byte[] compressed = outputStream.ToArray();
                byte[] uncompressed = codec.Decompress(compressed, compressed.Length);

                Assert.IsTrue(Enumerable.SequenceEqual(data, uncompressed));
            }
        }

        [Test]
        public void ToStringAndName()
        {
            SnappyCodec codec = new SnappyCodec();

            Assert.AreEqual("snappy", codec.GetName());
            Assert.AreEqual("snappy", codec.ToString());
        }

        [Test]
        public void Equal()
        {
            SnappyCodec codec1 = new SnappyCodec();
            SnappyCodec codec2 = new SnappyCodec();

            Assert.IsTrue(codec1.Equals(codec1));
            Assert.IsTrue(codec2.Equals(codec2));
            Assert.IsTrue(codec1.Equals(codec2));
            Assert.IsTrue(codec2.Equals(codec1));
        }

        [Test]
        public void HashCode()
        {
            SnappyCodec codec = new SnappyCodec();

            Assert.AreNotEqual(0, codec.GetHashCode());
        }
    }
}
