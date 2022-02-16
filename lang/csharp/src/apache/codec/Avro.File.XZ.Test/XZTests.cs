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

namespace Avro.File.XZ.Test
{
    public class Tests
    {
        private static int[] _testLengths = new int[] { 0, 1000, 64 * 1024, 1 * 1024 * 1024 };

        [Test, Combinatorial]
        public void CompressDecompress([ValueSource(nameof(_testLengths))] int length, [Values] XZLevel level)
        {
            byte[] data = Enumerable.Range(0, length).Select(x => (byte)x).ToArray();

            XZCodec codec = new XZCodec(level);

            byte[] compressed = codec.Compress(data);
            byte[] uncompressed = codec.Decompress(compressed, compressed.Length);

            CollectionAssert.AreEqual(data, uncompressed);
        }

        [Test, Combinatorial]
        public void CompressDecompressStream([ValueSource(nameof(_testLengths))] int length, [Values] XZLevel level)
        {
            byte[] data = Enumerable.Range(0, length).Select(x => (byte)x).ToArray();

            XZCodec codec = new XZCodec(level);

            using (MemoryStream inputStream = new MemoryStream(data))
            using (MemoryStream outputStream = new MemoryStream())
            {
                codec.Compress(inputStream, outputStream);

                byte[] compressed = outputStream.ToArray();
                byte[] uncompressed = codec.Decompress(compressed, compressed.Length);

                CollectionAssert.AreEqual(data, uncompressed);
            }
        }

        [Test]
        public void ToStringAndName([Values] XZLevel level)
        {
            XZCodec codec = new XZCodec(level);

            Assert.AreEqual("xz", codec.GetName());
            Assert.AreEqual($"xz-{(int)level}", codec.ToString());
        }
    }
}
