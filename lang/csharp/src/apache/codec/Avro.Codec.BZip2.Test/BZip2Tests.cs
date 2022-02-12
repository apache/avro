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
using System.Linq;
using NUnit.Framework;

namespace Avro.Codec.BZip2.Test
{
    public class Tests
    {
        [TestCase(0)]
        [TestCase(1000)]
        [TestCase(64 * 1024)]
        [TestCase(1 * 1024 * 1024)]
        public void CompressDecompress(int length)
        {
            byte[] data = Enumerable.Range(0, length).Select(x => (byte)x).ToArray();

            BZip2Codec codec = new BZip2Codec();

            byte[] compressed = codec.Compress(data);
            byte[] uncompressed = codec.Decompress(compressed, compressed.Length);

            CollectionAssert.AreEqual(data, uncompressed);
        }

        [Test]
        [TestCase(BZip2Level.Level1, ExpectedResult = "bzip2-1")]
        [TestCase(BZip2Level.Level2, ExpectedResult = "bzip2-2")]
        [TestCase(BZip2Level.Level3, ExpectedResult = "bzip2-3")]
        public string ToStringAndName(BZip2Level level)
        {
            BZip2Codec codec = new BZip2Codec(level);

            Assert.AreEqual("bzip2", codec.GetName());

            return codec.ToString();
        }
    }
}
