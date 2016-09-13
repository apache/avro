﻿/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.IO;
using System.Net;
using System.Security.Cryptography;
using NUnit.Framework;
using Avro.ipc;


namespace Avro.test.ipc
{
    [TestFixture]
    public class HttpTransceiverTest
    {
        class MockStream : Stream
        {
            public Stack<byte[]> readResults = new Stack<byte[]>();

            public override bool CanRead
            {
                get { throw new NotImplementedException(); }
            }

            public override bool CanSeek
            {
                get { throw new NotImplementedException(); }
            }

            public override bool CanWrite
            {
                get { throw new NotImplementedException(); }
            }

            public override long Length
            {
                get { throw new NotImplementedException(); }
            }

            public override long Position
            {
                get { throw new NotImplementedException(); }

                set { throw new NotImplementedException(); }
            }

            public override void Flush()
            {
                throw new NotImplementedException();
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                byte[] a = this.readResults.Pop();
                System.Array.Copy(a, 0, buffer, offset, a.Length);
                return a.Length;
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotImplementedException();
            }

            public override void SetLength(long value)
            {
                throw new NotImplementedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new NotImplementedException();
            }
        }

        [Test]
        public void ReadFully()
        {
            MockStream mockStream = new MockStream();
            Random random = new Random();
            byte[] part1 = new byte[25];
            random.NextBytes(part1);
            byte[] part2 = new byte[75];
            random.NextBytes(part2);
            mockStream.readResults.Push(part1);
            mockStream.readResults.Push(part2);

            byte[] buffer = new byte[100];
            HttpTransceiver.ReadFully(mockStream, buffer);
        }

        [Test]
        [ExpectedException(typeof(IOException))]
        public void ReadIntShortBuffer()
        {
            byte[] intBuffer = new byte[4];
            MemoryStream memoryStream = new MemoryStream();
            memoryStream.WriteByte(1);
            memoryStream.WriteByte(12);
            int value = HttpTransceiver.ReadInt(memoryStream, intBuffer);
        }

        [Test]
        public void ReadBuffers()
        {
            byte[] intBuffer = new byte[4];
            IList<MemoryStream> streams = new List<MemoryStream>();
            IList<String> hashes = new List<string>();

            Random random = new Random();
            for (int i = 0; i < 5; i++)
            {
                int length = random.Next(64 * 1024, 512 * 1024);
                byte[] buffer = new byte[length];
                random.NextBytes(buffer);
                using (SHA1CryptoServiceProvider hashAlg = new SHA1CryptoServiceProvider())
                {
                    hashes.Add(BitConverter.ToString(hashAlg.ComputeHash(buffer)));
                }
                streams.Add(new MemoryStream(buffer));
            }
            Stream iostream = new MemoryStream();
            HttpTransceiver.WriteBuffers(streams, iostream);
            iostream.Seek(0L, SeekOrigin.Begin);
            IList<MemoryStream> outputStreams = HttpTransceiver.ReadBuffers(iostream, intBuffer);
            Assert.AreEqual(5, outputStreams.Count);

            for(int i=0;i<outputStreams.Count;i++)
            {
                MemoryStream memoryStream = outputStreams[i];
                String expectedHash = hashes[i];
                byte[] buffer = memoryStream.ToArray();
                String actualHash;
                using (SHA1CryptoServiceProvider hashAlg = new SHA1CryptoServiceProvider())
                {
                    actualHash = BitConverter.ToString(hashAlg.ComputeHash(buffer));
                }
                Assert.AreEqual(expectedHash, actualHash, "Stream " + i + " does not match.");
            }



        }
    }
}