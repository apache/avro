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
using System.IO;
using Avro.Generic;
using Avro.IO;
using NUnit.Framework;

namespace Avro.Test.Generic
{
    /// <summary>
    /// Collection-allocation bounds (AVRO-4295 / AVRO-4306) must also apply to the
    /// <see cref="PreresolvingDatumReader{T}"/> path (the base of
    /// <see cref="GenericDatumReader{T}"/> and
    /// <see cref="Avro.Specific.SpecificDatumReader{T}"/>), not only to the
    /// DefaultReader/GenericReader path. These tests exercise that path directly
    /// via <see cref="GenericDatumReader{T}"/> for the map, zero-byte-element and
    /// non-seekable-stream cases; the huge-array-count case on the specific reader
    /// is covered by SpecificTests.TestSpecificReaderRejectsArrayCountBeyondStream.
    /// </summary>
    [TestFixture]
    class PreresolvingDatumReaderBoundsTests
    {
        private static byte[] Encode(Action<BinaryEncoder> write)
        {
            var ms = new MemoryStream();
            write(new BinaryEncoder(ms));
            return ms.ToArray();
        }

        // A map block declaring far more entries than the stream could hold must be
        // rejected before allocating, on the PreresolvingDatumReader path. Asserting
        // the guard's message ensures the rejection comes from the bounds check
        // (before any allocation), not from a later end-of-stream fault.
        [TestCase]
        public void MapCountBeyondStreamRejected()
        {
            var schema = Schema.Parse("{\"type\":\"map\",\"values\":\"long\"}");
            // One block declaring 1,000,000 entries, followed by no data.
            byte[] malicious = Encode(enc => enc.WriteLong(1000000));

            var reader = new GenericDatumReader<object>(schema, schema);
            var ex = Assert.Throws<AvroException>(
                () => reader.Read(null, new BinaryDecoder(new MemoryStream(malicious))));
            Assert.That(ex.Message, Does.Contain("bytes are available"));
        }

        // Zero-byte elements (array of null) consume no input, so the
        // bytes-remaining check cannot bound them; the cumulative item cap must.
        [TestCase]
        public void ArrayOfNullHugeCountRejected()
        {
            var schema = Schema.Parse("{\"type\":\"array\",\"items\":\"null\"}");
            // 20,000,000 null items (zero bytes each) exceeds the 10,000,000 item cap.
            byte[] malicious = Encode(enc => enc.WriteLong(20000000));

            var reader = new GenericDatumReader<object>(schema, schema);
            var ex = Assert.Throws<AvroException>(
                () => reader.Read(null, new BinaryDecoder(new MemoryStream(malicious))));
            Assert.That(ex.Message, Does.Contain("zero-byte elements"));
        }

        // A legitimate small array of null must still be read, not falsely rejected.
        [TestCase]
        public void ArrayOfNullSmallCountReads()
        {
            var schema = Schema.Parse("{\"type\":\"array\",\"items\":\"null\"}");
            byte[] data = Encode(enc =>
            {
                enc.WriteLong(3); // block of 3 null items
                enc.WriteLong(0); // terminator
            });

            var reader = new GenericDatumReader<object>(schema, schema);
            var result = (object[])reader.Read(null, new BinaryDecoder(new MemoryStream(data)));
            Assert.AreEqual(3, result.Length);
            Assert.IsNull(result[0]);
            Assert.IsNull(result[2]);
        }

        // A valid collection on a non-seekable stream (RemainingBytes() == -1, so
        // the bytes-available check is skipped) must still be read correctly.
        [TestCase]
        public void NonSeekableStreamStillReads()
        {
            var schema = Schema.Parse("{\"type\":\"array\",\"items\":\"long\"}");
            byte[] data = Encode(enc =>
            {
                enc.WriteLong(3); // block of 3 items
                enc.WriteLong(1);
                enc.WriteLong(2);
                enc.WriteLong(3);
                enc.WriteLong(0); // terminator
            });

            var reader = new GenericDatumReader<object>(schema, schema);
            using (var ns = new NonSeekableStream(new MemoryStream(data)))
            {
                var result = (object[])reader.Read(null, new BinaryDecoder(ns));
                Assert.AreEqual(3, result.Length);
                Assert.AreEqual(1L, result[0]);
                Assert.AreEqual(2L, result[1]);
                Assert.AreEqual(3L, result[2]);
            }
        }

        // Minimal read-only, forward-only stream wrapper reporting CanSeek=false.
        private sealed class NonSeekableStream : Stream
        {
            private readonly Stream inner;
            public NonSeekableStream(Stream inner) { this.inner = inner; }
            public override bool CanRead => true;
            public override bool CanSeek => false;
            public override bool CanWrite => false;
            public override long Length => throw new NotSupportedException();
            public override long Position
            {
                get => throw new NotSupportedException();
                set => throw new NotSupportedException();
            }
            public override void Flush() { }
            public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);
            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    inner.Dispose();
                }

                base.Dispose(disposing);
            }
        }
    }
}
