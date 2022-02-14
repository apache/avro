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
using System.Collections.Generic;
using System.IO;

namespace Avro.IO
{
    /// <summary>
    /// Utility to present <see cref="MemoryStream" />s as an <see cref="InputStream" />.
    /// </summary>
    /// <seealso cref="InputStream" />
    /// <seealso cref="ByteBufferOutputStream" />
    public class ByteBufferInputStream : InputStream
    {
        private readonly IList<MemoryStream> _buffers;
        private int _currentBuffer;

        /// <summary>
        /// Initializes a new instance of a <see cref="ByteBufferInputStream" />.
        /// </summary>
        /// <param name="buffers">The buffers.</param>
        public ByteBufferInputStream(IList<MemoryStream> buffers)
        {
            _buffers = buffers;
        }

        /// <inheritdoc/>
        public override int Read(byte[] buffer, int offset, int count)
        {
            if (count == 0)
            {
                return 0;
            }

            MemoryStream nextBuffer = GetNextNonEmptyBuffer();
            long remaining = nextBuffer.Length - nextBuffer.Position;
            if (count > remaining)
            {
                int remainingCheck = nextBuffer.Read(buffer, offset, (int)remaining);

                return remainingCheck != remaining ?
                    throw new InvalidDataException($"remainingCheck [{remainingCheck}] and remaining[{remaining}] are different.") :
                    (int)remaining;
            }

            int lenCheck = nextBuffer.Read(buffer, offset, count);

            return lenCheck != count ?
                throw new InvalidDataException($"lenCheck [{lenCheck}] and len[{count}] are different.") :
                count;
        }

        /// <summary>
        /// Gets the next non empty buffer.
        /// </summary>
        /// <returns>
        /// Memory Stream of next non empty buffer
        /// </returns>
        /// <exception cref="EndOfStreamException"></exception>
        private MemoryStream GetNextNonEmptyBuffer()
        {
            while (_currentBuffer < _buffers.Count)
            {
                MemoryStream buffer = _buffers[_currentBuffer];
                if (buffer.Position < buffer.Length)
                {
                    return buffer;
                }

                _currentBuffer++;
            }

            throw new EndOfStreamException();
        }

        /// <summary>
        /// Throws a <see cref="NotSupportedException" />.
        /// </summary>
        /// <exception cref="NotSupportedException"></exception>
        public override long Length
        {
            get { throw new NotSupportedException(); }
        }
    }
}
