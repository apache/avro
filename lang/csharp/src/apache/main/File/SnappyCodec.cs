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

namespace Avro.File
{
    /// <summary>
    /// Implements deflate compression and decompression.
    /// </summary>
    public class SnappyCodec : Codec
    {
        /// <inheritdoc/>
        public override byte[] Compress(byte[] uncompressedData)
        {
            MemoryStream outStream = new MemoryStream();

            byte[] compressed = IronSnappy.Snappy.Encode(uncompressedData);
            outStream.Write(compressed, 0, compressed.Length);

            var crc = ByteSwap(Crc32.Compute(uncompressedData));
            outStream.Write(BitConverter.GetBytes(crc), 0, 4);

            return outStream.ToArray();
        }

        /// <inheritdoc/>
        public override byte[] Decompress(byte[] compressedData, int blockLength)
        {
            byte[] decompressedData = IronSnappy.Snappy.Decode(compressedData.AsSpan(0, blockLength - 4));

            var crc = ByteSwap(Crc32.Compute(decompressedData));
            if (crc != BitConverter.ToUInt32(compressedData, blockLength - 4))
            {
                throw new AvroRuntimeException("CRC32 check failure uncompressing block with Snappy");
            }

            return decompressedData;
        }

        private static uint ByteSwap(uint word)
        {
            return ((word >> 24) & 0x000000FF) | ((word >> 8) & 0x0000FF00) | ((word << 8) & 0x00FF0000) | ((word << 24) & 0xFF000000);
        }

        /// <inheritdoc/>
        public override string GetName()
        {
            return DataFileConstants.SnappyCodec;
        }

        /// <inheritdoc/>
        public override bool Equals(object other)
        {
            if (this == other)
                return true;
            return this.GetType().Name == other.GetType().Name;
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return DataFileConstants.SnappyCodecHash;
        }
    }
}
