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

namespace Avro.File.Snappy
{
    /// <summary>
    /// Implements Snappy compression and decompression.
    /// </summary>
    public class SnappyCodec : Codec
    {
        /// <inheritdoc/>
        /// <inheritdoc/>
        public override byte[] Compress(byte[] uncompressedData)
        {
            using (MemoryStream outputStream = new MemoryStream())
            {
                byte[] compressedData = IronSnappy.Snappy.Encode(uncompressedData);
                outputStream.Write(compressedData, 0, compressedData.Length);

                var crc = ByteSwap(Crc32.Compute(uncompressedData));
                outputStream.Write(BitConverter.GetBytes(crc), 0, 4);

                return outputStream.ToArray();
            }
        }

        /// <inheritdoc/>
        public override void Compress(MemoryStream inputStream, MemoryStream outputStream)
        {
            inputStream.Position = 0;

            byte[] uncompressedData = inputStream.ToArray();
            byte[] compressedData = IronSnappy.Snappy.Encode(uncompressedData);

            outputStream.SetLength(0);

            outputStream.Write(compressedData, 0, compressedData.Length);

            var crc = ByteSwap(Crc32.Compute(uncompressedData));
            outputStream.Write(BitConverter.GetBytes(crc), 0, 4);
        }

        /// <inheritdoc/>
        public override byte[] Decompress(byte[] compressedData, int blockLength)
        {
            byte[] uncompressedData = IronSnappy.Snappy.Decode(compressedData.AsSpan(0, blockLength - 4));

            return ByteSwap(Crc32.Compute(uncompressedData)) == BitConverter.ToUInt32(compressedData, blockLength - 4) ?
                uncompressedData :
                throw new IOException("Checksum failure");
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
            return this == other || GetType().Name == other.GetType().Name;
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return GetName().GetHashCode();
        }
    }
}
