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
using System.IO.Compression;
using Zstandard.Net;

namespace Avro.File.Zstandard
{
    /// <summary>
    /// Zstandard Compression level
    /// </summary>
    public enum ZstandardLevel
    {
        Level1 = 1,
        Level2 = 2,
        Level3 = 3,
        Level4 = 4,
        Level5 = 5,
        Level6 = 6,
        Level7 = 7,
        Level8 = 8,
        Level9 = 9,
        Level10 = 10,
        Level11 = 11,
        Level12 = 12,
        Level13 = 13,
        Level14 = 14,
        Level15 = 15,
        Level16 = 16,
        Level17 = 17,
        Level18 = 18,
        Level19 = 19,
        Default = Level3,
        Minimum = Level1,
        Maximum = Level19
    }

    /// <summary>
    /// Implements Zstandard compression and decompression.
    /// </summary>
    public class ZstandardCodec : Codec
    {
        public ZstandardLevel Level {get; private set;}

        public ZstandardCodec()
            : this(ZstandardLevel.Default)
        {
        }

        public ZstandardCodec(ZstandardLevel level)
        {
            Level = level;
        }

        /// <inheritdoc/>
        public override byte[] Compress(byte[] uncompressedData)
        {
            using (var outputStream = new MemoryStream())
            using (var compressionStream = new ZstandardStream(outputStream, CompressionMode.Compress))
            {
                compressionStream.CompressionLevel = (int)Level;
                compressionStream.Write(uncompressedData, 0, uncompressedData.Length);
                compressionStream.Flush();
                return outputStream.ToArray();
            }
        }

        /// <inheritdoc/>
        public override void Compress(MemoryStream inputStream, MemoryStream outputStream)
        {
            inputStream.Position = 0;
            outputStream.SetLength(0);

            using (var compressionStream = new ZstandardStream(outputStream, CompressionMode.Compress, true))
            {
                compressionStream.CompressionLevel = (int)Level;
                inputStream.CopyTo(compressionStream);
                compressionStream.Flush();
            }
        }

        /// <inheritdoc/>
        public override byte[] Decompress(byte[] compressedData, int blockLength)
        {
            using (var memoryStream = new MemoryStream(compressedData, 0, blockLength))
            using (var outputStream = new MemoryStream())
            using (var compressionStream = new ZstandardStream(memoryStream, CompressionMode.Decompress))
            {
                compressionStream.CopyTo(outputStream);
                compressionStream.Flush();
                return outputStream.ToArray();
            }
        }

        /// <inheritdoc/>
        public override string GetName()
        {
            return DataFileConstants.ZstandardCodec;
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

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"{GetName()}[{(int)Level}]";
        }
    }
}
