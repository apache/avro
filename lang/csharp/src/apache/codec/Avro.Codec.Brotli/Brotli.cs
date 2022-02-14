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
using BrotliSharpLib;

namespace Avro.Codec.Brotli
{
    /// <summary>
    /// Brotli Compression level
    /// </summary>
    public enum BrotliLevel
    {
        Default = 1,
        Level0 = 0,
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
        Level11 = 11
    }

    /// <summary>
    /// Implements Brotli compression and decompression.
    /// </summary>
    public class BrotliCodec : File.Codec
    {
        public const string DataFileConstant = "brotli";

        private readonly BrotliLevel _level;

        public BrotliCodec()
            : this(BrotliLevel.Default)
        {
        }

        public BrotliCodec(BrotliLevel level)
        {
            _level = level;
        }

        /// <inheritdoc/>
        public override byte[] Compress(byte[] uncompressedData)
        {
            return BrotliSharpLib.Brotli.CompressBuffer(uncompressedData, 0, uncompressedData.Length, (int)_level);
        }

        /// <inheritdoc/>
        public override void Compress(MemoryStream inputStream, MemoryStream outputStream)
        {
            outputStream.SetLength(0);
            using (BrotliStream brotliStreams = new BrotliStream(outputStream, System.IO.Compression.CompressionMode.Compress, false))
            {
                brotliStreams.SetQuality((int)_level);
                inputStream.CopyTo(brotliStreams);
            }
        }

        /// <inheritdoc/>
        public override byte[] Decompress(byte[] compressedData, int blockLength)
        {
            return BrotliSharpLib.Brotli.DecompressBuffer(compressedData, 0, blockLength);
        }

        /// <inheritdoc/>
        public override string GetName()
        {
            return DataFileConstant;
        }

        /// <inheritdoc/>
        public override bool Equals(object other)
        {
            return this == other || GetType().Name == other.GetType().Name;
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return 0;
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"{GetName()}-{(int)_level}";
        }
    }
}
