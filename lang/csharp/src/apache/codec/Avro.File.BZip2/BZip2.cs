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

namespace Avro.File.BZip2
{
    /// <summary>
    /// BZip2 Compression level
    /// </summary>
    public enum BZip2Level
    {
        Default = 9,
        Level1 = 1,
        Level2 = 2,
        Level3 = 3,
        Level4 = 4,
        Level5 = 5,
        Level6 = 6,
        Level7 = 7,
        Level8 = 8,
        Level9 = 9
    }

    /// <summary>
    /// Implements BZip2 compression and decompression.
    /// </summary>
    public class BZip2Codec : Codec
    {
        public BZip2Level Level {get; private set;}

        public BZip2Codec()
            : this(BZip2Level.Default)
        {
        }

        public BZip2Codec(BZip2Level level)
        {
            Level = level;
        }

        /// <inheritdoc/>
        public override byte[] Compress(byte[] uncompressedData)
        {
            using (MemoryStream inputStream = new MemoryStream(uncompressedData))
            using (MemoryStream outputStream = new MemoryStream())
            {
                Compress(inputStream, outputStream);
                return outputStream.ToArray();
            }
        }

        /// <inheritdoc/>
        public override void Compress(MemoryStream inputStream, MemoryStream outputStream)
        {
            inputStream.Position = 0;
            outputStream.SetLength(0);
            ICSharpCode.SharpZipLib.BZip2.BZip2.Compress(inputStream, outputStream, false, (int)Level);
        }

        /// <inheritdoc/>
        public override byte[] Decompress(byte[] compressedData, int blockLength)
        {
            using (MemoryStream inputStream = new MemoryStream(compressedData, 0, blockLength))
            using (MemoryStream outputStream = new MemoryStream())
            {
                ICSharpCode.SharpZipLib.BZip2.BZip2.Decompress(inputStream, outputStream, false);
                return outputStream.ToArray();
            }
        }

        /// <inheritdoc/>
        public override string GetName()
        {
            return DataFileConstants.BZip2Codec;
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
            return $"{GetName()}-{(int)Level}";
        }
    }
}
