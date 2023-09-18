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

namespace Avro.File
{
    /// <summary>
    /// Implements deflate compression and decompression.
    /// </summary>
    /// <seealso cref="Codec" />
    /// <seealso cref="DeflateStream" />
    public class DeflateCodec : Codec
    {
        /// <inheritdoc/>
        public override byte[] Compress(byte[] uncompressedData)
        {
            MemoryStream outStream = new MemoryStream();

            using (DeflateStream Compress =
                        new DeflateStream(outStream,
                        CompressionMode.Compress))
            {
                Compress.Write(uncompressedData, 0, uncompressedData.Length);
            }

            return outStream.ToArray();
        }

        /// <inheritdoc/>
        public override void Compress(MemoryStream inputStream, MemoryStream outputStream)
        {
            outputStream.SetLength(0);
            using (DeflateStream Compress =
                        new DeflateStream(outputStream,
                        CompressionMode.Compress,
                        true))
            {
                Compress.Write(inputStream.GetBuffer(), 0, (int)inputStream.Length);
            }
        }

        /// <inheritdoc/>
        public override byte[] Decompress(byte[] compressedData, int length)
        {
            using (MemoryStream inStream = new MemoryStream(compressedData, 0, length))
            using (MemoryStream outStream = new MemoryStream())
            {
                using (DeflateStream decompress = new DeflateStream(inStream, CompressionMode.Decompress))
                {
                    decompress.CopyTo(outStream);
                }
                return outStream.ToArray();
            }
        }

        /// <inheritdoc/>
        public override string GetName()
        {
            return DataFileConstants.DeflateCodec;
        }

        /// <inheritdoc/>
        public override bool Equals(object other)
        {
            return this == other || GetType().Name == other.GetType().Name;
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return DataFileConstants.DeflateCodecHash;
        }
    }
}
