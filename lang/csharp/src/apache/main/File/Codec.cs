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
using System.Globalization;
using System.IO;
using System.Reflection;

namespace Avro.File
{
    /// <summary>
    /// Base class for Avro-supported compression codecs for data files. Note that Codec objects may
    /// maintain internal state (e.g. buffers) and are not thread safe.
    /// </summary>
    public abstract class Codec
    {
        /// <summary>
        /// Default upper bound, in bytes, on the size a single data-file block may
        /// decompress to. A block with a very high compression ratio (or a malformed
        /// block) can otherwise expand to far more memory than its compressed size.
        /// Mirrors the Java SDK's decompression limit (AVRO-4247). Overridable with
        /// the AVRO_MAX_DECOMPRESS_LENGTH environment variable.
        /// </summary>
        public static readonly long DefaultMaxDecompressLength = 200L * 1024 * 1024; // 200 MiB

        /// <summary>
        /// Name of the environment variable used to override the default maximum
        /// decompressed size of a single block.
        /// </summary>
        public const string MaxDecompressLengthEnvVar = "AVRO_MAX_DECOMPRESS_LENGTH";

        /// <summary>
        /// The maximum number of bytes a single block is allowed to decompress to.
        /// </summary>
        /// <returns>The configured limit, honoring the environment override.</returns>
        public static long GetMaxDecompressLength()
        {
            var value = Environment.GetEnvironmentVariable(MaxDecompressLengthEnvVar);
            if (value != null && long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) && parsed > 0)
            {
                return parsed;
            }

            return DefaultMaxDecompressLength;
        }

        /// <summary>
        /// Throws if the given decompressed length exceeds the maximum allowed.
        /// </summary>
        /// <param name="length">The number of decompressed bytes.</param>
        /// <param name="maxLength">The maximum number of decompressed bytes allowed.</param>
        public static void CheckDecompressLength(long length, long maxLength)
        {
            if (length > maxLength)
            {
                throw new AvroRuntimeException(
                    $"Decompressed block size {length} exceeds the maximum allowed of {maxLength} bytes. " +
                    $"Set the {MaxDecompressLengthEnvVar} environment variable to raise the limit.");
            }
        }

        /// <summary>
        /// Copies a decompression stream to the destination, rejecting the block as
        /// soon as its decompressed size would exceed <paramref name="maxLength"/> so
        /// an over-large (or malicious) block is not fully materialized in memory.
        /// </summary>
        /// <param name="source">The decompression stream to read from.</param>
        /// <param name="destination">The stream to write the decompressed data to.</param>
        /// <param name="maxLength">The maximum number of decompressed bytes allowed.</param>
        public static void CopyBounded(Stream source, Stream destination, long maxLength)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            if (destination == null)
            {
                throw new ArgumentNullException(nameof(destination));
            }

            if (maxLength < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxLength), "maxLength must not be negative.");
            }

            byte[] buffer = new byte[81920];
            long total = 0;
            int read;
            while ((read = source.Read(buffer, 0, buffer.Length)) > 0)
            {
                // Pre-add bound check: total is always <= maxLength here and
                // read > 0, so maxLength - total >= 0 and this cannot overflow.
                // Rejecting before adding stops total from overflowing and
                // wrapping past the limit for a very large maxLength.
                if (read > maxLength - total)
                {
                    throw new AvroRuntimeException(
                        $"Decompressed block size exceeds the maximum allowed of {maxLength} bytes. " +
                        $"Set the {MaxDecompressLengthEnvVar} environment variable to raise the limit.");
                }

                total += read;
                destination.Write(buffer, 0, read);
            }
        }

        /// <summary>
        /// Compress data using implemented codec.
        /// </summary>
        /// <param name="uncompressedData">The uncompressed data.</param>
        /// <returns>
        /// byte array.
        /// </returns>
        public abstract byte[] Compress(byte[] uncompressedData);

        /// <summary>
        /// Compress data using implemented codec.
        /// </summary>
        /// <param name="inputStream">The stream which contains the data to be compressed.</param>
        /// <param name="outputStream">A reusable stream which will hold the compressed data. That stream should be empty.</param>
        public abstract void Compress(MemoryStream inputStream, MemoryStream outputStream);

        /// <summary>
        /// Decompress data using implemented codec.
        /// </summary>
        /// <param name="compressedData">The buffer holding data to decompress.</param>
        /// <returns>A byte array holding the decompressed data.</returns>
        [Obsolete]
        public virtual byte[] Decompress(byte[] compressedData)
        {
            return Decompress(compressedData, compressedData.Length);
        }

        /// <summary>
        /// Decompress data using implemented codec
        /// </summary>
        /// <param name="compressedData">The buffer holding data to decompress.</param>
        /// <param name="length">The actual length of bytes to decompress from the buffer.</param>
        /// <returns>A byte array holding the decompressed data.</returns>
        public abstract byte[] Decompress(byte[] compressedData, int length);

        /// <summary>
        /// Name of this codec type.
        /// </summary>
        /// <returns>The codec name.</returns>
        public abstract string GetName();

        /// <summary>
        /// Codecs must implement an equals() method.
        /// </summary>
        /// <param name="other">The <see cref="object" /> to compare with this instance.</param>
        /// <returns>
        ///   <c>true</c> if the specified <see cref="object" /> is equal to this instance; otherwise, <c>false</c>.
        /// </returns>
        public abstract override bool Equals(object other);

        /// <summary>
        /// Codecs must implement a HashCode() method that is
        /// consistent with Equals.
        /// </summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table.
        /// </returns>
        public abstract override int GetHashCode();

        /// <summary>
        /// Codec types.
        /// </summary>
        public enum Type
        {
            /// <summary>
            /// Codec type that implements the "deflate" compression algorithm.
            /// </summary>
            Deflate,

            /// <summary>
            /// Codec that does not perform any compression.
            /// </summary>
            Null,

            /// <summary>
            /// Codec type that implements the "Snappy" compression algorithm.
            /// </summary>
            Snappy,

            /// <summary>
            /// Codec type that implements the "BZip2" compression algorithm.
            /// </summary>
            BZip2,

            /// <summary>
            /// Codec type that implements the "XZ" compression algorithm.
            /// </summary>
            XZ,

            /// <summary>
            /// Codec type that implements the "Zstandard" compression algorithm.
            /// </summary>
            Zstandard
        }

        /// <summary>
        /// Represents a function capable of resolving a codec string
        /// with a matching codec implementation a reader can use to decompress data.
        /// </summary>
        /// <param name="codecMetaString">The codec string</param>
        public delegate Codec CodecResolver(string codecMetaString);

        /// <summary>
        /// The codec resolvers
        /// </summary>
        private static readonly List<CodecResolver> _codecResolvers = new List<CodecResolver>();

        /// <summary>
        /// Registers a function that will attempt to resolve a codec identifying string
        /// with a matching codec implementation when reading compressed Avro data.
        /// </summary>
        /// <param name="resolver">A function that is able to find a codec implementation for a given codec string</param>
        public static void RegisterResolver(CodecResolver resolver)
        {
            _codecResolvers.Add(resolver);
        }

        /// <summary>
        /// Factory method to return child codec instance based on Codec.Type.
        /// </summary>
        /// <param name="codecType">Type of the codec.</param>
        /// <returns>
        /// Codec based on type.
        /// </returns>
        public static Codec CreateCodec(Type codecType)
        {
            switch (codecType)
            {
                case Type.Deflate:
                    return new DeflateCodec();
                case Type.Null:
                    return new NullCodec();
                case Type.Snappy:
                case Type.BZip2:
                case Type.XZ:
                case Type.Zstandard:
                    {
                        // Create codec dynamically from "Avro.File.CODECNAME" assembly
                        Assembly assembly = Assembly.Load($"Avro.File.{codecType}");
                        return assembly.CreateInstance($"Avro.File.{codecType}.{codecType}Codec") as Codec;
                    }
            }

            throw new AvroRuntimeException($"Unrecognized codec: {codecType}");
        }

        /// <summary>
        /// Factory method to return child codec instance based on string type.
        /// </summary>
        /// <param name="codecType">Type of the codec.</param>
        /// <returns>Codec based on type.</returns>
        public static Codec CreateCodecFromString(string codecType)
        {
            if (codecType == null)
            {
                // If codec is absent, it is assumed to be "null"
                // https://avro.apache.org/docs/current/spec.html
                return CreateCodec(Type.Null);
            }

            foreach (var resolver in _codecResolvers)
            {
                var candidateCodec = resolver(codecType);
                if (candidateCodec != null)
                {
                    return candidateCodec;
                }
            }

            switch (codecType)
            {
                case DataFileConstants.DeflateCodec:
                    return CreateCodec(Type.Deflate);
                case DataFileConstants.NullCodec:
                    return CreateCodec(Type.Null);
                case DataFileConstants.SnappyCodec:
                    return CreateCodec(Type.Snappy);
                case DataFileConstants.BZip2Codec:
                    return CreateCodec(Type.BZip2);
                case DataFileConstants.XZCodec:
                    return CreateCodec(Type.XZ);
                case DataFileConstants.ZstandardCodec:
                    return CreateCodec(Type.Zstandard);
            }

            throw new AvroRuntimeException($"Unrecognized codec: {codecType}");
        }

        /// <summary>
        /// Returns name of codec.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return GetName();
        }
    }
}
