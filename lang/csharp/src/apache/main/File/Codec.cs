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

using System.Collections.Generic;
using System.IO;

namespace Avro.File
{
    /// <summary>
    /// Base class for Avro-supported compression codecs for data files. Note that Codec objects may
    /// maintain internal state (e.g. buffers) and are not thread safe.
    /// </summary>
    public abstract class Codec
    {
        /// <summary>
        /// Compress data using implemented codec
        /// </summary>
        /// <param name="uncompressedData"></param>
        /// <returns></returns>
        abstract public byte[] Compress(byte[] uncompressedData);

        /// <summary>
        /// Compress data using implemented codec
        /// </summary>
        /// <param name="inputStream">The stream which contains the data to be compressed</param>
        /// <param name="outputStream">A reusable stream which will hold the compressed data. That stream should be empty.</param>
        abstract public void Compress(MemoryStream inputStream, MemoryStream outputStream);

        /// <summary>
        /// Decompress data using implemented codec
        /// </summary>
        /// <param name="compressedData">The buffer holding data to decompress.</param>
        /// <returns></returns>
        abstract public byte[] Decompress(byte[] compressedData);
        
        /// <summary>
        /// Decompress data using implemented codec
        /// </summary>
        /// <param name="compressedData">The buffer holding data to decompress.</param>
        /// <param name="length">The actual length of bytes to decompress from the buffer.</param>
        /// <returns></returns>
        abstract public byte[] Decompress(byte[] compressedData, int length);

        /// <summary>
        /// Name of this codec type
        /// </summary>
        /// <returns></returns>
        abstract public string GetName();

        /// <summary>
        ///  Codecs must implement an equals() method
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        abstract public override bool Equals(object other);

        /// <summary>
        /// Codecs must implement a HashCode() method that is
        /// consistent with Equals
        /// </summary>
        /// <returns></returns>
        abstract public override int GetHashCode();

        /// <summary>
        /// Codec types
        /// </summary>
        public enum Type
        {
            /// <summary>
            /// Codec type that implments the "deflate" compression algorithm.
            /// </summary>
            Deflate,

            //Snappy

            /// <summary>
            /// Codec that does not perform any compression.
            /// </summary>
            Null
        };

        /// <summary>
        /// Represents a function capable of resolving a codec string
        /// with a matching codec implementation a reader can use to decompress data.
        /// </summary>
        /// <param name="codecMetaString">The codec string</param>
        public delegate Codec CodecResolver(string codecMetaString);

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
        /// Factory method to return child
        /// codec instance based on Codec.Type
        /// </summary>
        /// <param name="codecType"></param>
        /// <returns></returns>
        public static Codec CreateCodec(Type codecType)
        {
            switch (codecType)
            {
                case Type.Deflate:
                    return new DeflateCodec();
                default:
                    return new NullCodec();
            }
        }

        /// <summary>
        /// Factory method to return child
        /// codec instance based on string type
        /// </summary>
        /// <param name="codecType"></param>
        /// <returns></returns>
        public static Codec CreateCodecFromString(string codecType)
        {
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
                    return new DeflateCodec();
                default:
                    return new NullCodec();
            }
        }

        /// <summary>
        /// Returns name of codec
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return GetName();
        }
    }
}
