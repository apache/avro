﻿/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace Avro.File
{
    abstract public class Codec
    {
        /// <summary>
        /// Compress data using implemented codec
        /// </summary>
        /// <param name="uncompressedData"></param>
        /// <returns></returns>
        abstract public byte[] Compress(byte[] uncompressedData);

        /// <summary>
        /// Decompress data using implemented codec
        /// </summary>
        /// <param name="compressedData"></param>
        /// <returns></returns>
        abstract public byte[] Decompress(byte[] compressedData);

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
            Deflate,
            //Snappy
            Null
        };

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