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
using System.Runtime.InteropServices;
using Joveler.Compression.XZ;

namespace Avro.Codec.XZ
{
    /// <summary>
    /// XZ Compression level
    /// </summary>
    public enum XZLevel
    {
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
        Default = Level6,
        Minimum = Level0,
        Maximum = Level9
    }

    /// <summary>
    /// Implements XZ compression and decompression.
    /// </summary>
    public class XZCodec : File.Codec
    {
        public const string DataFileConstant = "xz";

        private readonly XZLevel _level;
        private readonly bool _extreme;
        private readonly int _threads;

        public XZCodec()
            : this(XZLevel.Default)
        {
        }

        public XZCodec(XZLevel level)
            : this(level, false)
        {
        }

        public XZCodec(XZLevel level, bool extreme)
            : this(level, extreme, 0)
        {
        }

        public XZCodec(XZLevel level, bool extreme, int numOfThreads)
        {
            _level = level;
            _extreme = extreme;
            _threads = numOfThreads;
        }

        // !!!
        // !!! This must be called as a global init before using this library
        // !!! TODO: Make it more convinient
        // !!!
        public static void Initialize()
        {
            string arch = RuntimeInformation.OSArchitecture.ToString().ToLower();
            string foundLibPath = string.Empty;
            string libPath;
            string rid;
            string libName;

            // Determine Platform (needed for proper Runtime ID)
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                rid = $"win-{arch}";
                libName = "liblzma.dll";
            }
            else
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                rid = $"linux-{arch}";
                libName = "liblzma.so";
            }
            else
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                rid = $"osx-{arch}";
                libName = "liblzma.dylib";
            }
            else
            {
                // Unknown platform
                throw new PlatformNotSupportedException("Unknown runtime platform!");
            }

            // Try to search for the lib in the working directory and the application binary directory
            foreach (var relPath in new List<string> { ".", AppDomain.CurrentDomain.BaseDirectory })
            {
                // Try first the lib name directly
                libPath = Path.Combine(relPath, libName);
                if (System.IO.File.Exists(libPath))
                {
                    foundLibPath = libPath;
                    break;
                }

                // Try the runtimes/RID/native location
                // This is the default location for netstandard native libs
                libPath = Path.Combine(relPath, "runtimes", rid, "native", libName);
                if (System.IO.File.Exists(libPath))
                {
                    foundLibPath = libPath;
                    break;
                }
            }

            // Try the OS search path if nothing is found yet
            if (string.IsNullOrEmpty(foundLibPath))
            {
                var values = Environment.GetEnvironmentVariable("PATH");
                foreach (string path in values.Split(Path.PathSeparator))
                {
                    libPath = Path.Combine(path, libName);
                    if (System.IO.File.Exists(libPath))
                    {
                        foundLibPath = libPath;
                        break;
                    }
                }
            }

            if (string.IsNullOrEmpty(foundLibPath))
                throw new PlatformNotSupportedException($"Unable to find {libName}");

            // Initialize XZ library
            XZInit.GlobalInit(foundLibPath);
        }

        public static void Uninitialize()
        {
            XZInit.GlobalCleanup();
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
            XZCompressOptions compOpts = new XZCompressOptions
            {
                Level = (LzmaCompLevel)(int)_level,
                ExtremeFlag = _extreme,
                LeaveOpen = true
            };

            XZThreadedCompressOptions threadOpts = new XZThreadedCompressOptions
            {
                Threads = _threads,
            };

            outputStream.SetLength(0);

            using (XZStream xzStream = new XZStream(outputStream, compOpts, threadOpts))
            {
                inputStream.CopyTo(xzStream);
                xzStream.Flush();
            }
        }

        /// <inheritdoc/>
        public override byte[] Decompress(byte[] compressedData, int blockLength)
        {
            XZDecompressOptions decompOpts = new XZDecompressOptions();

            using (MemoryStream inputStream = new MemoryStream(compressedData))
            using (MemoryStream outputStream = new MemoryStream())
            using (XZStream xzStream = new XZStream(inputStream, decompOpts))
            {
                xzStream.CopyTo(outputStream);
                xzStream.Flush();
                return outputStream.ToArray();
            }
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
