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

namespace Avro.Codec.Snappy
{
    /// <summary>
    /// Implements a 32-bit CRC hash algorithm.
    /// </summary>
    internal static class Crc32
    {
        const uint DefaultPolynomial = 0xedb88320u;
        const uint DefaultSeed = 0xffffffffu;

        static uint[] defaultTable;

        public static uint Compute(byte[] buffer)
        {
            return Compute(DefaultPolynomial, DefaultSeed, buffer);
        }

        public static uint Compute(uint polynomial, uint seed, ReadOnlySpan<byte> buffer)
        {
            return ~CalculateHash(InitializeTable(polynomial), seed, buffer);
        }

        static uint[] InitializeTable(uint polynomial)
        {
            if (polynomial == DefaultPolynomial && defaultTable != null)
                return defaultTable;

            uint[] createTable = new uint[256];
            for (int i = 0; i < 256; i++)
            {
                uint entry = (uint)i;
                for (int j = 0; j < 8; j++)
                    if ((entry & 1) == 1)
                        entry = (entry >> 1) ^ polynomial;
                    else
                        entry >>= 1;
                createTable[i] = entry;
            }

            if (polynomial == DefaultPolynomial)
                defaultTable = createTable;

            return createTable;
        }

        static uint CalculateHash(uint[] table, uint seed, ReadOnlySpan<byte> buffer)
        {
            uint hash = seed;
            for (int i = 0; i < buffer.Length; i++)
                hash = (hash >> 8) ^ table[buffer[i] ^ hash & 0xff];
            return hash;
        }
    }
}
