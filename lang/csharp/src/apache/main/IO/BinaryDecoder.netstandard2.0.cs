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
using System.Text;

namespace Avro.IO
{
    /// <content>
    /// Contains the netstandard2.0 specific functionality for BinaryDecoder.
    /// </content>
    public partial class BinaryDecoder
    {
        /// <summary>
        /// It is hard to find documentation about the real maximum array length in .NET Framework 4.6.1, but this seems to work :-/
        /// </summary>
        /*
         * TODO: look into when gcAllowVeryLargeObjects was introduced.  The check using this may no longer be needed.
         * It was enabled by default with .Net Framework 4.5 onward.
         */
        private const int MaxDotNetArrayLength = 0x3FFFFFFF;

        /// <summary>
        /// A float is written as 4 bytes.
        /// The float is converted into a 32-bit integer using a method equivalent to
        /// Java's floatToIntBits and then encoded in little-endian format.
        /// </summary>
        /// <returns>
        /// The float just read
        /// </returns>
        public float ReadFloat()
        {
            byte[] buffer = read(4);

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(buffer);
            }

            return BitConverter.ToSingle(buffer, 0);
        }

        /// <summary>
        /// A double is written as 8 bytes.
        /// The double is converted into a 64-bit integer using a method equivalent to
        /// Java's doubleToLongBits and then encoded in little-endian format.
        /// </summary>
        /// <returns>
        /// A double value.
        /// </returns>
        public double ReadDouble()
        {
            long bits = (_stream.ReadByte() & 0xffL) |
              (_stream.ReadByte() & 0xffL) << 8 |
              (_stream.ReadByte() & 0xffL) << 16 |
              (_stream.ReadByte() & 0xffL) << 24 |
              (_stream.ReadByte() & 0xffL) << 32 |
              (_stream.ReadByte() & 0xffL) << 40 |
              (_stream.ReadByte() & 0xffL) << 48 |
              (_stream.ReadByte() & 0xffL) << 56;

            return BitConverter.Int64BitsToDouble(bits);
        }

        /// <summary>
        /// Reads a string written by <see cref="BinaryEncoder.WriteString(string)" />.
        /// </summary>
        /// <returns>
        /// String read from the stream.
        /// </returns>
        /// <exception cref="InvalidDataException">Can not deserialize a string with negative length!</exception>
        /// <exception cref="AvroException">
        /// String length is not supported!
        /// or
        /// Unable to read {length} bytes from a byte array of length {bytes.Length}
        /// </exception>
        public string ReadString()
        {
            int length = ReadInt();

            if (length < 0)
            {
                throw new InvalidDataException("Can not deserialize a string with negative length!");
            }

            // TODO: Refer to comments on MaxDotNetArrayLength
            if (length > MaxDotNetArrayLength)
            {
                throw new AvroException("String length is not supported!");
            }

            using (BinaryReader binaryReader = new BinaryReader(_stream, Encoding.UTF8, true))
            {
                byte[] bytes = binaryReader.ReadBytes(length);

                if (bytes.Length != length)
                {
                    throw new AvroException($"Unable to read {length} bytes from a byte array of length {bytes.Length}");
                }

                return Encoding.UTF8.GetString(bytes);
            }
        }

        /// <summary>
        /// Reads the specified buffer.
        /// </summary>
        /// <param name="buffer">The buffer.</param>
        /// <param name="start">The start.</param>
        /// <param name="len">The length.</param>
        /// <exception cref="EndOfStreamException"></exception>
        private void Read(byte[] buffer, int start, int len)
        {
            while (len > 0)
            {
                int n = _stream.Read(buffer, start, len);
                if (n <= 0)
                {
                    throw new EndOfStreamException();
                }

                start += n;
                len -= n;
            }
        }
    }
}
