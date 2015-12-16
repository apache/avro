/**
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
using System.IO;

namespace Avro.IO
{
    /// <summary>
    /// Decoder for Avro binary format
    /// </summary>
    public class BinaryDecoder : Decoder
    {
        private readonly Stream stream;

        public BinaryDecoder(Stream stream)
        {
            this.stream = stream;
        }

        /// <summary>
        /// null is written as zero bytes
        /// </summary>
        public void ReadNull()
        {
        }

        /// <summary>
        /// a boolean is written as a single byte 
        /// whose value is either 0 (false) or 1 (true).
        /// </summary>
        /// <returns></returns>
        public bool ReadBoolean()
        {
            byte b = read();
            if (b == 0) return false;
            if (b == 1) return true;
            throw new AvroException("Not a boolean value in the stream: " + b);
        }

        /// <summary>
        /// int and long values are written using variable-length, zig-zag coding.
        /// </summary>
        /// <param name="?"></param>
        /// <returns></returns>
        public int ReadInt()
        {
            return (int)ReadLong();
        }
        /// <summary>
        /// int and long values are written using variable-length, zig-zag coding.
        /// </summary>
        /// <param name="?"></param>
        /// <returns></returns>
        public long ReadLong()
        {
            byte b = read();
            ulong n = b & 0x7FUL;
            int shift = 7;
            while ((b & 0x80) != 0)
            {
                b = read();
                n |= (b & 0x7FUL) << shift;
                shift += 7;
            }
            long value = (long)n;
            return (-(value & 0x01L)) ^ ((value >> 1) & 0x7fffffffffffffffL);
        }

        /// <summary>
        /// A float is written as 4 bytes.
        /// The float is converted into a 32-bit integer using a method equivalent to
        /// Java's floatToIntBits and then encoded in little-endian format.
        /// </summary>
        /// <returns></returns>
        public float ReadFloat()
        {
            byte[] buffer = read(4);

            if (!BitConverter.IsLittleEndian)
                Array.Reverse(buffer);

            return BitConverter.ToSingle(buffer, 0);

            //int bits = (Stream.ReadByte() & 0xff |
            //(Stream.ReadByte()) & 0xff << 8 |
            //(Stream.ReadByte()) & 0xff << 16 |
            //(Stream.ReadByte()) & 0xff << 24);
            //return intBitsToFloat(bits);
        }

        /// <summary>
        /// A double is written as 8 bytes.
        /// The double is converted into a 64-bit integer using a method equivalent to
        /// Java's doubleToLongBits and then encoded in little-endian format.
        /// </summary>
        /// <param name="?"></param>
        /// <returns></returns>
        public double ReadDouble()
        {
            long bits = (stream.ReadByte() & 0xffL) |
              (stream.ReadByte() & 0xffL) << 8 |
              (stream.ReadByte() & 0xffL) << 16 |
              (stream.ReadByte() & 0xffL) << 24 |
              (stream.ReadByte() & 0xffL) << 32 |
              (stream.ReadByte() & 0xffL) << 40 |
              (stream.ReadByte() & 0xffL) << 48 |
              (stream.ReadByte() & 0xffL) << 56;
             return BitConverter.Int64BitsToDouble(bits);
        }

        /// <summary>
        /// Bytes are encoded as a long followed by that many bytes of data. 
        /// </summary>
        /// <returns></returns>
        public byte[] ReadBytes()
        {
            return read(ReadLong());
        }

        public string ReadString()
        {
            int length = ReadInt();
            byte[] buffer = new byte[length];
            //TODO: Fix this because it's lame;
            ReadFixed(buffer);
            return System.Text.Encoding.UTF8.GetString(buffer);
        }

        public int ReadEnum()
        {
            return ReadInt();
        }

        public long ReadArrayStart()
        {
            return doReadItemCount();
        }

        public long ReadArrayNext()
        {
            return doReadItemCount();
        }

        public long ReadMapStart()
        {
            return doReadItemCount();
        }

        public long ReadMapNext()
        {
            return doReadItemCount();
        }

        public int ReadUnionIndex()
        {
            return ReadInt();
        }

        public void ReadFixed(byte[] buffer)
        {
            ReadFixed(buffer, 0, buffer.Length);
        }

        public void ReadFixed(byte[] buffer, int start, int length)
        {
            Read(buffer, start, length);
        }

        public void SkipNull()
        {
            ReadNull();
        }

        public void SkipBoolean()
        {
            ReadBoolean();
        }


        public void SkipInt()
        {
            ReadInt();
        }

        public void SkipLong()
        {
            ReadLong();
        }

        public void SkipFloat()
        {
            Skip(4);
        }

        public void SkipDouble()
        {
            Skip(8);
        }

        public void SkipBytes()
        {
            Skip(ReadLong());
        }

        public void SkipString()
        {
            SkipBytes();
        }

        public void SkipEnum()
        {
            ReadLong();
        }

        public void SkipUnionIndex()
        {
            ReadLong();
        }

        public void SkipFixed(int len)
        {
            Skip(len);
        }

        // Read p bytes into a new byte buffer
        private byte[] read(long p)
        {
            byte[] buffer = new byte[p];
            Read(buffer, 0, buffer.Length);
            return buffer;
        }

        private static float intBitsToFloat(int value)
        {
            return BitConverter.ToSingle(BitConverter.GetBytes(value), 0);
        }

        private byte read()
        {
            int n = stream.ReadByte();
            if (n >= 0) return (byte)n;
            throw new AvroException("End of stream reached");
        }

        private void Read(byte[] buffer, int start, int len)
        {
            while (len > 0)
            {
                int n = stream.Read(buffer, start, len);
                if (n <= 0) throw new AvroException("End of stream reached");
                start += n;
                len -= n;
            }
        }

        private long doReadItemCount()
        {
            long result = ReadLong();
            if (result < 0)
            {
                ReadLong(); // Consume byte-count if present
                result = -result;
            }
            return result;
        }

        private void Skip(int p)
        {
            stream.Seek(p, SeekOrigin.Current);
        }

        private void Skip(long p)
        {
            stream.Seek(p, SeekOrigin.Current);
        }

        internal void skip(long block_size)
        {
            throw new NotImplementedException();
        }

    }
}
