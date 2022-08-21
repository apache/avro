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

namespace Avro.IO
{
    /// <summary>
    /// Base class for <a href="parsing/package-summary.html">parser</a>-based
    /// <seealso cref="Encoder"/>s.
    /// </summary>
    public abstract class ParsingEncoder : Encoder
    {
        /// <summary>
        /// Tracks the number of items that remain to be written in the collections
        /// (array or map).
        /// </summary>
        private long[] counts = new long[10];

        protected int Pos = -1;

        public abstract void WriteNull();

        public abstract void WriteBoolean(bool value);

        public abstract void WriteInt(int value);

        public abstract void WriteLong(long value);

        public abstract void WriteFloat(float value);

        public abstract void WriteDouble(double value);

        public abstract void WriteBytes(byte[] value);

        public abstract void WriteBytes(byte[] value, int offset, int length);

        public abstract void WriteString(string value);

        public abstract void WriteEnum(int value);

        public abstract void WriteArrayStart();

        public abstract void WriteArrayEnd();

        public abstract void WriteMapStart();

        public abstract void WriteMapEnd();

        public abstract void WriteUnionIndex(int value);

        public abstract void WriteFixed(byte[] data);

        public abstract void WriteFixed(byte[] data, int start, int len);

        public abstract void Flush();

        public void SetItemCount(long value)
        {
            if (counts[Pos] != 0)
            {
                throw new AvroTypeException("Incorrect number of items written. " + counts[Pos] +
                                            " more required.");
            }

            counts[Pos] = value;
        }

        public void StartItem()
        {
            counts[Pos]--;
        }

        /// <summary>
        /// Push a new collection on to the stack. </summary>
        protected void Push()
        {
            if (++Pos == counts.Length)
            {
                Array.Resize(ref counts, Pos + 10);
            }

            counts[Pos] = 0;
        }

        protected void Pop()
        {
            if (counts[Pos] != 0)
            {
                throw new AvroTypeException("Incorrect number of items written. " + counts[Pos] + " more required.");
            }

            Pos--;
        }

        protected int Depth()
        {
            return Pos;
        }
    }
}
