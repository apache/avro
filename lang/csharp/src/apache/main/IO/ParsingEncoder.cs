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

        /// <summary>
        /// Position into the counts stack.
        /// </summary>
        protected int Pos = -1;

        /// <inheritdoc />
        public abstract void WriteNull();

        /// <inheritdoc />
        public abstract void WriteBoolean(bool value);

        /// <inheritdoc />
        public abstract void WriteInt(int value);

        /// <inheritdoc />
        public abstract void WriteLong(long value);

        /// <inheritdoc />
        public abstract void WriteFloat(float value);

        /// <inheritdoc />
        public abstract void WriteDouble(double value);

        /// <inheritdoc />
        public abstract void WriteBytes(byte[] value);

        /// <inheritdoc />
        public abstract void WriteBytes(byte[] value, int offset, int length);

        /// <inheritdoc />
        public abstract void WriteString(string value);

        /// <inheritdoc />
        public abstract void WriteEnum(int value);

        /// <inheritdoc />
        public abstract void WriteArrayStart();

        /// <inheritdoc />
        public abstract void WriteArrayEnd();

        /// <inheritdoc />
        public abstract void WriteMapStart();

        /// <inheritdoc />
        public abstract void WriteMapEnd();

        /// <inheritdoc />
        public abstract void WriteUnionIndex(int value);

        /// <inheritdoc />
        public abstract void WriteFixed(byte[] data);

        /// <inheritdoc />
        public abstract void WriteFixed(byte[] data, int start, int len);

        /// <inheritdoc />
        public abstract void Flush();

        /// <inheritdoc />
        public void SetItemCount(long value)
        {
            if (counts[Pos] != 0)
            {
                throw new AvroTypeException("Incorrect number of items written. " + counts[Pos] +
                                            " more required.");
            }

            counts[Pos] = value;
        }

        /// <inheritdoc />
        public void StartItem()
        {
            counts[Pos]--;
        }

        /// <summary>
        /// Push a new collection on to the stack.
        /// </summary>
        protected void Push()
        {
            if (++Pos == counts.Length)
            {
                Array.Resize(ref counts, Pos + 10);
            }

            counts[Pos] = 0;
        }

        /// <summary>
        /// Pop a new collection on to the stack.
        /// </summary>
        protected void Pop()
        {
            if (counts[Pos] != 0)
            {
                throw new AvroTypeException("Incorrect number of items written. " + counts[Pos] + " more required.");
            }

            Pos--;
        }

        /// <summary>
        /// Returns the position into the stack.
        /// </summary>
        protected int Depth()
        {
            return Pos;
        }
    }
}
