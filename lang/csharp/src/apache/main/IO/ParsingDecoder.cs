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

using Avro.IO.Parsing;

namespace Avro.IO
{
    /// <summary>
    /// Base class for <a href="parsing/package-summary.html">parser</a>-based
    /// <seealso cref="Decoder"/>s.
    /// </summary>
    public abstract class ParsingDecoder : Decoder, Parser.ActionHandler, SkipParser.SkipHandler
    {
        /// <inheritdoc />
        public abstract void ReadNull();

        /// <inheritdoc />
        public abstract bool ReadBoolean();

        /// <inheritdoc />
        public abstract int ReadInt();

        /// <inheritdoc />
        public abstract long ReadLong();

        /// <inheritdoc />
        public abstract float ReadFloat();

        /// <inheritdoc />
        public abstract double ReadDouble();

        /// <inheritdoc />
        public abstract byte[] ReadBytes();

        /// <inheritdoc />
        public abstract string ReadString();

        /// <inheritdoc />
        public abstract int ReadEnum();

        /// <inheritdoc />
        public abstract long ReadArrayStart();

        /// <inheritdoc />
        public abstract long ReadArrayNext();

        /// <inheritdoc />
        public abstract long ReadMapStart();

        /// <inheritdoc />
        public abstract long ReadMapNext();

        /// <inheritdoc />
        public abstract int ReadUnionIndex();

        /// <inheritdoc />
        public abstract void ReadFixed(byte[] buffer);

        /// <inheritdoc />
        public abstract void ReadFixed(byte[] buffer, int start, int length);

        /// <inheritdoc />
        public abstract void SkipNull();

        /// <inheritdoc />
        public abstract void SkipBoolean();

        /// <inheritdoc />
        public abstract void SkipInt();

        /// <inheritdoc />
        public abstract void SkipLong();

        /// <inheritdoc />
        public abstract void SkipFloat();

        /// <inheritdoc />
        public abstract void SkipDouble();

        /// <inheritdoc />
        public abstract void SkipBytes();

        /// <inheritdoc />
        public abstract void SkipString();

        /// <inheritdoc />
        public abstract void SkipEnum();

        /// <inheritdoc />
        public abstract void SkipUnionIndex();

        /// <inheritdoc />
        public abstract void SkipFixed(int len);

        /// <summary>
        ///  Skips an array on the stream.
        /// </summary>
        public abstract void SkipArray();

        /// <summary>
        ///  Skips a map on the stream.
        /// </summary>
        public abstract void SkipMap();

        /// <inheritdoc />
        public abstract Symbol DoAction(Symbol input, Symbol top);

        /// <summary>
        /// The parser.
        /// </summary>
        protected readonly SkipParser Parser;

        /// <summary>
        /// Initializes a new instance of the <see cref="ParsingDecoder"/> class.
        /// </summary>
        protected ParsingDecoder(Symbol root)
        {
            this.Parser = new SkipParser(root, this, this);
        }

        /// <summary>
        ///  Skips a fixed type on the stream.
        /// </summary>
        protected abstract void SkipFixed();

        /// <inheritdoc />
        public virtual void SkipAction()
        {
            Parser.PopSymbol();
        }

        /// <inheritdoc />
        public virtual void SkipTopSymbol()
        {
            Symbol top = Parser.TopSymbol();
            if (top == Symbol.Null)
            {
                ReadNull();
            }
            else if (top == Symbol.Boolean)
            {
                ReadBoolean();
            }
            else if (top == Symbol.Int)
            {
                ReadInt();
            }
            else if (top == Symbol.Long)
            {
                ReadLong();
            }
            else if (top == Symbol.Float)
            {
                ReadFloat();
            }
            else if (top == Symbol.Double)
            {
                ReadDouble();
            }
            else if (top == Symbol.String)
            {
                SkipString();
            }
            else if (top == Symbol.Bytes)
            {
                SkipBytes();
            }
            else if (top == Symbol.Enum)
            {
                ReadEnum();
            }
            else if (top == Symbol.Fixed)
            {
                SkipFixed();
            }
            else if (top == Symbol.Union)
            {
                ReadUnionIndex();
            }
            else if (top == Symbol.ArrayStart)
            {
                SkipArray();
            }
            else if (top == Symbol.MapStart)
            {
                SkipMap();
            }
        }
    }
}
