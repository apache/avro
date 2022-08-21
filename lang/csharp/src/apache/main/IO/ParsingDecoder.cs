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
        public abstract void ReadNull();

        public abstract bool ReadBoolean();

        public abstract int ReadInt();

        public abstract long ReadLong();

        public abstract float ReadFloat();

        public abstract double ReadDouble();

        public abstract byte[] ReadBytes();

        public abstract string ReadString();

        public abstract int ReadEnum();

        public abstract long ReadArrayStart();

        public abstract long ReadArrayNext();

        public abstract long ReadMapStart();

        public abstract long ReadMapNext();

        public abstract int ReadUnionIndex();

        public abstract void ReadFixed(byte[] buffer);

        public abstract void ReadFixed(byte[] buffer, int start, int length);

        public abstract void SkipNull();

        public abstract void SkipBoolean();

        public abstract void SkipInt();

        public abstract void SkipLong();

        public abstract void SkipFloat();

        public abstract void SkipDouble();

        public abstract void SkipBytes();

        public abstract void SkipString();

        public abstract void SkipEnum();

        public abstract void SkipUnionIndex();

        public abstract void SkipFixed(int len);

        public abstract void SkipArray();

        public abstract void SkipMap();

        public abstract Symbol DoAction(Symbol input, Symbol top);
        protected readonly SkipParser parser;

        protected ParsingDecoder(Symbol root)
        {
            this.parser = new SkipParser(root, this, this);
        }

        protected abstract void SkipFixed();

        public virtual void SkipAction()
        {
            parser.PopSymbol();
        }

        public virtual void SkipTopSymbol()
        {
            Symbol top = parser.TopSymbol();
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
