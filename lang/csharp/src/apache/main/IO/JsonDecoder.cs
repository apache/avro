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
using System.Text;
using Avro.IO.Parsing;
using Newtonsoft.Json;

namespace Avro.IO
{
    /// <summary>
    /// A <seealso cref="Decoder"/> for Avro's JSON data encoding.
    ///
    /// JsonDecoder is not thread-safe.
    /// </summary>
    public class JsonDecoder : ParsingDecoder
    {
        private JsonReader reader;
        private readonly Stack<ReorderBuffer> reorderBuffers = new Stack<ReorderBuffer>();
        private ReorderBuffer currentReorderBuffer;

        private class ReorderBuffer
        {
            public readonly IDictionary<string, IList<JsonElement>> SavedFields =
                new Dictionary<string, IList<JsonElement>>();

            public JsonReader OrigParser;
        }

        private JsonDecoder(Symbol root, Stream stream) : base(root)
        {
            Configure(stream);
        }

        private JsonDecoder(Symbol root, string str) : base(root)
        {
            Configure(str);
        }

        public JsonDecoder(Schema schema, Stream stream) : this(getSymbol(schema), stream)
        {
        }

        public JsonDecoder(Schema schema, string str) : this(getSymbol(schema), str)
        {
        }

        private static Symbol getSymbol(Schema schema)
        {
            return (new JsonGrammarGenerator()).Generate(schema);
        }

        /// <summary>
        /// Reconfigures this JsonDecoder to use the InputStream provided.
        /// <p/>
        /// Otherwise, this JsonDecoder will reset its state and then reconfigure its
        /// input.
        /// </summary>
        /// <param name="stream"> The InputStream to read from. Cannot be null. </param>
        /// <returns> this JsonDecoder </returns>
        public JsonDecoder Configure(Stream stream)
        {
            parser.Reset();
            reorderBuffers.Clear();
            currentReorderBuffer = null;
            this.reader = new JsonTextReader(new StreamReader(stream));
            this.reader.Read();
            return this;
        }

        /// <summary>
        /// Reconfigures this JsonDecoder to use the String provided for input.
        /// <p/>
        /// Otherwise, this JsonDecoder will reset its state and then reconfigure its
        /// input.
        /// </summary>
        /// <param name="str"> The String to read from. Cannot be null. </param>
        /// <returns> this JsonDecoder </returns>
        public JsonDecoder Configure(string str)
        {
            parser.Reset();
            reorderBuffers.Clear();
            currentReorderBuffer = null;
            this.reader = new JsonTextReader(new StringReader(str));
            this.reader.Read();
            return this;
        }

        private void advance(Symbol symbol)
        {
            this.parser.ProcessTrailingImplicitActions();
            parser.Advance(symbol);
        }

        public override void ReadNull()
        {
            advance(Symbol.Null);
            if (reader.TokenType == JsonToken.Null)
            {
                reader.Read();
            }
            else
            {
                throw error("null");
            }
        }

        public override bool ReadBoolean()
        {
            advance(Symbol.Boolean);
            if (reader.TokenType == JsonToken.Boolean)
            {
                bool result = Convert.ToBoolean(reader.Value);
                reader.Read();
                return result;
            }
            else
            {
                throw error("boolean");
            }
        }

        public override int ReadInt()
        {
            advance(Symbol.Int);
            if (reader.TokenType == JsonToken.Integer || reader.TokenType == JsonToken.Float)
            {
                int result = Convert.ToInt32(reader.Value);
                reader.Read();
                return result;
            }
            else
            {
                throw error("int");
            }
        }

        public override long ReadLong()
        {
            advance(Symbol.Long);
            if (reader.TokenType == JsonToken.Integer || reader.TokenType == JsonToken.Float)
            {
                long result = Convert.ToInt64(reader.Value);
                reader.Read();
                return result;
            }
            else
            {
                throw error("long");
            }
        }

        public override float ReadFloat()
        {
            advance(Symbol.Float);
            if (reader.TokenType == JsonToken.Integer || reader.TokenType == JsonToken.Float)
            {
                float result = (float)Convert.ToDouble(reader.Value);
                reader.Read();
                return result;
            }
            else
            {
                throw error("float");
            }
        }

        public override double ReadDouble()
        {
            advance(Symbol.Double);
            if (reader.TokenType == JsonToken.Integer || reader.TokenType == JsonToken.Float)
            {
                double result = Convert.ToDouble(reader.Value);
                reader.Read();
                return result;
            }
            else
            {
                throw error("double");
            }
        }

        public override string ReadString()
        {
            advance(Symbol.String);
            if (parser.TopSymbol() == Symbol.MapKeyMarker)
            {
                parser.Advance(Symbol.MapKeyMarker);
                if (reader.TokenType != JsonToken.PropertyName)
                {
                    throw error("map-key");
                }
            }
            else
            {
                if (reader.TokenType != JsonToken.String)
                {
                    throw error("string");
                }
            }

            string result = Convert.ToString(reader.Value);
            reader.Read();
            return result;
        }

        public override void SkipString()
        {
            advance(Symbol.String);
            if (parser.TopSymbol() == Symbol.MapKeyMarker)
            {
                parser.Advance(Symbol.MapKeyMarker);
                if (reader.TokenType != JsonToken.PropertyName)
                {
                    throw error("map-key");
                }
            }
            else
            {
                if (reader.TokenType != JsonToken.String)
                {
                    throw error("string");
                }
            }

            reader.Read();
        }

        public override byte[] ReadBytes()
        {
            advance(Symbol.Bytes);
            if (reader.TokenType == JsonToken.String)
            {
                byte[] result = readByteArray();
                reader.Read();
                return result;
            }
            else
            {
                throw error("bytes");
            }
        }

        private byte[] readByteArray()
        {
            Encoding iso = Encoding.GetEncoding("ISO-8859-1");
            byte[] result = iso.GetBytes(Convert.ToString(reader.Value));
            return result;
        }

        public override void SkipBytes()
        {
            advance(Symbol.Bytes);
            if (reader.TokenType == JsonToken.String)
            {
                reader.Read();
            }
            else
            {
                throw error("bytes");
            }
        }

        private void checkFixed(int size)
        {
            advance(Symbol.Fixed);
            Symbol.IntCheckAction top = (Symbol.IntCheckAction)parser.PopSymbol();
            if (size != top.Size)
            {
                throw new AvroTypeException("Incorrect length for fixed binary: expected " + top.Size +
                                            " but received " + size + " bytes.");
            }
        }

        public override void ReadFixed(byte[] bytes)
        {
            ReadFixed(bytes, 0, bytes.Length);
        }

        public override void ReadFixed(byte[] bytes, int start, int len)
        {
            checkFixed(len);
            if (reader.TokenType == JsonToken.String)
            {
                byte[] result = readByteArray();
                reader.Read();
                if (result.Length != len)
                {
                    throw new AvroTypeException("Expected fixed length " + len + ", but got" + result.Length);
                }

                Array.Copy(result, 0, bytes, start, len);
            }
            else
            {
                throw error("fixed");
            }
        }

        public override void SkipFixed(int length)
        {
            checkFixed(length);
            doSkipFixed(length);
        }

        private void doSkipFixed(int length)
        {
            if (reader.TokenType == JsonToken.String)
            {
                byte[] result = readByteArray();
                reader.Read();
                if (result.Length != length)
                {
                    throw new AvroTypeException("Expected fixed length " + length + ", but got" + result.Length);
                }
            }
            else
            {
                throw error("fixed");
            }
        }

        protected override void SkipFixed()
        {
            advance(Symbol.Fixed);
            Symbol.IntCheckAction top = (Symbol.IntCheckAction)parser.PopSymbol();
            doSkipFixed(top.Size);
        }

        public override int ReadEnum()
        {
            advance(Symbol.Enum);
            Symbol.EnumLabelsAction top = (Symbol.EnumLabelsAction)parser.PopSymbol();
            if (reader.TokenType == JsonToken.String)
            {
                string label = Convert.ToString(reader.Value);
                int n = top.FindLabel(label);
                if (n >= 0)
                {
                    reader.Read();
                    return n;
                }

                throw new AvroTypeException("Unknown symbol in enum " + label);
            }
            else
            {
                throw error("fixed");
            }
        }

        public override long ReadArrayStart()
        {
            advance(Symbol.ArrayStart);
            if (reader.TokenType == JsonToken.StartArray)
            {
                reader.Read();
                return doArrayNext();
            }
            else
            {
                throw error("array-start");
            }
        }

        public override long ReadArrayNext()
        {
            advance(Symbol.ItemEnd);
            return doArrayNext();
        }

        private long doArrayNext()
        {
            if (reader.TokenType == JsonToken.EndArray)
            {
                parser.Advance(Symbol.ArrayEnd);
                reader.Read();
                return 0;
            }
            else
            {
                return 1;
            }
        }

        public override void SkipArray()
        {
            advance(Symbol.ArrayStart);
            if (reader.TokenType == JsonToken.StartArray)
            {
                reader.Skip();
                reader.Read();
                advance(Symbol.ArrayEnd);
            }
            else
            {
                throw error("array-start");
            }
        }

        public override long ReadMapStart()
        {
            advance(Symbol.MapStart);
            if (reader.TokenType == JsonToken.StartObject)
            {
                reader.Read();
                return doMapNext();
            }
            else
            {
                throw error("map-start");
            }
        }

        public override long ReadMapNext()
        {
            advance(Symbol.ItemEnd);
            return doMapNext();
        }

        private long doMapNext()
        {
            if (reader.TokenType == JsonToken.EndObject)
            {
                reader.Read();
                advance(Symbol.MapEnd);
                return 0;
            }
            else
            {
                return 1;
            }
        }

        public override void SkipMap()
        {
            advance(Symbol.MapStart);
            if (reader.TokenType == JsonToken.StartObject)
            {
                reader.Skip();
                reader.Read();
                advance(Symbol.MapEnd);
            }
            else
            {
                throw error("map-start");
            }
        }

        public override int ReadUnionIndex()
        {
            advance(Symbol.Union);
            Symbol.Alternative a = (Symbol.Alternative)parser.PopSymbol();

            string label;
            if (reader.TokenType == JsonToken.Null)
            {
                label = "null";
            }
            else if (reader.TokenType == JsonToken.StartObject)
            {
                reader.Read();
                if (reader.TokenType == JsonToken.PropertyName)
                {
                    label = Convert.ToString(reader.Value);
                    reader.Read();
                    parser.PushSymbol(Symbol.UnionEnd);
                }
                else
                {
                    throw error("start-union");
                }
            }
            else
            {
                throw error("start-union");
            }

            int n = a.FindLabel(label);
            if (n < 0)
            {
                throw new AvroTypeException("Unknown union branch " + label);
            }

            parser.PushSymbol(a.GetSymbol(n));
            return n;
        }

        public override void SkipNull()
        {
            ReadNull();
        }

        public override void SkipBoolean()
        {
            ReadBoolean();
        }

        public override void SkipInt()
        {
            ReadInt();
        }

        public override void SkipLong()
        {
            ReadLong();
        }

        public override void SkipFloat()
        {
            ReadFloat();
        }

        public override void SkipDouble()
        {
            ReadDouble();
        }

        public override void SkipEnum()
        {
            ReadEnum();
        }

        public override void SkipUnionIndex()
        {
            ReadUnionIndex();
        }

        public override Symbol DoAction(Symbol input, Symbol top)
        {
            if (top is Symbol.FieldAdjustAction)
            {
                Symbol.FieldAdjustAction fa = (Symbol.FieldAdjustAction)top;
                string name = fa.FName;
                if (currentReorderBuffer != null)
                {
                    IList<JsonElement> node = currentReorderBuffer.SavedFields[name];
                    if (node != null)
                    {
                        currentReorderBuffer.SavedFields.Remove(name);
                        currentReorderBuffer.OrigParser = reader;
                        reader = makeParser(node);
                        return null;
                    }
                }

                if (reader.TokenType == JsonToken.PropertyName)
                {
                    do
                    {
                        string fn = Convert.ToString(reader.Value);
                        reader.Read();
                        if (name.Equals(fn) || (fa.Aliases != null && fa.Aliases.Contains(fn)))
                        {
                            return null;
                        }
                        else
                        {
                            if (currentReorderBuffer == null)
                            {
                                currentReorderBuffer = new ReorderBuffer();
                            }

                            currentReorderBuffer.SavedFields[fn] = getValueAsTree(reader);
                        }
                    } while (reader.TokenType == JsonToken.PropertyName);

                    throw new AvroTypeException("Expected field name not found: " + fa.FName);
                }
            }
            else if (top == Symbol.FieldEnd)
            {
                if (currentReorderBuffer != null && currentReorderBuffer.OrigParser != null)
                {
                    reader = currentReorderBuffer.OrigParser;
                    currentReorderBuffer.OrigParser = null;
                }
            }
            else if (top == Symbol.RecordStart)
            {
                if (reader.TokenType == JsonToken.StartObject)
                {
                    reader.Read();
                    reorderBuffers.Push(currentReorderBuffer);
                    currentReorderBuffer = null;
                }
                else
                {
                    throw error("record-start");
                }
            }
            else if (top == Symbol.RecordEnd || top == Symbol.UnionEnd)
            {
                // AVRO-2034 advance to the end of our object
                while (reader.TokenType != JsonToken.EndObject)
                {
                    reader.Read();
                }

                if (top == Symbol.RecordEnd)
                {
                    if (currentReorderBuffer != null && currentReorderBuffer.SavedFields.Count > 0)
                    {
                        throw error("Unknown fields: " + currentReorderBuffer.SavedFields.Keys);
                    }

                    currentReorderBuffer = reorderBuffers.Pop();
                }

                // AVRO-2034 advance beyond the end object for the next record.
                reader.Read();
            }
            else
            {
                throw new AvroTypeException("Unknown action symbol " + top);
            }

            return null;
        }


        private class JsonElement
        {
            public readonly JsonToken Token;
            public readonly object Value;

            public JsonElement(JsonToken t, object value)
            {
                this.Token = t;
                this.Value = value;
            }

            public JsonElement(JsonToken t) : this(t, null)
            {
            }
        }

        private static IList<JsonElement> getValueAsTree(JsonReader reader)
        {
            int level = 0;
            IList<JsonElement> result = new List<JsonElement>();
            do
            {
                JsonToken t = reader.TokenType;
                switch (t)
                {
                    case JsonToken.StartObject:
                    case JsonToken.StartArray:
                        level++;
                        result.Add(new JsonElement(t));
                        break;
                    case JsonToken.EndObject:
                    case JsonToken.EndArray:
                        level--;
                        result.Add(new JsonElement(t));
                        break;
                    case JsonToken.PropertyName:
                    case JsonToken.String:
                    case JsonToken.Integer:
                    case JsonToken.Float:
                    case JsonToken.Boolean:
                    case JsonToken.Null:
                        result.Add(new JsonElement(t, reader.Value));
                        break;
                }

                reader.Read();
            } while (level != 0);

            result.Add(new JsonElement(JsonToken.None));
            return result;
        }

        private JsonReader makeParser(in IList<JsonElement> elements)
        {
            return new JsonElementReader(elements);
        }

        private class JsonElementReader : JsonReader
        {
            private readonly IList<JsonElement> elements;

            public JsonElementReader(IList<JsonElement> elements)
            {
                this.elements = elements;
                pos = 0;
            }

            private int pos;

            public override object Value
            {
                get { return elements[pos].Value; }
            }

            public override JsonToken TokenType
            {
                get { return elements[pos].Token; }
            }

            public override bool Read()
            {
                pos++;
                return true;
            }

            public new void Skip()
            {
                JsonToken tkn = elements[pos].Token;
                int level = (tkn == JsonToken.StartArray || tkn == JsonToken.EndArray) ? 1 : 0;
                while (level > 0)
                {
                    switch (elements[++pos].Token)
                    {
                        case JsonToken.StartArray:
                        case JsonToken.StartObject:
                            level++;
                            break;
                        case JsonToken.EndArray:
                        case JsonToken.EndObject:
                            level--;
                            break;
                    }
                }
            }
        }

        private AvroTypeException error(string type)
        {
            return new AvroTypeException("Expected " + type + ". Got " + reader.TokenType);
        }
    }
}
