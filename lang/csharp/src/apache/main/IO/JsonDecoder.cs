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
using System.Linq;
using System.Text;
using Avro.IO.Parsing;
using Newtonsoft.Json;

namespace Avro.IO
{
    /// <summary>
    /// A <see cref="Decoder"/> for Avro's JSON data encoding.
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

            public JsonReader OrigParser { get; set; }
        }

        private JsonDecoder(Symbol root, Stream stream) : base(root)
        {
            Configure(stream);
        }

        private JsonDecoder(Symbol root, string str) : base(root)
        {
            Configure(str);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonDecoder"/> class.
        /// </summary>
        public JsonDecoder(Schema schema, Stream stream) : this(GetSymbol(schema), stream)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonDecoder"/> class.
        /// </summary>
        public JsonDecoder(Schema schema, string str) : this(GetSymbol(schema), str)
        {
        }

        private static Symbol GetSymbol(Schema schema)
        {
            return (new JsonGrammarGenerator()).Generate(schema);
        }

        /// <summary>
        /// Reconfigures this JsonDecoder to use the InputStream provided.
        /// Otherwise, this JsonDecoder will reset its state and then reconfigure its
        /// input.
        /// </summary>
        /// <param name="stream"> The InputStream to read from. Cannot be null. </param>
        public void Configure(Stream stream)
        {
            Parser.Reset();
            reorderBuffers.Clear();
            currentReorderBuffer = null;
            reader = new JsonTextReader(new StreamReader(stream));
            reader.Read();
        }

        /// <summary>
        /// Reconfigures this JsonDecoder to use the String provided for input.
        /// Otherwise, this JsonDecoder will reset its state and then reconfigure its
        /// input.
        /// </summary>
        /// <param name="str"> The String to read from. Cannot be null. </param>
        public void Configure(string str)
        {
            Parser.Reset();
            reorderBuffers.Clear();
            currentReorderBuffer = null;
            reader = new JsonTextReader(new StringReader(str));
            reader.Read();
        }

        private void Advance(Symbol symbol)
        {
            Parser.ProcessTrailingImplicitActions();
            Parser.Advance(symbol);
        }

        /// <inheritdoc />
        public override void ReadNull()
        {
            Advance(Symbol.Null);
            if (reader.TokenType == JsonToken.Null)
            {
                reader.Read();
            }
            else
            {
                throw TypeError("null");
            }
        }

        /// <inheritdoc />
        public override bool ReadBoolean()
        {
            Advance(Symbol.Boolean);
            if (reader.TokenType == JsonToken.Boolean)
            {
                bool result = Convert.ToBoolean(reader.Value);
                reader.Read();
                return result;
            }
            else
            {
                throw TypeError("boolean");
            }
        }

        /// <inheritdoc />
        public override int ReadInt()
        {
            Advance(Symbol.Int);
            if (reader.TokenType == JsonToken.Integer || reader.TokenType == JsonToken.Float)
            {
                int result = Convert.ToInt32(reader.Value);
                reader.Read();
                return result;
            }
            else
            {
                throw TypeError("int");
            }
        }

        /// <inheritdoc />
        public override long ReadLong()
        {
            Advance(Symbol.Long);
            if (reader.TokenType == JsonToken.Integer || reader.TokenType == JsonToken.Float)
            {
                long result = Convert.ToInt64(reader.Value);
                reader.Read();
                return result;
            }
            else
            {
                throw TypeError("long");
            }
        }

        /// <inheritdoc />
        public override float ReadFloat()
        {
            Advance(Symbol.Float);
            if (reader.TokenType == JsonToken.Integer || reader.TokenType == JsonToken.Float)
            {
                float result = (float)Convert.ToDouble(reader.Value);
                reader.Read();
                return result;
            }
            else
            {
                throw TypeError("float");
            }
        }

        /// <inheritdoc />
        public override double ReadDouble()
        {
            Advance(Symbol.Double);
            if (reader.TokenType == JsonToken.Integer || reader.TokenType == JsonToken.Float)
            {
                double result = Convert.ToDouble(reader.Value);
                reader.Read();
                return result;
            }
            else
            {
                throw TypeError("double");
            }
        }

        /// <inheritdoc />
        public override string ReadString()
        {
            Advance(Symbol.String);
            if (Parser.TopSymbol() == Symbol.MapKeyMarker)
            {
                Parser.Advance(Symbol.MapKeyMarker);
                if (reader.TokenType != JsonToken.PropertyName)
                {
                    throw TypeError("map-key");
                }
            }
            else
            {
                if (reader.TokenType != JsonToken.String)
                {
                    throw TypeError("string");
                }
            }

            string result = Convert.ToString(reader.Value);
            reader.Read();
            return result;
        }

        /// <inheritdoc />
        public override void SkipString()
        {
            Advance(Symbol.String);
            if (Parser.TopSymbol() == Symbol.MapKeyMarker)
            {
                Parser.Advance(Symbol.MapKeyMarker);
                if (reader.TokenType != JsonToken.PropertyName)
                {
                    throw TypeError("map-key");
                }
            }
            else
            {
                if (reader.TokenType != JsonToken.String)
                {
                    throw TypeError("string");
                }
            }

            reader.Read();
        }

        /// <inheritdoc />
        public override byte[] ReadBytes()
        {
            Advance(Symbol.Bytes);
            if (reader.TokenType == JsonToken.String)
            {
                byte[] result = ReadByteArray();
                reader.Read();
                return result;
            }
            else
            {
                throw TypeError("bytes");
            }
        }

        private byte[] ReadByteArray()
        {
            Encoding iso = Encoding.GetEncoding("ISO-8859-1");
            byte[] result = iso.GetBytes(Convert.ToString(reader.Value));
            return result;
        }

        /// <inheritdoc />
        public override void SkipBytes()
        {
            Advance(Symbol.Bytes);
            if (reader.TokenType == JsonToken.String)
            {
                reader.Read();
            }
            else
            {
                throw TypeError("bytes");
            }
        }

        private void CheckFixed(int size)
        {
            Advance(Symbol.Fixed);
            Symbol.IntCheckAction top = (Symbol.IntCheckAction)Parser.PopSymbol();
            if (size != top.Size)
            {
                throw new AvroTypeException("Incorrect length for fixed binary: expected " + top.Size +
                                            " but received " + size + " bytes.");
            }
        }

        /// <inheritdoc />
        public override void ReadFixed(byte[] bytes)
        {
            ReadFixed(bytes, 0, bytes.Length);
        }

        /// <inheritdoc />
        public override void ReadFixed(byte[] bytes, int start, int len)
        {
            CheckFixed(len);
            if (reader.TokenType == JsonToken.String)
            {
                byte[] result = ReadByteArray();
                reader.Read();
                if (result.Length != len)
                {
                    throw new AvroTypeException("Expected fixed length " + len + ", but got" + result.Length);
                }

                Array.Copy(result, 0, bytes, start, len);
            }
            else
            {
                throw TypeError("fixed");
            }
        }

        /// <inheritdoc />
        public override void SkipFixed(int length)
        {
            CheckFixed(length);
            DoSkipFixed(length);
        }

        private void DoSkipFixed(int length)
        {
            if (reader.TokenType == JsonToken.String)
            {
                byte[] result = ReadByteArray();
                reader.Read();
                if (result.Length != length)
                {
                    throw new AvroTypeException("Expected fixed length " + length + ", but got" + result.Length);
                }
            }
            else
            {
                throw TypeError("fixed");
            }
        }

        /// <inheritdoc />
        protected override void SkipFixed()
        {
            Advance(Symbol.Fixed);
            Symbol.IntCheckAction top = (Symbol.IntCheckAction)Parser.PopSymbol();
            DoSkipFixed(top.Size);
        }

        /// <inheritdoc />
        public override int ReadEnum()
        {
            Advance(Symbol.Enum);
            Symbol.EnumLabelsAction top = (Symbol.EnumLabelsAction)Parser.PopSymbol();
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
                throw TypeError("fixed");
            }
        }

        /// <inheritdoc />
        public override long ReadArrayStart()
        {
            Advance(Symbol.ArrayStart);
            if (reader.TokenType == JsonToken.StartArray)
            {
                reader.Read();
                return DoArrayNext();
            }
            else
            {
                throw TypeError("array-start");
            }
        }

        /// <inheritdoc />
        public override long ReadArrayNext()
        {
            Advance(Symbol.ItemEnd);
            return DoArrayNext();
        }

        private long DoArrayNext()
        {
            if (reader.TokenType == JsonToken.EndArray)
            {
                Parser.Advance(Symbol.ArrayEnd);
                reader.Read();
                return 0;
            }
            else
            {
                return 1;
            }
        }

        /// <inheritdoc />
        public override void SkipArray()
        {
            Advance(Symbol.ArrayStart);
            if (reader.TokenType == JsonToken.StartArray)
            {
                reader.Skip();
                reader.Read();
                Advance(Symbol.ArrayEnd);
            }
            else
            {
                throw TypeError("array-start");
            }
        }

        /// <inheritdoc />
        public override long ReadMapStart()
        {
            Advance(Symbol.MapStart);
            if (reader.TokenType == JsonToken.StartObject)
            {
                reader.Read();
                return DoMapNext();
            }
            else
            {
                throw TypeError("map-start");
            }
        }

        /// <inheritdoc />
        public override long ReadMapNext()
        {
            Advance(Symbol.ItemEnd);
            return DoMapNext();
        }

        private long DoMapNext()
        {
            if (reader.TokenType == JsonToken.EndObject)
            {
                reader.Read();
                Advance(Symbol.MapEnd);
                return 0;
            }
            else
            {
                return 1;
            }
        }

        /// <inheritdoc />
        public override void SkipMap()
        {
            Advance(Symbol.MapStart);
            if (reader.TokenType == JsonToken.StartObject)
            {
                reader.Skip();
                reader.Read();
                Advance(Symbol.MapEnd);
            }
            else
            {
                throw TypeError("map-start");
            }
        }

        /// <inheritdoc />
        public override int ReadUnionIndex()
        {
            Advance(Symbol.Union);
            Symbol.Alternative a = (Symbol.Alternative)Parser.PopSymbol();

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
                    Parser.PushSymbol(Symbol.UnionEnd);
                }
                else
                {
                    throw TypeError("start-union");
                }
            }
            else
            {
                throw TypeError("start-union");
            }

            int n = a.FindLabel(label);
            if (n < 0)
            {
                throw new AvroTypeException("Unknown union branch " + label);
            }

            Parser.PushSymbol(a.GetSymbol(n));
            return n;
        }

        /// <inheritdoc />
        public override void SkipNull()
        {
            ReadNull();
        }

        /// <inheritdoc />
        public override void SkipBoolean()
        {
            ReadBoolean();
        }

        /// <inheritdoc />
        public override void SkipInt()
        {
            ReadInt();
        }

        /// <inheritdoc />
        public override void SkipLong()
        {
            ReadLong();
        }

        /// <inheritdoc />
        public override void SkipFloat()
        {
            ReadFloat();
        }

        /// <inheritdoc />
        public override void SkipDouble()
        {
            ReadDouble();
        }

        /// <inheritdoc />
        public override void SkipEnum()
        {
            ReadEnum();
        }

        /// <inheritdoc />
        public override void SkipUnionIndex()
        {
            ReadUnionIndex();
        }

        /// <inheritdoc />
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
                        reader = MakeParser(node);
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

                            currentReorderBuffer.SavedFields[fn] = GetValueAsTree(reader);
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
                    throw TypeError("record-start");
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
                        throw TypeError("Unknown fields: " + currentReorderBuffer.SavedFields.Keys
                            .Aggregate((x, y) => x + ", " + y ));
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
            private readonly JsonToken token;
            public JsonToken Token => token;
            private readonly object value;
            public object Value => value;

            public JsonElement(JsonToken t, object value)
            {
                token = t;
                this.value = value;
            }

            public JsonElement(JsonToken t) : this(t, null)
            {
            }
        }

        private static IList<JsonElement> GetValueAsTree(JsonReader reader)
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

        private JsonReader MakeParser(in IList<JsonElement> elements)
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
        }

        private AvroTypeException TypeError(string type)
        {
            return new AvroTypeException("Expected " + type + ". Got " + reader.TokenType);
        }
    }
}
