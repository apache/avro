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
using System.CodeDom;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Avro.IO.Parsing;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

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
        private readonly JsonMode mode;
        private ReorderBuffer currentReorderBuffer;

        private class ReorderBuffer
        {
            public readonly IDictionary<string, IList<JsonElement>> SavedFields =
                new Dictionary<string, IList<JsonElement>>();

            public JsonReader OrigParser { get; set; }
        }

        private JsonDecoder(Symbol root, Stream stream, JsonMode mode) : base(root)
        {
            this.mode = mode;
            Configure(stream);
        }

        private JsonDecoder(Symbol root, string str, JsonMode mode) : base(root)
        {
            this.mode = mode;
            Configure(str);
        }

        private JsonDecoder(Symbol root, JsonReader reader, JsonMode mode) : base(root)
        {
            this.mode = mode;
            Parser.Reset();
            reorderBuffers.Clear();
            currentReorderBuffer = null;
            this.reader = reader;
            this.reader.DateTimeZoneHandling = DateTimeZoneHandling.RoundtripKind;
            this.reader.DateParseHandling = DateParseHandling.DateTime;
            this.reader.Read();            
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonDecoder"/> class.
        /// </summary>
        public JsonDecoder(Schema schema, Stream stream, JsonMode mode = JsonMode.AvroJson) : this(GetSymbol(schema, mode), stream, mode)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonDecoder"/> class.
        /// </summary>
        public JsonDecoder(Schema schema, string str, JsonMode mode = JsonMode.AvroJson) : this(GetSymbol(schema, mode), str, mode)
        {
        }

        private static Symbol GetSymbol(Schema schema, JsonMode mode)
        {
            return (new JsonGrammarGenerator(mode)).Generate(schema);
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
            reader.DateTimeZoneHandling = DateTimeZoneHandling.RoundtripKind;
            reader.DateParseHandling = DateParseHandling.DateTime;
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
            reader.DateTimeZoneHandling = DateTimeZoneHandling.RoundtripKind;
            reader.DateParseHandling = DateParseHandling.DateTime;
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

        private decimal ReadDecimal(Symbol symbol)
        {
            Advance(symbol);
            if (reader.TokenType == JsonToken.Integer || reader.TokenType == JsonToken.Float)
            {
                decimal result = Convert.ToDecimal(reader.Value);
                reader.Read();
                return result;
            }
            else
            {
                throw TypeError("decimal");
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

        private DateTime ReadDateTime()
        {
            Advance(Symbol.JsonDateTime);
            if (reader.TokenType == JsonToken.Date)
            {
                DateTime result = (DateTime)reader.Value;
                reader.Read();
                return result;
            }
            else if (reader.TokenType == JsonToken.String)
            {
                string dateString = reader.Value.ToString();
                if (DateTime.TryParseExact(dateString, "yyyy-MM-dd", null, System.Globalization.DateTimeStyles.RoundtripKind, out DateTime result))
                {
                    reader.Read();
                    return result;
                }
                else if (DateTime.TryParseExact(dateString, "O", null, System.Globalization.DateTimeStyles.RoundtripKind, out DateTime result1))
                {
                    reader.Read();
                    return result1;
                }
                else
                {
                    throw new AvroTypeException("Error parsing date string");
                }
            }
            else
            {
                throw TypeError("date");
            }
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
            if (mode == JsonMode.PlainJson)
            {
                try
                {
                    return Convert.FromBase64String(Convert.ToString(reader.Value));
                }
                catch (FormatException e)
                {
                    throw new AvroTypeException("Error decoding base64 string: " + e.Message);
                }
            }
            else
            {
                Encoding iso = Encoding.GetEncoding("ISO-8859-1");
                byte[] result = iso.GetBytes(Convert.ToString(reader.Value));
                return result;
            }
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

            string label = null;
            if (reader.TokenType == JsonToken.Null)
            {
                label = "null";
            }
            else if (mode == JsonMode.PlainJson)
            {
                var symbolCount = a.Symbols.Length;

                if (reader.TokenType == JsonToken.Boolean)
                {
                    label = "boolean";
                }
                else if (reader.TokenType == JsonToken.Integer)
                {
                    for (int i = 0; i < symbolCount; i++)
                    {
                        var symbol = a.Symbols[i];
                        if (symbol == Symbol.Int || symbol == Symbol.Long)
                        {
                            label = a.Labels[i];
                            break;
                        }
                    }
                }
                else if (reader.TokenType == JsonToken.Float)
                {
                    for (int i = 0; i < symbolCount; i++)
                    {
                        var symbol = a.Symbols[i];
                        if (symbol == Symbol.Float || symbol == Symbol.Double || symbol == Symbol.Fixed)
                        {
                            label = a.Labels[i];
                            break;
                        }
                    }
                }
                else if (reader.TokenType == JsonToken.Bytes)
                {
                    for (int i = 0; i < symbolCount; i++)
                    {
                        var symbol = a.Symbols[i];
                        if (symbol == Symbol.Bytes || symbol == Symbol.Fixed)
                        {
                            label = a.Labels[i];
                            break;
                        }
                    }
                }
                else if (reader.TokenType == JsonToken.Date || reader.TokenType == JsonToken.String)
                {
                    if (mode == JsonMode.PlainJson)
                    {
                        Symbol pushSymbol = Symbol.String;
                        string dateString = reader.Value.ToString();

                        if (reader.TokenType == JsonToken.Date ||
                            DateTime.TryParseExact(dateString, "yyyy-MM-dd", null, System.Globalization.DateTimeStyles.RoundtripKind, out DateTime _) ||
                            DateTime.TryParseExact(dateString, "O", null, System.Globalization.DateTimeStyles.RoundtripKind, out DateTime _ ))
                        {
                            label = "date";
                            pushSymbol = Symbol.JsonDateTime;
                        }
                        else
                        {
                            label = "string";
                        }
                        if (label == "date")
                        {
                            for (int i = 0; i < symbolCount; i++)
                            {
                                var symbol = a.Symbols[i];
                                if (symbol == Symbol.JsonDateTime)
                                {
                                    label = a.Labels[i];
                                    break;
                                }
                            }
                        }
                        int n1 = a.FindLabel(label);
                        if (n1 < 0)
                        {
                            throw new AvroTypeException("Unknown union branch " + label);
                        }
                        Parser.PushSymbol(pushSymbol);
                        return n1;
                    }
                    else
                    {
                        label = "string";
                    }
                }
            }

            if (reader.TokenType == JsonToken.StartObject)
            {
                if ( mode == JsonMode.AvroJson)
                {
                    // in Avro JSON, the object is tagged with the type
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
                    // Plain JSON; read the object and infer the type
                    var objectToken = JToken.ReadFrom(reader);
                    int matchIndex = -1;
                    for (int i = 0; i < a.Symbols.Length; i++)
                    {
                        if (a.Symbols[i].SymKind == Symbol.Kind.Sequence)
                        {
                            JTokenReader objectReader = new JTokenReader(objectToken);
                            if (IsRecordMatch(a.Symbols[i], objectReader))
                            {
                                if (matchIndex >= 0)
                                {
                                    throw new AvroTypeException("Ambiguous union: matches both " + a.Labels[matchIndex] + " and " + a.Labels[i]);
                                }
                                matchIndex = i;
                            }
                        }
                    }
                    reader = new CompositeJsonReader(new JTokenReader(objectToken), reader);
                    reader.Read();
                    if (matchIndex >= 0)
                    {
                        label = a.Labels[matchIndex];
                        Parser.PushSymbol(Symbol.UnionEnd);
                    }
                    else
                    {
                        throw new AvroTypeException("Unknown union branch");
                    }
                }
            }
            else if ( label == null)
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

        private bool IsRecordMatch(Symbol symbol, JsonReader objectReader) 
        {
            JsonDecoder innerDecoder = new JsonDecoder(symbol, objectReader, JsonMode.PlainJson);
            try
            {
                
                while( objectReader.TokenType != JsonToken.None )
                {
                    switch(objectReader.TokenType)
                    {
                        case JsonToken.PropertyName:
                            break;
                        case JsonToken.Integer:
                            innerDecoder.Advance(Symbol.Int);
                            break;
                        case JsonToken.Float:
                            innerDecoder.Advance(Symbol.Float);
                            break;
                        case JsonToken.Boolean:
                            innerDecoder.Advance(Symbol.Boolean);
                            break;
                        case JsonToken.Date:
                            innerDecoder.Advance(Symbol.JsonDateTime);
                            break;
                        case JsonToken.String:
                            innerDecoder.Advance(Symbol.String);
                            break;
                        case JsonToken.Null:
                            innerDecoder.Advance(Symbol.Null);
                            break;
                        case JsonToken.Bytes:
                            innerDecoder.Advance(Symbol.Bytes);
                            break;
                        case JsonToken.StartObject:
                            innerDecoder.Advance(Symbol.RecordStart);
                            break;
                        case JsonToken.EndObject:
                            break;
                        case JsonToken.StartArray:
                            innerDecoder.Advance(Symbol.ArrayStart);
                            break;
                        case JsonToken.EndArray:
                            innerDecoder.Advance(Symbol.ArrayEnd);
                            break;
                        default:
                            break;
                    }
                    objectReader.Read();
                }
            }
            catch (AvroTypeException)
            {
                return false;
            }
            return true;
        }

        /// <inheritdoc />
        public override object ReadLogicalTypeValue(LogicalSchema logicalSchema)
        {
            switch (logicalSchema.LogicalType)
            {
                case Util.LogicalUnixEpochType<DateTime> dt:
                    if (mode == JsonMode.PlainJson)
                    {
                        return dt.ConvertToLogicalValue(ReadDateTime(), logicalSchema);
                    }
                    else
                    {
                        if (logicalSchema.BaseSchema.Tag == Schema.Type.Long)
                        {
                            return dt.ConvertToLogicalValue(ReadLong(), logicalSchema);
                        }
                        else
                        {
                            return dt.ConvertToLogicalValue(ReadInt(), logicalSchema);
                        }
                    }
                case Util.LogicalUnixEpochType<TimeSpan> ts:
                    if (mode == JsonMode.PlainJson)
                    {
                        return ts.ConvertToLogicalValue(ReadString(), logicalSchema);
                    }
                    else
                    {
                        if (logicalSchema.BaseSchema.Tag == Schema.Type.Long)
                        {
                            return ts.ConvertToLogicalValue(ReadLong(), logicalSchema);
                        }
                        else
                        {
                            return ts.ConvertToLogicalValue(ReadInt(), logicalSchema);
                        }
                    }
                case Util.Decimal dec:
                    if (mode == JsonMode.PlainJson)
                    {
                        return dec.ConvertToLogicalValue(ReadDecimal(logicalSchema.BaseSchema.Tag == Schema.Type.Bytes?Symbol.Bytes:Symbol.Fixed), logicalSchema);
                    }
                    else
                    {
                        if (logicalSchema.BaseSchema.Tag == Schema.Type.Fixed)
                        {
                            byte[] fixedValue = new byte[((FixedSchema)logicalSchema.BaseSchema).Size];
                            ReadFixed(fixedValue);
                            return dec.ConvertToLogicalValue(fixedValue, logicalSchema);
                        }
                        else
                        {
                            return dec.ConvertToLogicalValue(ReadBytes(), logicalSchema);
                        }
                    }
                default:
                    break;
            }

            Schema baseSchema = logicalSchema.BaseSchema;
            switch (baseSchema.Tag)
            {
                case Schema.Type.Int:
                    return logicalSchema.LogicalType.ConvertToLogicalValue(ReadInt(), logicalSchema);
                case Schema.Type.Long:
                    return logicalSchema.LogicalType.ConvertToLogicalValue(ReadLong(), logicalSchema);
                case Schema.Type.Bytes:
                    return logicalSchema.LogicalType.ConvertToLogicalValue(ReadBytes(), logicalSchema);
                case Schema.Type.String:
                    return logicalSchema.LogicalType.ConvertToLogicalValue(ReadString(), logicalSchema);
                case Schema.Type.Fixed:
                    byte[] fixedValue = new byte[((FixedSchema)baseSchema).Size];
                    ReadFixed(fixedValue);
                    return logicalSchema.LogicalType.ConvertToLogicalValue(fixedValue, logicalSchema);
                default:
                    throw new AvroException($"Unsupported logical type: {logicalSchema.Tag}");
            }
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
                            .Aggregate((x, y) => x + ", " + y));
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

    class CompositeJsonReader : JsonReader
    {
        private readonly JsonReader[] readers;
        private int currentReader;

        public CompositeJsonReader(JsonReader reader1, JsonReader reader2)
        {
            this.readers = new[] { reader1, reader2 };
            currentReader = 0;
        }

        public override object Value
        {
            get { return readers[currentReader].Value; }
        }

        public override JsonToken TokenType
        {
            get { return readers[currentReader].TokenType; }
        }

        public override bool Read()
        {
            if (readers[currentReader].Read())
            {
                return true;
            }

            currentReader++;
            if (currentReader < readers.Length)
            {
                return true;
            }

            return false;
        }
    }
}
