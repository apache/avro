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
using System.Collections;
using System.IO;
using System.Text;
using Newtonsoft.Json;

namespace Avro.IO
{
    /// <summary>
    /// An <seealso cref="Encoder"/> for Avro's JSON data encoding.
    ///
    /// JsonEncoder buffers output, and data may not appear on the output until
    /// <seealso cref="Encoder.Flush()"/> is called.
    ///
    /// JsonEncoder is not thread-safe.
    /// </summary>
    public class JsonEncoder : ParsingEncoder, Parser.ActionHandler
    {
        private readonly Parser parser;
        private JsonWriter writer;
        private bool includeNamespace = true;

        // Has anything been written into the collections?
        private readonly BitArray isEmpty = new BitArray(100);

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonEncoder"/> class.
        /// </summary>
        public JsonEncoder(Schema sc, Stream stream) : this(sc, getJsonWriter(stream, false))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonEncoder"/> class.
        /// </summary>
        public JsonEncoder(Schema sc, Stream stream, bool pretty) : this(sc, getJsonWriter(stream, pretty))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonEncoder"/> class.
        /// </summary>
        public JsonEncoder(Schema sc, JsonWriter writer)
        {
            Configure(writer);
            this.parser = new Parser((new JsonGrammarGenerator()).Generate(sc), this);
        }

        /// <inheritdoc />
        public override void Flush()
        {
            parser.ProcessImplicitActions();
            if (writer != null)
            {
                writer.Flush();
            }
        }

        // by default, one object per line.
        // with pretty option use default pretty printer with root line separator.
        private static JsonWriter getJsonWriter(Stream stream, bool pretty)
        {
            JsonWriter writer = new JsonTextWriter(new StreamWriter(stream));
            if (pretty)
            {
                writer.Formatting = Formatting.Indented;
            }

            return writer;
        }

        /// <summary>
        /// Whether to include the namespace.
        /// </summary>
        public virtual bool IncludeNamespace
        {
            get { return includeNamespace; }
            set { this.includeNamespace = value; }
        }


        /// <summary>
        /// Reconfigures this JsonEncoder to use the output stream provided.
        /// <p/>
        /// Otherwise, this JsonEncoder will flush its current output and then
        /// reconfigure its output to use a default UTF8 JsonWriter that writes to the
        /// provided OutputStream.
        /// </summary>
        /// <param name="stream"> The OutputStream to direct output to. Cannot be null. </param>
        /// <returns> this JsonEncoder </returns>
        public JsonEncoder Configure(Stream stream)
        {
            this.Configure(getJsonWriter(stream, false));
            return this;
        }

        /// <summary>
        /// Reconfigures this JsonEncoder to output to the JsonWriter provided.
        /// <p/>
        /// Otherwise, this JsonEncoder will flush its current output and then
        /// reconfigure its output to use the provided JsonWriter.
        /// </summary>
        /// <param name="jsonWriter"> The JsonWriter to direct output to. Cannot be null. </param>
        /// <returns> this JsonEncoder </returns>
        public JsonEncoder Configure(JsonWriter jsonWriter)
        {
            if (null != parser)
            {
                Flush();
            }

            this.writer = jsonWriter;
            return this;
        }

        /// <inheritdoc />
        public override void WriteNull()
        {
            parser.Advance(Symbol.Null);
            writer.WriteNull();
        }

        /// <inheritdoc />
        public override void WriteBoolean(bool b)
        {
            parser.Advance(Symbol.Boolean);
            writer.WriteValue(b);
        }

        /// <inheritdoc />
        public override void WriteInt(int n)
        {
            parser.Advance(Symbol.Int);
            writer.WriteValue(n);
        }

        /// <inheritdoc />
        public override void WriteLong(long n)
        {
            parser.Advance(Symbol.Long);
            writer.WriteValue(n);
        }

        /// <inheritdoc />
        public override void WriteFloat(float f)
        {
            parser.Advance(Symbol.Float);
            writer.WriteValue(f);
        }

        /// <inheritdoc />
        public override void WriteDouble(double d)
        {
            parser.Advance(Symbol.Double);
            writer.WriteValue(d);
        }

        /// <inheritdoc />
        public override void WriteString(string str)
        {
            parser.Advance(Symbol.String);
            if (parser.TopSymbol() == Symbol.MapKeyMarker)
            {
                parser.Advance(Symbol.MapKeyMarker);
                writer.WritePropertyName(str);
            }
            else
            {
                writer.WriteValue(str);
            }
        }

        /// <inheritdoc />
        public override void WriteBytes(byte[] bytes)
        {
            WriteBytes(bytes, 0, bytes.Length);
        }

        /// <inheritdoc />
        public override void WriteBytes(byte[] bytes, int start, int len)
        {
            parser.Advance(Symbol.Bytes);
            writeByteArray(bytes, start, len);
        }

        private void writeByteArray(byte[] bytes, int start, int len)
        {
            Encoding iso = Encoding.GetEncoding("ISO-8859-1");
            writer.WriteValue(iso.GetString(bytes, start, len));
        }

        /// <inheritdoc />
        public override void WriteFixed(byte[] bytes)
        {
            WriteFixed(bytes, 0, bytes.Length);
        }

        /// <inheritdoc />
        public override void WriteFixed(byte[] bytes, int start, int len)
        {
            parser.Advance(Symbol.Fixed);
            Symbol.IntCheckAction top = (Symbol.IntCheckAction)parser.PopSymbol();
            if (len != top.Size)
            {
                throw new AvroTypeException("Incorrect length for fixed binary: expected " + top.Size +
                                            " but received " + len + " bytes.");
            }

            writeByteArray(bytes, start, len);
        }

        /// <inheritdoc />
        public override void WriteEnum(int e)
        {
            parser.Advance(Symbol.Enum);
            Symbol.EnumLabelsAction top = (Symbol.EnumLabelsAction)parser.PopSymbol();
            if (e < 0 || e >= top.Size)
            {
                throw new AvroTypeException("Enumeration out of range: max is " + top.Size + " but received " + e);
            }

            writer.WriteValue(top.GetLabel(e));
        }

        /// <inheritdoc />
        public override void WriteArrayStart()
        {
            parser.Advance(Symbol.ArrayStart);
            writer.WriteStartArray();
            Push();
            if (Depth() >= isEmpty.Length)
            {
                isEmpty.Length += isEmpty.Length;
            }

            isEmpty.Set(Depth(), true);
        }

        /// <inheritdoc />
        public override void WriteArrayEnd()
        {
            if (!isEmpty.Get(Pos))
            {
                parser.Advance(Symbol.ItemEnd);
            }

            Pop();
            parser.Advance(Symbol.ArrayEnd);
            writer.WriteEndArray();
        }

        /// <inheritdoc />
        public override void WriteMapStart()
        {
            Push();
            if (Depth() >= isEmpty.Length)
            {
                isEmpty.Length += isEmpty.Length;
            }

            isEmpty.Set(Depth(), true);

            parser.Advance(Symbol.MapStart);
            writer.WriteStartObject();
        }

        /// <inheritdoc />
        public override void WriteMapEnd()
        {
            if (!isEmpty.Get(Pos))
            {
                parser.Advance(Symbol.ItemEnd);
            }

            Pop();

            parser.Advance(Symbol.MapEnd);
            writer.WriteEndObject();
        }

        /// <summary>
        /// Start an array item.
        /// </summary>
        public new void StartItem()
        {
            if (!isEmpty.Get(Pos))
            {
                parser.Advance(Symbol.ItemEnd);
            }

            base.StartItem();
            if (Depth() >= isEmpty.Length)
            {
                isEmpty.Length += isEmpty.Length;
            }

            isEmpty.Set(Depth(), false);
        }

        /// <inheritdoc />
        public override void WriteUnionIndex(int unionIndex)
        {
            parser.Advance(Symbol.Union);
            Symbol.Alternative top = (Symbol.Alternative)parser.PopSymbol();
            Symbol symbol = top.GetSymbol(unionIndex);
            if (symbol != Symbol.Null && includeNamespace)
            {
                writer.WriteStartObject();
                writer.WritePropertyName(top.GetLabel(unionIndex));
                parser.PushSymbol(Symbol.UnionEnd);
            }

            parser.PushSymbol(symbol);
        }

        /// <summary>
        /// Perform an action based on the given input.
        /// </summary>
        public virtual Symbol DoAction(Symbol input, Symbol top)
        {
            if (top is Symbol.FieldAdjustAction)
            {
                Symbol.FieldAdjustAction fa = (Symbol.FieldAdjustAction)top;
                writer.WritePropertyName(fa.FName);
            }
            else if (top == Symbol.RecordStart)
            {
                writer.WriteStartObject();
            }
            else if (top == Symbol.RecordEnd || top == Symbol.UnionEnd)
            {
                writer.WriteEndObject();
            }
            else if (top != Symbol.FieldEnd)
            {
                throw new AvroTypeException("Unknown action symbol " + top);
            }

            return null;
        }
    }
}
