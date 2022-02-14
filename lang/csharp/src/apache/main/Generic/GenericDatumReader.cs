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
using Decoder = Avro.IO.Decoder;

namespace Avro.Generic
{
    /// <summary>
    /// <see cref="PreresolvingDatumReader{T}" /> for reading data to <see cref="GenericRecord" />
    /// classes or primitives.
    /// <see cref="PreresolvingDatumReader{T}">For more information about performance considerations
    /// for choosing this implementation</see>.
    /// </summary>
    /// <typeparam name="T">Type to deserialize data into.</typeparam>
    /// <seealso cref="PreresolvingDatumReader&lt;T&gt;" />
    public class GenericDatumReader<T> : PreresolvingDatumReader<T>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="GenericDatumReader{T}" /> class.
        /// </summary>
        /// <param name="writerSchema">Schema that was used to write the data.</param>
        /// <param name="readerSchema">Schema to use to read the data.</param>
        public GenericDatumReader(Schema writerSchema, Schema readerSchema) : base(writerSchema, readerSchema)
        {
        }

        /// <inheritdoc/>
        protected override ArrayAccess GetArrayAccess(ArraySchema readerSchema)
        {
            return new GenericArrayAccess();
        }

        /// <inheritdoc/>
        protected override EnumAccess GetEnumAccess(EnumSchema readerSchema)
        {
            return new GenericEnumAccess(readerSchema);
        }

        /// <inheritdoc/>
        protected override FixedAccess GetFixedAccess(FixedSchema readerSchema)
        {
            return new GenericFixedAccess(readerSchema);
        }

        /// <inheritdoc/>
        protected override MapAccess GetMapAccess(MapSchema readerSchema)
        {
            return new GenericMapAccess();
        }

        /// <inheritdoc/>
        protected override RecordAccess GetRecordAccess(RecordSchema readerSchema)
        {
            return new GenericRecordAccess(readerSchema);
        }

        /// <inheritdoc/>
        protected override bool IsReusable(Schema.Type tag)
        {
            switch (tag)
            {
                case Schema.Type.Double:
                case Schema.Type.Boolean:
                case Schema.Type.Int:
                case Schema.Type.Long:
                case Schema.Type.Float:
                case Schema.Type.Bytes:
                case Schema.Type.String:
                case Schema.Type.Null:
                    return false;
            }

            return true;
        }

        /// <summary>
        /// Generic Record Access
        /// </summary>
        /// <seealso cref="PreresolvingDatumReader&lt;T&gt;" />
        internal class GenericRecordAccess : RecordAccess
        {
            private readonly RecordSchema _schema;

            /// <summary>
            /// Initializes a new instance of the <see cref="GenericRecordAccess"/> class.
            /// </summary>
            /// <param name="schema">The schema.</param>
            public GenericRecordAccess(RecordSchema schema)
            {
                _schema = schema;
            }

            /// <inheritdoc/>
            public void AddField(object record, string fieldName, int fieldPos, object fieldValue)
            {
                ((GenericRecord)record).Add(fieldName, fieldValue);
            }

            /// <inheritdoc/>
            public object CreateRecord(object reuse)
            {
                GenericRecord ru = (reuse == null || !(reuse is GenericRecord) || !(reuse as GenericRecord).Schema.Equals(_schema)) ?
                    new GenericRecord(_schema) :
                    reuse as GenericRecord;
                return ru;
            }

            /// <inheritdoc/>
            public object GetField(object record, string fieldName, int fieldPos)
            {
                return !((GenericRecord)record).TryGetValue(fieldPos, out object result) ?
                    null : result;
            }
        }

        /// <summary>
        /// Generic Array Access
        /// </summary>
        /// <seealso cref="PreresolvingDatumReader&lt;T&gt;" />
        private class GenericArrayAccess : ArrayAccess
        {
            /// <inheritdoc/>
            public void AddElements(object arrayObj, int elements, int index, ReadItem itemReader, Decoder decoder, bool reuse)
            {
                object[] array = (object[])arrayObj;
                for (int i = index; i < index + elements; i++)
                {
                    array[i] = reuse ? itemReader(array[i], decoder) : itemReader(null, decoder);
                }
            }

            /// <inheritdoc/>
            public object Create(object reuse)
            {
                return (reuse is object[]) ? reuse : Array.Empty<object>();
            }

            /// <inheritdoc/>
            public void EnsureSize(ref object array, int targetSize)
            {
                if (((object[])array).Length < targetSize)
                    SizeTo(ref array, targetSize);
            }

            /// <inheritdoc/>
            public void Resize(ref object array, int targetSize)
            {
                SizeTo(ref array, targetSize);
            }

            /// <summary>
            /// Sizes to.
            /// </summary>
            /// <param name="array">The array.</param>
            /// <param name="targetSize">Size of the target.</param>
            private static void SizeTo(ref object array, int targetSize)
            {
                object[] o = (object[])array;
                Array.Resize(ref o, targetSize);
                array = o;
            }
        }

        /// <summary>
        /// Generic Enum Access
        /// </summary>
        /// <seealso cref="PreresolvingDatumReader&lt;T&gt;" />
        private class GenericEnumAccess : EnumAccess
        {
            private readonly EnumSchema _schema;

            /// <summary>
            /// Initializes a new instance of the <see cref="GenericEnumAccess"/> class.
            /// </summary>
            /// <param name="schema">The schema.</param>
            public GenericEnumAccess(EnumSchema schema)
            {
                _schema = schema;
            }

            /// <inheritdoc/>
            public object CreateEnum(object reuse, int ordinal)
            {
                if (reuse is GenericEnum ge)
                {
                    if (ge.Schema.Equals(_schema))
                    {
                        ge.Value = _schema[ordinal];
                        return ge;
                    }
                }

                return new GenericEnum(_schema, _schema[ordinal]);
            }
        }

        /// <summary>
        /// Generic Fixed Access
        /// </summary>
        /// <seealso cref="PreresolvingDatumReader&lt;T&gt;" />
        private class GenericFixedAccess : FixedAccess
        {
            private readonly FixedSchema _schema;

            /// <summary>
            /// Initializes a new instance of the <see cref="GenericFixedAccess"/> class.
            /// </summary>
            /// <param name="schema">The schema.</param>
            public GenericFixedAccess(FixedSchema schema)
            {
                _schema = schema;
            }

            /// <inheritdoc/>
            public object CreateFixed(object reuse)
            {
                return (reuse is GenericFixed && (reuse as GenericFixed).Schema.Equals(_schema)) ?
                    reuse : new GenericFixed(_schema);
            }

            /// <inheritdoc/>
            public byte[] GetFixedBuffer(object f)
            {
                return ((GenericFixed)f).Value;
            }
        }

        /// <summary>
        /// Generic Map Access
        /// </summary>
        /// <seealso cref="PreresolvingDatumReader&lt;T&gt;" />
        private class GenericMapAccess : MapAccess
        {
            /// <inheritdoc/>
            // TODO: reuse is not used, create overload and deprecate this, or implement with reuse
            public void AddElements(object mapObj, int elements, ReadItem itemReader, Decoder decoder, bool reuse)
            {
                IDictionary<string, object> map = (IDictionary<string, object>)mapObj;
                for (int i = 0; i < elements; i++)
                {
                    string key = decoder.ReadString();
                    map[key] = itemReader(null, decoder);
                }
            }

            /// <inheritdoc/>
            public object Create(object reuse)
            {
                if (reuse is IDictionary<string, object> result)
                {
                    result.Clear();
                    return result;
                }
                return new Dictionary<string, object>();
            }
        }
    }
}
