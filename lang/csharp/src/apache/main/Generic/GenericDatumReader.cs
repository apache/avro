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
    public partial class GenericDatumReader<T> : PreresolvingDatumReader<T>
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
        protected override FixedAccess GetFixedAccess(FixedSchema readerSchema)
        {
            return new GenericFixedAccess(readerSchema);
        }
    }
}
