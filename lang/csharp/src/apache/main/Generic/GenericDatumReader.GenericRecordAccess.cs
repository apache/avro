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
    public partial class GenericDatumReader<T>
    {
        /// <summary>
        /// Generic Record Access
        /// </summary>
        /// <seealso cref="PreresolvingDatumReader&lt;T&gt;" />
        internal class GenericRecordAccess : RecordAccess
        {
            /// <summary>
            /// The record schema.
            /// </summary>
            private readonly RecordSchema _schema;

            /// <summary>
            /// Initializes a new instance of the <see cref="GenericRecordAccess"/> class.
            /// </summary>
            /// <param name="schema">The record schema.</param>
            public GenericRecordAccess(RecordSchema schema)
            {
                _schema = schema;
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
                return ((GenericRecord)record).TryGetValue(fieldPos, out object result) ? result : null;
            }

            /// <inheritdoc/>
            public void AddField(object record, string fieldName, int fieldPos, object fieldValue)
            {
                ((GenericRecord)record).Add(fieldName, fieldValue);
            }
        }
    }
}
