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
        /// Generic Fixed Access
        /// </summary>
        /// <seealso cref="PreresolvingDatumReader&lt;T&gt;" />
        internal class GenericFixedAccess : FixedAccess
        {
            /// <summary>
            /// The fixed schema
            /// </summary>
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
    }
}
