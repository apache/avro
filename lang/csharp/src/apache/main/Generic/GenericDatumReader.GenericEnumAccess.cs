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
        /// Generic Enum Access
        /// </summary>
        /// <seealso cref="PreresolvingDatumReader&lt;T&gt;" />
        internal class GenericEnumAccess : EnumAccess
        {
            /// <summary>
            /// The enum schema
            /// </summary>
            private readonly EnumSchema _schema;

            /// <summary>
            /// Initializes a new instance of the <see cref="GenericEnumAccess"/> class.
            /// </summary>
            /// <param name="schema">The enum schema.</param>
            public GenericEnumAccess(EnumSchema schema)
            {
                _schema = schema;
            }

            /// <inheritdoc/>
            public object CreateEnum(object reuse, int ordinal)
            {
                if (reuse is GenericEnum)
                {
                    var ge = (GenericEnum)reuse;
                    if (ge.Schema.Equals(_schema))
                    {
                        ge.Value = _schema[ordinal];
                        return ge;
                    }
                }

                return new GenericEnum(_schema, _schema[ordinal]);
            }
        }
    }
}
