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

using System.Collections.Generic;
using Avro.IO;

namespace Avro.Generic
{
    public partial class GenericDatumReader<T>
    {
        /// <summary>
        /// Generic Map Access
        /// </summary>
        /// <seealso cref="PreresolvingDatumReader&lt;T&gt;" />
        private class GenericMapAccess : MapAccess
        {
            /// <inheritdoc/>
            public object Create(object reuse)
            {
                if (reuse is IDictionary<string, object>)
                {
                    IDictionary<string, object> result = (IDictionary<string, object>)reuse;
                    result.Clear();
                    return result;
                }

                return new Dictionary<string, object>();
            }

            /// <inheritdoc/>
            public void AddElements(object mapObj, int elements, ReadItem itemReader, Decoder decoder, bool reuse)
            {
                var map = (IDictionary<string, object>)mapObj;
                for (int i = 0; i < elements; i++)
                {
                    var key = decoder.ReadString();
                    map[key] = itemReader(null, decoder);
                }
            }
        }
    }
}
