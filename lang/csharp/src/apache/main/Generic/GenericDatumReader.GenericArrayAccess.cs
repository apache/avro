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
using Avro.IO;

namespace Avro.Generic
{
    public partial class GenericDatumReader<T>
    {
        /// <summary>
        /// Generic Array Access
        /// </summary>
        /// <seealso cref="PreresolvingDatumReader&lt;T&gt;" />
        private class GenericArrayAccess : ArrayAccess
        {
            /// <inheritdoc/>
            public object Create(object reuse)
            {
                return (reuse is object[]) ? reuse : new object[0];
            }

            /// <inheritdoc/>
            public void EnsureSize(ref object array, int targetSize)
            {
                if (((object[])array).Length < targetSize)
                {
                    SizeTo(ref array, targetSize);
                }
            }

            /// <inheritdoc/>
            public void Resize(ref object array, int targetSize)
            {
                SizeTo(ref array, targetSize);
            }

            /// <inheritdoc/>
            public void AddElements(object arrayObj, int elements, int index, ReadItem itemReader, Decoder decoder, bool reuse)
            {
                var array = (object[])arrayObj;
                for (int i = index; i < index + elements; i++)
                {
                    array[i] = reuse ? itemReader(array[i], decoder) : itemReader(null, decoder);
                }
            }

            /// <inheritdoc/>
            private static void SizeTo(ref object array, int targetSize)
            {
                var o = (object[])array;
                Array.Resize(ref o, targetSize);
                array = o;
            }
        }
    }
}
