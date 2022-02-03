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
using Encoder = Avro.IO.Encoder;

namespace Avro.Generic
{
    public partial class GenericDatumWriter<T>
    {
        /// <summary>
        /// Generic Array Access
        /// </summary>
        /// <seealso cref="PreresolvingDatumWriter&lt;T&gt;" />
        private class GenericArrayAccess : ArrayAccess
        {
            /// <inheritdoc/>
            public void EnsureArrayObject(object value)
            {
                if (value == null || !(value is Array)) throw TypeMismatch(value, "array", "Array");
            }

            /// <inheritdoc/>
            public long GetArrayLength(object value)
            {
                return ((Array)value).Length;
            }

            /// <inheritdoc/>
            public void WriteArrayValues(object array, WriteItem valueWriter, Encoder encoder)
            {
                var arrayInstance = (Array)array;
                for (int i = 0; i < arrayInstance.Length; i++)
                {
                    encoder.StartItem();
                    valueWriter(arrayInstance.GetValue(i), encoder);
                }
            }
        }
    }
}
