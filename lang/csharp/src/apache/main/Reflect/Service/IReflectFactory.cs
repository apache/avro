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
using System.Text;
using Avro.Reflect.Model;

namespace Avro.Reflect.Service
{
    /// <summary>
    /// Factory to create objects for reflest serialixation and deserialization
    /// </summary>
    public interface IReflectFactory
    {
        /// <summary>
        /// Create reflect reader
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="writerSchema"></param>
        /// <param name="readerSchema"></param>
        /// <returns></returns>
        ReflectReader<T> CreateReader<T>(Schema writerSchema, Schema readerSchema);

        /// <summary>
        /// Create reflect writer
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="writerSchema"></param>
        /// <returns></returns>
        ReflectWriter<T> CreateWriter<T>(Schema writerSchema);
    }
}
