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
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Avro.Reflect.Reflection;

namespace Avro.Reflect.Interface
{
    /// <summary>
    /// Interface to work with caches.
    /// - cache of C# classes and their properties. The key for the cache is the schema full name.
    /// - cache of enum types. Cache key is the schema fullname.
    /// </summary>
    public interface IReflectCache
    {
        /// <summary>
        /// Add an entry to the class cache.
        /// </summary>
        /// <param name="objType">Type of the C# class</param>
        /// <param name="s">Schema</param>
        void LoadClassCache(Type objType, Schema s);

        /// <summary>
        /// Find a class that matches the schema full name.
        /// </summary>
        /// <param name="schema"></param>
        /// <returns></returns>
        DotnetClass GetClass(RecordSchema schema);

        /// <summary>
        /// Find an array helper for an array schema node.
        /// </summary>
        /// <param name="schema">Schema</param>
        /// <param name="enumerable">The array object. If it is null then Add(), Count() and Clear methods will throw exceptions.</param>
        /// <returns></returns>
        ArrayHelper GetArrayHelper(ArraySchema schema, IEnumerable enumerable);

        /// <summary>
        /// Lookup an entry in the cache - based on the schema fullname
        /// </summary>
        /// <param name="schema"></param>
        /// <returns></returns>
        Type GetEnumeration(NamedSchema schema);

        /// <summary>
        /// Find a registered converter
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="propType"></param>Ss
        /// <returns>The first matching converter - null if there isn't one</returns>
        IAvroFieldConverter GetDefaultConverter(Avro.Schema.Type tag, Type propType);
    }
}
