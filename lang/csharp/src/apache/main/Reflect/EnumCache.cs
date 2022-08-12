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
using System.Collections.Concurrent;
using Avro;

namespace Avro.Reflect
{
    /// <summary>
    /// Cache of enum types. Cache key is the schema fullname.
    /// </summary>
    [Obsolete()]
    public static class EnumCache
    {
        private static ConcurrentDictionary<string, Type> _nameEnumMap = new ConcurrentDictionary<string, Type>();

        /// <summary>
        /// Add and entry to the cache
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="dotnetEnum"></param>
        [Obsolete()]
        public static void AddEnumNameMapItem(NamedSchema schema, Type dotnetEnum)
        {
            AddEnumNameMapItem(schema.Fullname, dotnetEnum);
        }

        /// <summary>
        /// Add and entry to the cache
        /// </summary>
        /// <param name="schemaFullName"></param>
        /// <param name="dotnetEnum"></param>
        [Obsolete()]
        public static void AddEnumNameMapItem(string schemaFullName, Type dotnetEnum)
        {
            _nameEnumMap.TryAdd(schemaFullName, dotnetEnum);
        }

        /// <summary>
        /// Lookup an entry in the cache - based on the schema fullname
        /// </summary>
        /// <param name="schema"></param>
        /// <returns></returns>
        [Obsolete()]
        public static Type GetEnumeration(NamedSchema schema)
        {
            return GetEnumeration(schema.Fullname);
        }

        /// <summary>
        /// Lookup an entry in the cache - based on the schema fullname
        /// </summary>
        /// <param name="schemaFullName"></param>
        /// <returns></returns>
        [Obsolete()]
        public static Type GetEnumeration(string schemaFullName)
        {
            Type t;
            if (!_nameEnumMap.TryGetValue(schemaFullName, out t))
            {
                throw new AvroException($"Couldn't find enumeration for avro fullname: {schemaFullName}");
            }

            return t;
        }
    }
}
