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
using Avro.Reflect.Array;

namespace Avro.Reflect.Service
{
    /// <summary>
    /// Additional functionality to serialize and deserialize arrays.
    /// Works with array helpert
    /// </summary>
    public class ArrayService : IArrayService
    {
        private readonly IReflectCache _reflectCache;

        /// <summary>
        /// Public constructor
        /// </summary>
        /// <param name="reflectCache"></param>
        public ArrayService(IReflectCache reflectCache)
        {
            _reflectCache = reflectCache;
        }

        /// <summary>
        /// Find an array helper for an array schema node.
        /// </summary>
        /// <param name="schema">Schema</param>
        /// <param name="enumerable">The array object. If it is null then Add(), Count() and Clear methods will throw exceptions.</param>
        /// <returns></returns>
        public IArrayHelper GetArrayHelper(ArraySchema schema, IEnumerable enumerable)
        {
            string s = GetHelperName(schema);

            if (s != null && _reflectCache.TryGetArrayHelperType(s, out Type arrayHelperType))
            {
                return (IArrayHelper)Activator.CreateInstance(arrayHelperType, enumerable);
            }

            return (IArrayHelper)Activator.CreateInstance(typeof(ArrayHelper), enumerable);
        }

        internal string GetHelperName(ArraySchema ars)
        {
            // ArraySchema is unnamed schema and doesn't have a FulllName, use "helper" metadata.
            // Metadata is json string, strip quotes

            string s = null;
            s = ars.GetProperty("helper");
            return (s != null && s.Length > 2) ? s.Substring(1, s.Length - 2) : null;
        }
    }
}
