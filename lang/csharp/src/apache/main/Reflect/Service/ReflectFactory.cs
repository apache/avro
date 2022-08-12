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
using Avro.Reflect.Model;

namespace Avro.Reflect.Service
{
    /// <summary>
    /// Factory to create objects for reflest serialixation and deserialization
    /// </summary>
    public class ReflectFactory : IReflectFactory
    {
        private readonly IReflectCache _refletCache;
        private readonly IArrayService _arrayService;
        private readonly IDotnetclassFactory _dotnetclassFactory;

        /// <summary>
        /// Public constructor
        /// </summary>
        public ReflectFactory(
            IReflectCache refletCache,
            IArrayService arrayService,
            IDotnetclassFactory dotnetclassFactory)
        {
            _refletCache = refletCache;
            _arrayService = arrayService;
            _dotnetclassFactory = dotnetclassFactory;
        }

        /// <summary>
        /// Create reflect reader
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="writerSchema"></param>
        /// <param name="readerSchema"></param>
        /// <returns></returns>
        public ReflectReader<T> CreateReader<T>(Schema writerSchema, Schema readerSchema)
        {
            LoadClassCache(typeof(T), readerSchema);

            return new ReflectReader<T>(writerSchema, readerSchema, _refletCache, _arrayService);
        }

        /// <summary>
        /// Create reflect writer
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="writerSchema"></param>
        /// <returns></returns>
        public ReflectWriter<T> CreateWriter<T>(Schema writerSchema)
        {
            LoadClassCache(typeof(T), writerSchema);

            return new ReflectWriter<T>(writerSchema, _refletCache, _arrayService);
        }

        internal void LoadClassCache(Type objType, Schema s)
        {
            Dictionary<string, Schema> previousFields = new Dictionary<string, Schema>();

            switch (s)
            {
                case RecordSchema rs:
                    if (!objType.IsClass)
                    {
                        throw new AvroException($"Cant map scalar type {objType.Name} to record {rs.Fullname}");
                    }

                    if (typeof(byte[]).IsAssignableFrom(objType)
                        || typeof(string).IsAssignableFrom(objType)
                        || typeof(IEnumerable).IsAssignableFrom(objType)
                        || typeof(IDictionary).IsAssignableFrom(objType))
                    {
                        throw new AvroException($"Cant map type {objType.Name} to record {rs.Fullname}");
                    }

                    AddClassNameMapItem(rs, objType);
                    var c = _refletCache.GetClass(rs.Fullname);
                    foreach (var f in rs.Fields)
                    {
                        /*              
                        //.StackOverflowException
                        var t = c.GetPropertyType(f);
                        LoadClassCache(t, f.Schema);
                        */
                        if (!previousFields.ContainsKey(f.Name))
                        {
                            previousFields.Add(f.Name, f.Schema);
                            var t = c.GetPropertyType(f);
                            LoadClassCache(t, f.Schema);
                        }
                    }

                    break;
                case ArraySchema ars:
                    if (!typeof(IEnumerable).IsAssignableFrom(objType))
                    {
                        throw new AvroException($"Cant map type {objType.Name} to array {ars.Name}");
                    }

                    if (!objType.IsGenericType)
                    {
                        throw new AvroException($"{objType.Name} needs to be a generic type");
                    }

                    LoadClassCache(objType.GenericTypeArguments[0], ars.ItemSchema);
                    break;
                case MapSchema ms:
                    if (!typeof(IDictionary).IsAssignableFrom(objType))
                    {
                        throw new AvroException($"Cant map type {objType.Name} to map {ms.Name}");
                    }

                    if (!objType.IsGenericType)
                    {
                        throw new AvroException($"Cant map non-generic type {objType.Name} to map {ms.Name}");
                    }

                    if (!typeof(string).IsAssignableFrom(objType.GenericTypeArguments[0]))
                    {
                        throw new AvroException($"First type parameter of {objType.Name} must be assignable to string");
                    }

                    LoadClassCache(objType.GenericTypeArguments[1], ms.ValueSchema);
                    break;
                case NamedSchema ns:
                    _refletCache.AddEnum(ns.Fullname, objType);
                    break;
                case UnionSchema us:
                    if (us.Schemas.Count == 2 && (us.Schemas[0].Tag == Schema.Type.Null || us.Schemas[1].Tag == Schema.Type.Null))
                    {
                        // in this case objType will match the non null type in the union
                        foreach (var o in us.Schemas)
                        {
                            if (o.Tag == Schema.Type.Null)
                            {
                                continue;
                            }

                            if (objType.IsClass)
                            {
                                LoadClassCache(objType, o);
                            }

                            var innerType = Nullable.GetUnderlyingType(objType);
                            if (innerType != null && innerType.IsEnum)
                            {
                                LoadClassCache(innerType, o);
                            }
                        }
                    }
                    else
                    {
                        // check the schema types are registered
                        foreach (var o in us.Schemas)
                        {
                            if (o.Tag == Schema.Type.Record && _refletCache.GetClass(o.Fullname) == null)
                            {
                                throw new AvroException($"Class for union record type {o.Fullname} is not registered. Create a ClassCache object and call LoadClassCache");
                            }
                        }
                    }

                    break;
            }
        }

        private void AddClassNameMapItem(RecordSchema schema, Type dotnetClass)
        {
            if (schema != null && _refletCache.GetClass(schema.Fullname) != null)
            {
                return;
            }

            if (!dotnetClass.IsClass)
            {
                throw new AvroException($"Type {dotnetClass.Name} is not a class");
            }

            _refletCache.AddClass(schema.Fullname, _dotnetclassFactory.CreateDotnetClass(schema, dotnetClass));
        }
    }
}
