/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Reflection;
using Avro;

namespace Avro.POCO
{
    public static class ClassCache
    {
        private static ConcurrentDictionary<string, IDotnetClass> _nameClassMap = new ConcurrentDictionary<string, IDotnetClass>();

        public static void AddClassNameMapItem(RecordSchema schema, Type dotnetClass)
        {
            _nameClassMap.TryAdd(schema.Fullname, ProcessAttributes(schema, dotnetClass));
        }

        public static IDotnetClass GetClass(RecordSchema schema)
        {
            IDotnetClass c;
            if (!_nameClassMap.TryGetValue(schema.Fullname, out c))
            {
               return null;
            }

            return c;
        }

        public static IDotnetClass ProcessAttributes(RecordSchema schema, Type t)
        {
            IDotnetClass c;

            if (schema != null && _nameClassMap.TryGetValue(schema.Fullname, out c))
            {
                return c;
            }

            if (!t.IsClass)
            {
                throw new AvroException( $"Type {t.Name} is not a class");
            }

            bool byPosition = false;
            foreach (var att in t.GetCustomAttributes())
            {
                AvroAttribute avroAttr = att as AvroAttribute;
                if (avroAttr != null)
                {
                    byPosition = avroAttr.ByPosition;
                }
            }

            if (byPosition)
            {
                return new ByPosClass(t, schema);
            }
            else
            {
                return new ByNameClass(t, schema);
            }
        }
        public static void LoadClassCache(Type objType, Schema s)
        {
            // if (s==null)
            // {
            //     return;
            // }
            switch(s)
            {
                case RecordSchema rs:
                    if (!objType.IsClass)
                    {
                        throw new AvroException($"Cant map scalar type {objType.Name} to record {rs.Fullname}");
                    }
                    if (typeof(byte[]).IsAssignableFrom(objType)
                        || typeof(string).IsAssignableFrom(objType)
                        || typeof(IList).IsAssignableFrom(objType)
                        || typeof(IDictionary).IsAssignableFrom(objType)
                    )
                    {
                        throw new AvroException($"Cant map type {objType.Name} to record {rs.Fullname}");
                    }

                    ClassCache.AddClassNameMapItem(rs, objType);
                    var c = ClassCache.GetClass(rs);
                    foreach (var f in rs.Fields)
                    {
                        var t = c.GetFieldType(f);
                        LoadClassCache(t, f.Schema);
                    }
                    break;
                case ArraySchema ars:
                    if (!typeof(IList).IsAssignableFrom(objType))
                    {
                        throw new AvroException($"Cant map type {objType.Name} to array {ars.Fullname}");
                    }
                    if (!objType.IsGenericType)
                    {
                        throw new AvroException($"Cant map non-generic type {objType.Name} to array {ars.Fullname}");
                    }

                    LoadClassCache(objType.GenericTypeArguments[0], ars.ItemSchema);
                    break;
                case MapSchema ms:
                    if (!typeof(IDictionary).IsAssignableFrom(objType))
                    {
                        throw new AvroException($"Cant map type {objType.Name} to map {ms.Fullname}");
                    }
                    if (!objType.IsGenericType)
                    {
                        throw new AvroException($"Cant map non-generic type {objType.Name} to map {ms.Fullname}");
                    }
                    if (!typeof(string).IsAssignableFrom(objType.GenericTypeArguments[0]))
                    {
                        throw new AvroException($"First type parameter of {objType.Name} mustbe assignable to string");
                    }

                    LoadClassCache(objType.GenericTypeArguments[1], ms.ValueSchema);
                    break;
                case NamedSchema ns:
                    EnumCache.AddEnumNameMapItem(ns, objType);
                    break;
            }
        }
    }
}
