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
using System.Collections.Generic;
using System.Reflection;

namespace Avro.Specific
{
    /// <summary>
    /// Resolves and creates types associated with a schema and/or name. You should generally use
    /// the shared <see cref="Instance"/> to take advantage caching.
    /// </summary>
    public sealed class ObjectCreator
    {
        /// <summary>
        /// Shareable instance of the <see cref="ObjectCreator"/>.
        /// </summary>
        public static ObjectCreator Instance { get; } = new ObjectCreator();

        /// <summary>
        /// Static generic dictionary type used for creating new dictionary instances
        /// </summary>
        private readonly Type GenericMapType = typeof(Dictionary<,>);

        /// <summary>
        /// Static generic list type used for creating new array instances
        /// </summary>
        private readonly Type GenericListType = typeof(List<>);

        /// <summary>
        /// Static generic list type used for creating new IList instances
        /// </summary>
        private readonly Type GenericIListType = typeof(IList<>);

        /// <summary>
        /// Static generic nullable type used for creating new nullable instances
        /// </summary>
        private readonly Type GenericNullableType = typeof(Nullable<>);

        private readonly ConcurrentDictionary<string, Type> typeCacheByName;
        private readonly Assembly execAssembly;
        private readonly Assembly entryAssembly;
        private readonly bool diffAssembly;

        /// <summary>
        /// Initializes a new instance of the <see cref="ObjectCreator"/> class.
        /// </summary>
        public ObjectCreator()
        {
            typeCacheByName = new ConcurrentDictionary<string, Type>();
            execAssembly = Assembly.GetExecutingAssembly();
            entryAssembly = Assembly.GetEntryAssembly();

            // entryAssembly returns null when running from NUnit
            diffAssembly = entryAssembly != null && execAssembly != entryAssembly;
        }

        /// <summary>
        /// Find the type with the given name
        /// </summary>
        /// <param name="name">the object type to locate</param>
        /// <returns>the object type, or <c>null</c> if not found</returns>
        /// <exception cref="AvroException">
        /// No type found matching the given name.
        /// </exception>
        private Type FindType(string name)
        {
            return typeCacheByName.GetOrAdd(name, (_) =>
            {
                Type type = null;

                if (TryGetIListItemTypeName(name, out var itemTypeName))
                {
                    return GenericIListType.MakeGenericType(FindType(itemTypeName));
                }

                if (TryGetNullableItemTypeName(name, out itemTypeName))
                {
                    return GenericNullableType.MakeGenericType(FindType(itemTypeName));
                }

                // if entry assembly different from current assembly, try entry assembly first
                if (diffAssembly)
                {
                    type = entryAssembly.GetType(name);
                }

                // try current assembly and mscorlib
                if (type == null)
                {
                    type = Type.GetType(name);
                }

                // type is still not found, need to look through all loaded assemblies
                if (type == null)
                {
                    type = FindTypeInLoadedAssemblies(name);
                }

                return type
                    ?? throw new AvroException($"Unable to find type '{name}' in all loaded " +
                    $"assemblies");
            });
        }

        private bool TryGetIListItemTypeName(string name, out string itemTypeName)
        {
            const string listPrefix = "IList<";
            const string fullListPrefix = "System.Collections.Generic.IList<";

            if (!name.EndsWith(">", StringComparison.Ordinal))
            {
                itemTypeName = null;
                return false;
            }

            if (name.StartsWith(fullListPrefix, StringComparison.Ordinal))
            {
                itemTypeName = name.Substring(
                    fullListPrefix.Length, name.Length - fullListPrefix.Length - 1);
                return true;
            }

            if (name.StartsWith(listPrefix, StringComparison.Ordinal))
            {
                itemTypeName = name.Substring(
                    listPrefix.Length, name.Length - listPrefix.Length - 1);
                return true;
            }

            itemTypeName = null;
            return false;
        }

        private bool TryGetNullableItemTypeName(string name, out string itemTypeName)
        {
            const string nullablePrefix = "Nullable<";
            const string fullNullablePrefix = "System.Nullable<";

            if (name.StartsWith(fullNullablePrefix, StringComparison.Ordinal))
            {
                itemTypeName = name.Substring(
                    fullNullablePrefix.Length, name.Length - fullNullablePrefix.Length - 1);
                return true;
            }

            if (name.StartsWith(nullablePrefix, StringComparison.Ordinal))
            {
                itemTypeName = name.Substring(
                    nullablePrefix.Length, name.Length - nullablePrefix.Length - 1);
                return true;
            }

            itemTypeName = null;
            return false;
        }

        /// <summary>
        /// Gets the type for the specified schema
        /// </summary>
        /// <param name="schema"></param>
        /// <returns></returns>
        /// <exception cref="AvroException">
        /// No type found matching the given name.
        /// </exception>
        public Type GetType(Schema schema)
        {
            switch(schema.Tag) {
            case Schema.Type.Null:
                break;
            case Schema.Type.Boolean:
                return typeof(bool);
            case Schema.Type.Int:
                return typeof(int);
            case Schema.Type.Long:
                return typeof(long);
            case Schema.Type.Float:
                return typeof(float);
            case Schema.Type.Double:
                return typeof(double);
            case Schema.Type.Bytes:
                return typeof(byte[]);
            case Schema.Type.String:
                return typeof(string);
            case Schema.Type.Union:
                {
                    if (schema is UnionSchema unSchema && unSchema.Count == 2)
                    {
                        Schema s1 = unSchema.Schemas[0];
                        Schema s2 = unSchema.Schemas[1];

                        // Nullable ?
                        Type itemType = null;
                        if (s1.Tag == Schema.Type.Null)
                        {
                            itemType = GetType(s2);
                        }
                        else if (s2.Tag == Schema.Type.Null)
                        {
                            itemType = GetType(s1);
                        }

                        if (itemType != null)
                        {
                            if (itemType.IsValueType && !itemType.IsEnum)
                            {
                                try
                                {
                                    return GenericNullableType.MakeGenericType(itemType);
                                }
                                catch
                                {
                                }
                            }

                            return itemType;
                        }
                    }

                    return typeof(object);
                }
            case Schema.Type.Array:
                {
                    ArraySchema arrSchema = schema as ArraySchema;
                    Type itemSchema = GetType(arrSchema.ItemSchema);

                    return GenericListType.MakeGenericType(itemSchema);
                }
            case Schema.Type.Map:
                {
                    MapSchema mapSchema = schema as MapSchema;
                    Type itemSchema = GetType(mapSchema.ValueSchema);

                    return GenericMapType.MakeGenericType(typeof(string), itemSchema );
                }
            case Schema.Type.Enumeration:
            case Schema.Type.Record:
            case Schema.Type.Fixed:
            case Schema.Type.Error:
                {
                    // Should all be named types
                    if (schema is NamedSchema named)
                    {
                        return FindType(named.Fullname);
                    }

                    break;
                }
            }

            // Fallback
            return FindType(schema.Name);
        }

        /// <summary>
        /// Finds the type with the given name in all loaded assemblies. If several assemblies contain
        /// a matching type, the one found last in the loaded assemblies wins.
        /// </summary>
        /// <param name="name">the object type to locate</param>
        /// <returns>the object type, or <c>null</c> if not found</returns>
        private static Type FindTypeInLoadedAssemblies(string name)
        {
            Assembly[] assemblies = AppDomain.CurrentDomain.GetAssemblies();
            string unmangledName = CodeGenUtil.Instance.UnMangle(name);

            // A name with a namespace can only match the full name of a type. Looking it up directly
            // in each assembly finds the same type as enumerating all types of all assemblies, without
            // loading all of them.
            if (name.IndexOf('.') >= 0)
            {
                Type type = null;
                foreach (Assembly assembly in assemblies)
                {
                    try
                    {
                        type = assembly.GetType(name, false) ?? assembly.GetType(unmangledName, false) ?? type;
                    }
                    catch
                    {
                        // Looking up a type could fail for a variety of non-fatal reasons, continue.
                    }
                }

                if (type != null)
                {
                    return type;
                }
            }

            return FindTypeByEnumeratingLoadedAssemblies(assemblies, name, unmangledName);
        }

        /// <summary>
        /// Finds the type with the given name or full name by enumerating all types of all loaded assemblies.
        /// </summary>
        /// <param name="assemblies">the loaded assemblies</param>
        /// <param name="name">the object type to locate</param>
        /// <param name="unmangledName">the object type to locate, without escaped reserved words</param>
        /// <returns>the object type, or <c>null</c> if not found</returns>
        private static Type FindTypeByEnumeratingLoadedAssemblies(Assembly[] assemblies, string name, string unmangledName)
        {
            Type type = null;
            foreach (Assembly assembly in assemblies)
            {
                // Loading all types from all assemblies could fail for a variety of
                // non -fatal reasons. If we fail to load types from an assembly, continue.
                try
                {
                    // Change the search to look for Types by both NAME and FULLNAME
                    foreach (Type t in assembly.GetTypes())
                    {
                        if (name == t.Name || name == t.FullName || unmangledName == t.FullName)
                        {
                            type = t;
                            break;
                        }
                    }
                }
                catch
                {
                    continue;
                }
            }

            return type;
        }

        /// <summary>
        /// Gets the type of the specified type name
        /// </summary>
        /// <param name="name">name of the object to get type of</param>
        /// <param name="schemaType">schema type for the object</param>
        /// <returns>Type</returns>
        /// <exception cref="AvroException">
        /// No type found matching the given name.
        /// </exception>
        public Type GetType(string name, Schema.Type schemaType)
        {
            Type type = FindType(name);

            if (schemaType == Schema.Type.Map)
            {
                type = GenericMapType.MakeGenericType(typeof(string), type);
            }
            else if (schemaType == Schema.Type.Array)
            {
                type = GenericListType.MakeGenericType(type);
            }

            return type;
        }

        /// <summary>
        /// Creates new instance of the given type
        /// </summary>
        /// <param name="name">fully qualified name of the type</param>
        /// <param name="schemaType">type of schema</param>
        /// <returns>new object of the given type</returns>
        /// <exception cref="AvroException">
        /// No type found matching the given name.
        /// </exception>
        public object New(string name, Schema.Type schemaType)
        {
            return Activator.CreateInstance(GetType(name, schemaType));
        }
    }
}
