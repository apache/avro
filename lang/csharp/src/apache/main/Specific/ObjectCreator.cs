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
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using System.Reflection.Emit;

namespace Avro.Specific
{

    public sealed class ObjectCreator
    {
        private static readonly ObjectCreator instance = new ObjectCreator();
        public static ObjectCreator Instance { get { return instance; } }

        /// <summary>
        /// Static generic dictionary type used for creating new dictionary instances 
        /// </summary>
        private Type GenericMapType = typeof(Dictionary<,>);

        /// <summary>
        /// Static generic list type used for creating new array instances
        /// </summary>
        private Type GenericListType = typeof(List<>);

        /// <summary>
        /// Static generic nullable type used for creating new nullable instances
        /// </summary>
        private Type GenericNullableType = typeof(Nullable<>);
        
        private readonly Assembly execAssembly;
        private readonly Assembly entryAssembly;
        private readonly bool diffAssembly;

        public delegate object CtorDelegate();
        private Type ctorType = typeof(CtorDelegate);
        Dictionary<NameCtorKey, CtorDelegate> ctors;

        private ObjectCreator()
        {
            execAssembly = System.Reflection.Assembly.GetExecutingAssembly();
            entryAssembly = System.Reflection.Assembly.GetEntryAssembly();
            if (entryAssembly != null && execAssembly != entryAssembly) // entryAssembly returns null when running from NUnit
                diffAssembly = true;

            GenericMapType = typeof(Dictionary<,>);
            GenericListType = typeof(List<>);

            ctors = new Dictionary<NameCtorKey, CtorDelegate>();
        }

        public struct NameCtorKey : IEquatable<NameCtorKey>
        {
            public string name { get; private set; }
            public Schema.Type type { get; private set; }
            public NameCtorKey(string value1, Schema.Type value2)
                : this()
            {
                name = value1;
                type = value2;
            }
            public bool Equals(NameCtorKey other)
            {
                return Equals(other.name, name) && other.type == type;
            }
            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj))
                    return false;
                if (obj.GetType() != typeof(NameCtorKey))
                    return false;
                return Equals((NameCtorKey)obj);
            }
            public override int GetHashCode()
            {
                unchecked
                {
                    return ((name != null ? name.GetHashCode() : 0) * 397) ^ type.GetHashCode();
                }
            }
            public static bool operator ==(NameCtorKey left, NameCtorKey right)
            {
                return left.Equals(right);
            }
            public static bool operator !=(NameCtorKey left, NameCtorKey right)
            {
                return !left.Equals(right);
            }
        }

        /// <summary>
        /// Find the type with the given name
        /// </summary>
        /// <param name="name">the object type to locate</param>
        /// <param name="throwError">whether or not to throw an error if the type wasn't found</param>
        /// <returns>the object type, or <c>null</c> if not found</returns>
        private Type FindType(string name,bool throwError) 
        {
            Type type;

            // Modify provided type to ensure it can be discovered.
            // This is mainly for Generics, and Nullables.
            name = name.Replace("Nullable", "Nullable`1");
            name = name.Replace("IList", "System.Collections.Generic.IList`1");
            name = name.Replace("<", "[");
            name = name.Replace(">", "]");

            if (diffAssembly)
            {
                // entry assembly different from current assembly, try entry assembly first
                type = entryAssembly.GetType(name);
                if (type == null)   // now try current assembly and mscorlib
                    type = Type.GetType(name);
            }
            else
                type = Type.GetType(name);

            Type[] types;

            if (type == null) // type is still not found, need to loop through all loaded assemblies
            {
                Assembly[] assemblies = AppDomain.CurrentDomain.GetAssemblies();
                foreach (Assembly assembly in assemblies)
                {
                    // Fix for Mono 3.0.10
                    if (assembly.FullName.StartsWith("MonoDevelop.NUnit"))
                        continue;

                    types = assembly.GetTypes();

                    // Change the search to look for Types by both NAME and FULLNAME
                    foreach (Type t in types)
                    {
                        if (name == t.Name || name == t.FullName) type = t;
                    }
                    
                    if (type != null)
                        break;
                }
            }

            if (null == type && throwError)
            {
                throw new AvroException("Unable to find type " + name + " in all loaded assemblies");
            }

            return type;
        }


        /// <summary>
        /// Gets the type for the specified schema
        /// </summary>
        /// <param name="schema"></param>
        /// <returns></returns>
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
                    UnionSchema unSchema = schema as UnionSchema;
                    if (null != unSchema && unSchema.Count==2)
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

                        if (null != itemType ) 
                        {
                            if (itemType.IsValueType && !itemType.IsEnum)
                            {
                                try
                                {
                                    return GenericNullableType.MakeGenericType(new [] {itemType});
                                }
                                catch (Exception) { }
                            }
                            
                            return itemType;
                        }
                    }

                    return typeof(object);
                }
            case Schema.Type.Array: {
                ArraySchema arrSchema = schema as ArraySchema;
                Type itemSchema = GetType(arrSchema.ItemSchema);

                return GenericListType.MakeGenericType(new [] {itemSchema}); }
            case Schema.Type.Map: {
                MapSchema mapSchema = schema as MapSchema;
                Type itemSchema = GetType(mapSchema.ValueSchema);

                return GenericMapType.MakeGenericType(new [] { typeof(string), itemSchema }); }
            case Schema.Type.Enumeration:
            case Schema.Type.Record:
            case Schema.Type.Fixed:
            case Schema.Type.Error: {
                // Should all be named types
                var named = schema as NamedSchema;
                if(null!=named) {
                    return FindType(named.Fullname,true);
                }
                break; }
            }

            // Fallback
            return FindType(schema.Name,true);
        }

        /// <summary>
        /// Gets the type of the specified type name
        /// </summary>
        /// <param name="name">name of the object to get type of</param>
        /// <param name="schemaType">schema type for the object</param>
        /// <returns>Type</returns>
        public Type GetType(string name, Schema.Type schemaType)
        {
            Type type = FindType(name, true);

            if (schemaType == Schema.Type.Map)
            {
                type = GenericMapType.MakeGenericType(new[] { typeof(string), type });
            }
            else if (schemaType == Schema.Type.Array)
            {
                type = GenericListType.MakeGenericType(new [] {type});
            }

            return type;
        }

        /// <summary>
        /// Gets the default constructor for the specified type
        /// </summary>
        /// <param name="name">name of object for the type</param>
        /// <param name="schemaType">schema type for the object</param>
        /// <param name="type">type of the object</param>
        /// <returns>Default constructor for the type</returns>
        public CtorDelegate GetConstructor(string name, Schema.Type schemaType, Type type)
        {
            ConstructorInfo ctorInfo = type.GetConstructor(Type.EmptyTypes);
            if (ctorInfo == null)
                throw new AvroException("Class " + name + " has no default constructor");

            DynamicMethod dynMethod = new DynamicMethod("DM$OBJ_FACTORY_" + name, typeof(object), null, type, true);
            ILGenerator ilGen = dynMethod.GetILGenerator();
            ilGen.Emit(OpCodes.Nop);
            ilGen.Emit(OpCodes.Newobj, ctorInfo);
            ilGen.Emit(OpCodes.Ret);

            return (CtorDelegate)dynMethod.CreateDelegate(ctorType);
        }

        /// <summary>
        /// Creates new instance of the given type
        /// </summary>
        /// <param name="name">fully qualified name of the type</param>
        /// <param name="schemaType">type of schema</param>
        /// <returns>new object of the given type</returns>
        public object New(string name, Schema.Type schemaType)
        {
            NameCtorKey key = new NameCtorKey(name, schemaType);
            
            CtorDelegate ctor;
            lock(ctors)
            {
                if (!ctors.TryGetValue(key, out ctor))
                {
                    Type type = GetType(name, schemaType);
                    ctor = GetConstructor(name, schemaType, type);

                    ctors.Add(key, ctor);
                }
            }
            return ctor();
        }
    }
}
