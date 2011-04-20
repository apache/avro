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

        private readonly Assembly execAssembly;
        private readonly Assembly entryAssembly;
        private readonly bool diffAssembly;
        private readonly Type[] margs;
        private readonly Type[] largs;

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
            margs = new Type[2] { typeof(string), null };
            largs = new Type[1] { null };

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
        /// Gets the type of the specified type name
        /// </summary>
        /// <param name="name">name of the object to get type of</param>
        /// <param name="schemaType">schema type for the object</param>
        /// <returns>Type</returns>
        public Type GetType(string name, Schema.Type schemaType)
        {
            Type type;
            if (diffAssembly)
            {
                // entry assembly different from current assembly, try entry assembly first
                type = entryAssembly.GetType(name);
                if (type == null)   // now try current assembly and mscorlib
                    type = Type.GetType(name);
            }
            else
                type = Type.GetType(name);

            if (type == null) // type is still not found, need to loop through all loaded assemblies
            {
                Assembly[] assemblies = AppDomain.CurrentDomain.GetAssemblies();
                foreach (Assembly assembly in assemblies)
                {
                    type = assembly.GetType(name);
                    if (type != null)
                        break;
                }
            }
            if (type == null)
                throw new AvroException("Unable to find type " + name + " in all loaded assemblies");

            if (schemaType == Schema.Type.Map)
            {
                margs[1] = type;
                type = GenericMapType.MakeGenericType(margs);
            }
            else if (schemaType == Schema.Type.Array)
            {
                largs[0] = type;
                type = GenericListType.MakeGenericType(largs);
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
            if (!ctors.TryGetValue(key, out ctor))
            {
                Type type = GetType(name, schemaType);
                ctor = GetConstructor(name, schemaType, type);

                ctors.Add(key, ctor);
            }
            return ctor();
        }
    }
}
