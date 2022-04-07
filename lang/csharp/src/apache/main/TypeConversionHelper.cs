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

namespace Avro
{
    internal static class TypeConversionHelper
    {
        /// <summary>
        /// Returns true iof the type is a <see cref="Nullable"/> of a primitive type
        /// </summary>
        public static bool IsNullable(this Type type)
        {
            return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
        }

        /// <summary>
        /// Returns the primitive or Nullable&lt;T&gt; type as a <see cref="Nullable"/> or null if it cannot be expressed as a Nullable&lt;T&gt;
        /// </summary>
        public static Type MakeNullable(this Type type)
        {
            return type.IsPrimitive
                ? typeof(Nullable<>).MakeGenericType(type)
                : IsNullable(type)
                    ? type
                    : null;
        }

        /// <summary>
        /// Returns the Nullable or primitive type as a primitive type or null if it cannot be expressed as primitive.
        /// </summary>
        public static Type MakePrimitive(this Type type)
        {
            return type.IsPrimitive
                ? type
                : IsNullable(type)
                    ? type.GetGenericArguments()[0]
                    : null;
        }
    }
}
