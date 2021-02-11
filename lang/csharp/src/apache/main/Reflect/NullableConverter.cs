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

namespace Avro.Reflect
{
    /// <summary>
    /// A helper to create a NullableConverter over a non-nullable one
    /// </summary>
    /// <typeparam name="T">Any <see cref="IAvroFieldConverter"/></typeparam>
    public class NullableConverter<T> : IAvroFieldConverter
        where T : class, IAvroFieldConverter, new()
    {
        private readonly IAvroFieldConverter _converter;

        /// <summary>
        /// Creates a new instance of <see cref="NullableConverter{T}"/> for the <see cref="T"/>
        /// </summary>
        public NullableConverter()
        {
            var converter = new T();
            this._converter = new NullableConverter(
                converter,
                converter.GetAvroType().MakeNullable(),
                converter.GetPropertyType().MakeNullable());
        }

        /// <summary>
        /// Converts the possibly-null property instance to a null or the converted type for Avro
        /// </summary>
        /// <param name="o">The property value</param>
        /// <param name="s">The schema</param>
        /// <returns>Null or the converted instance</returns>
        public object ToAvroType(object o, Schema s)
        {
            return this._converter.ToAvroType(o, s);
        }

        /// <summary>
        /// Converts the possibly-null Avro field to null or an instance of the property
        /// </summary>
        /// <param name="o">The Avro value</param>
        /// <param name="s">The schema</param>
        /// <returns></returns>
        public object FromAvroType(object o, Schema s)
        {
            return this._converter.FromAvroType(o, s);
        }

        /// <summary>
        /// Returns the Avro type as a nullable type
        /// </summary>
        public Type GetAvroType()
        {
            return this._converter.GetAvroType();
        }

        /// <summary>
        /// Returns the Property type as a nullable type
        /// </summary>
        public Type GetPropertyType()
        {
            return this._converter.GetPropertyType();
        }
    }

    /// <summary>
    /// A class that wraps a converter to support nullable conversions
    /// </summary>
    internal class NullableConverter : IAvroFieldConverter
    {
        private readonly IAvroFieldConverter _converter;
        private readonly Type _avroType;
        private readonly Type _propType;

        public NullableConverter(IAvroFieldConverter converter, Type avroType, Type propType)
        {
            this._converter = converter;
            this._avroType = avroType;
            this._propType = propType;
        }

        public object ToAvroType(object o, Schema s)
        {
            if (o == null)
            {
                return null;
            }

            return this._converter.ToAvroType(o, s);
        }

        public object FromAvroType(object o, Schema s)
        {
            if (o == null)
            {
                return null;
            }

            return this._converter.FromAvroType(o, s);
        }

        public Type GetAvroType()
        {
            return this._avroType;
        }

        public Type GetPropertyType()
        {
            return this._propType;
        }
    }
}
