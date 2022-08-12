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
using System.Linq;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using Avro.Reflect.Converter;

namespace Avro.Reflect.Service
{
    /// <summary>
    /// Service to work with default IAvroFieldConverter
    /// </summary>
    public class ConverterService : IConverterService
    {
        private readonly IEnumerable<IAvroFieldConverter> _convertors;

        /// <summary>
        /// Public constructor
        /// </summary>
        public ConverterService(IEnumerable<IAvroFieldConverter> convertors)
        {
            _convertors = convertors;
        }

        /// <summary>
        /// Find a converter
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="property"></param>
        /// <returns>The first matching converter - null if there isn't one</returns>
        public IAvroFieldConverter GetConverter(Schema schema, PropertyInfo property)
        {
            return GetFieldConverter(schema, property) ?? GetRegisteredConverter(schema, property);
        }

        internal IAvroFieldConverter GetFieldConverter(Schema schema, PropertyInfo property)
        {
            var avroFieldConverters = property
                .GetCustomAttributes(typeof(AvroFieldAttribute), true)
                .Select(attr => ((AvroFieldAttribute)attr).Converter)
                .Where(converter => converter != null)
                .ToArray();

            if (avroFieldConverters.Length > 1)
                throw new AvroException($"More them one converter is defined by AvroFieldAttribute for '{property.Name}'.");

            return avroFieldConverters.Length == 1 ? avroFieldConverters[0] : null;
        }

        internal IAvroFieldConverter GetRegisteredConverter(Schema schema, PropertyInfo property)
        {
            Type avroType;
            switch (schema.Tag)
            {
                case Avro.Schema.Type.Null:
                    return null;
                case Avro.Schema.Type.Boolean:
                    avroType = typeof(bool);
                    break;
                case Avro.Schema.Type.Int:
                    avroType = typeof(int);
                    break;
                case Avro.Schema.Type.Long:
                    avroType = typeof(long);
                    break;
                case Avro.Schema.Type.Float:
                    avroType = typeof(float);
                    break;
                case Avro.Schema.Type.Double:
                    avroType = typeof(double);
                    break;
                case Avro.Schema.Type.Bytes:
                    avroType = typeof(byte[]);
                    break;
                case Avro.Schema.Type.String:
                    avroType = typeof(string);
                    break;
                case Avro.Schema.Type.Record:
                    return null;
                case Avro.Schema.Type.Enumeration:
                    return null;
                case Avro.Schema.Type.Array:
                    return null;
                case Avro.Schema.Type.Map:
                    return null;
                case Avro.Schema.Type.Union:
                    return null;
                case Avro.Schema.Type.Fixed:
                    avroType = typeof(byte[]);
                    break;
                case Avro.Schema.Type.Error:
                    return null;
                default:
                    return null;
            }

            foreach (var c in _convertors)
            {
                if (c.GetAvroType() == avroType && c.GetPropertyType() == property.PropertyType)
                {
                    return c;
                }
            }

            return null;
        }
    }
}
