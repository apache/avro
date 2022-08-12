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
using System.Reflection;
using System.Text;
using Avro.Reflect.Converter;
using Avro.Reflect.Model;

namespace Avro.Reflect.Service
{
    /// <summary>
    /// Factory to create DotnetClass and related objects
    /// </summary>
    public class DotnetclassFactory : IDotnetclassFactory
    {
        private readonly IConverterService _converterService;

        /// <summary>
        /// Public constructor
        /// </summary>
        /// <param name="converterService"></param>
        public DotnetclassFactory(IConverterService converterService)
        {
            _converterService = converterService;
        }

        /// <summary>
        /// Create IDotnetClass object
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public IDotnetClass CreateDotnetClass(RecordSchema schema, Type type)
        {
            Dictionary<string, IDotnetProperty> properties = new Dictionary<string, IDotnetProperty>();

            foreach (var field in schema.Fields)
            {
                PropertyInfo property = GetPropertyInfo(field, type);
                properties.Add(field.Name, CreateDotnetProperty(field.Schema, property));
            }

            return new DotnetClass(type, properties);
        }

        /// <summary>
        /// Create DotnetProperty object
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="property"></param>
        /// <returns></returns>
        private IDotnetProperty CreateDotnetProperty(Schema schema, PropertyInfo property)
        {
            var converter = _converterService.GetConverter(schema, property);
            var type = converter?.GetPropertyType() ?? property.PropertyType;

            if (IsTypeCompatibleWithSchema(schema, type))
                return new DotnetProperty(property, converter);

            throw new AvroException($"Property {property.Name} in object {property.DeclaringType} isn't compatible with Avro schema type {schema.Tag}");
        }

        private PropertyInfo GetPropertyInfo(Field field, Type type)
        {
            var prop = type.GetProperty(field.Name);
            if (prop != null)
            {
                return prop;
            }
            foreach (var p in type.GetProperties())
            {
                foreach (var attr in p.GetCustomAttributes(true))
                {
                    var avroAttr = attr as AvroFieldAttribute;
                    if (avroAttr != null && avroAttr.FieldName != null && avroAttr.FieldName == field.Name)
                    {
                        return p;
                    }
                }
            }

            throw new AvroException($"Class {type.Name} doesn't contain property {field.Name}");
        }

        private bool IsTypeCompatibleWithSchema(Schema schema, Type propType)
        {
            var schemaTag = schema.Tag;

            switch (schemaTag)
            {
                case Avro.Schema.Type.Null:
                    return (Nullable.GetUnderlyingType(propType) != null) || (!propType.IsValueType);
                case Avro.Schema.Type.Boolean:
                    return propType == typeof(bool);
                case Avro.Schema.Type.Int:
                    return propType == typeof(int);
                case Avro.Schema.Type.Long:
                    return propType == typeof(long);
                case Avro.Schema.Type.Float:
                    return propType == typeof(float);
                case Avro.Schema.Type.Double:
                    return propType == typeof(double);
                case Avro.Schema.Type.Bytes:
                    return propType == typeof(byte[]);
                case Avro.Schema.Type.String:
                    return typeof(string).IsAssignableFrom(propType);
                case Avro.Schema.Type.Record:
                    //TODO: this probably should work for struct too
                    return propType.IsClass;
                case Avro.Schema.Type.Enumeration:
                    return propType.IsEnum;
                case Avro.Schema.Type.Array:
                    return typeof(IEnumerable).IsAssignableFrom(propType);
                case Avro.Schema.Type.Map:
                    return typeof(IDictionary).IsAssignableFrom(propType);
                case Avro.Schema.Type.Union:
                    return true;
                case Avro.Schema.Type.Fixed:
                    return propType == typeof(byte[]);
                case Avro.Schema.Type.Error:
                    return propType.IsClass;
                case Avro.Schema.Type.Logical:
                    var logicalSchema = (LogicalSchema)schema;
                    var type = logicalSchema.LogicalType.GetCSharpType(false);
                    return type == propType;
            }

            return false;
        }
    }
}
