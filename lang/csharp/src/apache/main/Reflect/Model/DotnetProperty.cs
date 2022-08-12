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
using System.Reflection;
using System.Collections;
using Avro.Reflect.Converter;

namespace Avro.Reflect.Model
{
    /// <summary>
    /// Class that represent Dotnet property
    /// </summary>
    internal class DotnetProperty : IDotnetProperty
    {
        private readonly PropertyInfo _property;
        private readonly IAvroFieldConverter _converter;

        internal DotnetProperty(PropertyInfo property, IAvroFieldConverter converter)
        {
            _converter = converter;
            _property = property;
        }

        /// <summary>
        /// Get .Net property type
        /// </summary>
        /// <returns></returns>
        public Type GetPropertyType()
        {
            if (_converter != null)
            {
                return _converter.GetAvroType();
            }

            return _property.PropertyType;
        }

        /// <summary>
        /// Get value
        /// </summary>
        /// <param name="o"></param>
        /// <param name="s"></param>
        /// <returns></returns>
        public object GetValue(object o, Schema s)
        {
            if (_converter != null)
            {
                return _converter.ToAvroType(_property.GetValue(o), s);
            }

            return _property.GetValue(o);
        }

        /// <summary>
        /// Set value
        /// </summary>
        /// <param name="o"></param>
        /// <param name="v"></param>
        /// <param name="s"></param>
        public void SetValue(object o, object v, Schema s)
        {
            if (_converter != null)
            {
                _property.SetValue(o, _converter.FromAvroType(v, s));
            }
            else
            {
                _property.SetValue(o, v);
            }
        }
    }
}
