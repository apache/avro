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
using System.Reflection;

namespace Avro.POCO
{
    public class DotnetProperty
    {
        private PropertyInfo _property;
        public IAvroFieldConverter Converter;

        public DotnetProperty(PropertyInfo property, IAvroFieldConverter converter)
        {
            _property = property;
            Converter = converter;
        }

        public DotnetProperty(PropertyInfo property) : this(property, null)
        {
        }

        virtual public Type GetPropertyType()
        {
            if (Converter != null)
            {
                return Converter.GetPropertyType();
            }

            return _property.PropertyType;
        }

        virtual public object GetValue(object o)
        {
            if (Converter != null)
            {
                return Converter.ToAvroType(_property.GetValue(o));
            }

            return _property.GetValue(o);
        }

        virtual public void SetValue(object o, object v)
        {
            if (Converter != null)
            {
                _property.SetValue(o, Converter.FromAvroType(v));
            }
            else
            {
                _property.SetValue(o, v);
            }
        }
    }
}
