/*  Copyright 2019 Pitney Bowes Inc.
 *
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
using System.Collections.Concurrent;
using Avro;

namespace Avro.Reflect
{
    public class DotnetClass
    {
        private ConcurrentDictionary<string, DotnetProperty> _propertyMap = new ConcurrentDictionary<string, DotnetProperty>();

        public DotnetClass(Type t, RecordSchema r, ClassCache cache)
        {
            _type = t;
            foreach (var f in r.Fields)
            {
                bool hasAttribute = false;
                PropertyInfo prop = GetPropertyInfo(f);

                foreach (var attr in prop.GetCustomAttributes(true))
                {
                    var avroAttr = attr as AvroAttribute;
                    if (avroAttr != null)
                    {
                        hasAttribute = true;
                        _propertyMap.TryAdd(f.Name, new DotnetProperty(prop, f.Schema.Tag, avroAttr.Converter, cache));
                        break;
                    }
                }

                if (!hasAttribute)
                {
                    _propertyMap.TryAdd(f.Name, new DotnetProperty(prop, f.Schema.Tag, cache));
                }
            }
        }

        private PropertyInfo GetPropertyInfo(Field f)
        {
            var prop = _type.GetProperty(f.Name);
            if (prop == null)
            {
                foreach(var p in _type.GetProperties())
                {
                    foreach (var attr in p.GetCustomAttributes(true))
                    {
                        var avroAttr = attr as AvroAttribute;
                        if (avroAttr != null && avroAttr.FieldName != null && avroAttr.FieldName == f.Name)
                        {
                            prop = p;
                            break;
                        }
                    }
                }
                throw new AvroException($"Class {_type.Name} doesnt contain property {f.Name}");
            }

            return prop;
        }

        private Type _type { get; set; }

        public object GetValue(object o, Field f)
        {
            DotnetProperty p;
            if (!_propertyMap.TryGetValue(f.Name, out p))
            {
                throw new AvroException($"ByPosClass doesnt contain property {f.Name}");
            }
            return p.GetValue(o, f.Schema);
        }

        public void SetValue(object o, Field f, object v)
        {
            DotnetProperty p;
            if (!_propertyMap.TryGetValue(f.Name, out p))
            {
                throw new AvroException($"ByPosClass doesnt contain property {f.Name}");
            }
            p.SetValue(o, v, f.Schema);
        }

        public Type GetClassType()
        {
            return _type;
        }

        public Type GetPropertyType(Field f)
        {
            DotnetProperty p;
            if (!_propertyMap.TryGetValue(f.Name, out p))
            {
                throw new AvroException($"ByPosClass doesnt contain property {f.Name}");
            }
            return p.GetPropertyType();
        }
    }
}
