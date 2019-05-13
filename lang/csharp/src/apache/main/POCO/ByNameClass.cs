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
using System.Collections.Concurrent;
using Avro;

namespace Avro.POCO
{
    public class ByNameClass : IDotnetClass
    {
        private ConcurrentDictionary<string, DotnetProperty> _propertyMap = new ConcurrentDictionary<string, DotnetProperty>();

        public ByNameClass(Type t, RecordSchema r)
        {
            _type = t;
            foreach (var f in r.Fields)
            {
                bool hasAttribute = false;
                var prop = _type.GetProperty(f.Name);
                if (prop == null)
                {
                    throw new AvroException( $"Class {_type.Name} doesnt contain property {f.Name}");
                }

                foreach (var attr in prop.GetCustomAttributes(true))
                {
                    var avroAttr = attr as AvroFieldAttribute;
                    if (avroAttr != null)
                    {
                        hasAttribute = true;
                        _propertyMap.TryAdd(f.Name, new DotnetProperty(prop, avroAttr.Converter));
                    }
                }

                if (!hasAttribute)
                {
                        _propertyMap.TryAdd(f.Name, new DotnetProperty(prop));
                }
            }
        }

        private Type _type { get; set; }

        private void AddProperty(Field f, DotnetProperty p)
        {
            if (_propertyMap.ContainsKey(f.Name))
            {
                throw new AvroException($"ByPosClass already contains property {f.Name}: {_propertyMap[f.Name]}");
            }
            _propertyMap.AddOrUpdate(f.Name, p, (i, v)=>v);
        }

        public object GetValue(object o, Field f)
        {
            DotnetProperty p;
            if (!_propertyMap.TryGetValue(f.Name, out p))
            {
                throw new AvroException($"ByPosClass doesnt contain property {f.Name}");
            }
            return p.GetValue(o);
        }

        public void SetValue(object o, Field f, object v)
        {
            DotnetProperty p;
            if (!_propertyMap.TryGetValue(f.Name, out p))
            {
                throw new AvroException($"ByPosClass doesnt contain property {f.Name}");
            }
            p.SetValue(o, v);
        }

        public Type GetClassType()
        {
            return _type;
        }

        public Type GetFieldType(Field f)
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
