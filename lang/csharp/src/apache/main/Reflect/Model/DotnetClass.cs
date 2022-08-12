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
using Avro;
using System.Collections.Generic;

namespace Avro.Reflect.Model
{
    /// <summary>
    /// Collection of DotNetProperty objects to repre
    /// </summary>
    public class DotnetClass : IDotnetClass
    {
        private Dictionary<string, IDotnetProperty> _propertyMap;
        private Type _type;

        /// <summary>
        /// Public constructor
        /// </summary>
        /// <param name="t"></param>
        /// <param name="propertyMap"></param>
        public DotnetClass(Type t, Dictionary<string, IDotnetProperty> propertyMap)
        {
            _type = t;
            _propertyMap = propertyMap;
        }

        /// <summary>
        /// Return the value of a property from an object referenced by a field
        /// </summary>
        /// <param name="o">the object</param>
        /// <param name="f">FieldSchema used to look up the property</param>
        /// <returns></returns>
        public object GetValue(object o, Field f)
        {
            IDotnetProperty p;
            if (!_propertyMap.TryGetValue(f.Name, out p))
            {
                throw new AvroException($"ByPosClass doesn't contain property {f.Name}");
            }

            return p.GetValue(o, f.Schema);
        }

        /// <summary>
        /// Set the value of a property in a C# object
        /// </summary>
        /// <param name="o">the object</param>
        /// <param name="f">field schema</param>
        /// <param name="v">value for the property referenced by the field schema</param>
        public void SetValue(object o, Field f, object v)
        {
            IDotnetProperty p;
            if (!_propertyMap.TryGetValue(f.Name, out p))
            {
                throw new AvroException($"ByPosClass doesn't contain property {f.Name}");
            }

            p.SetValue(o, v, f.Schema);
        }

        /// <summary>
        /// Return the type of the Class
        /// </summary>
        /// <returns>The </returns>
        public Type GetClassType()
        {
            return _type;
        }

        /// <summary>
        /// Return the type of a property referenced by a field
        /// </summary>
        /// <param name="f"></param>
        /// <returns></returns>
        public Type GetPropertyType(Field f)
        {
            IDotnetProperty p;
            if (!_propertyMap.TryGetValue(f.Name, out p))
            {
                throw new AvroException($"ByPosClass doesn't contain property {f.Name}");
            }

            return p.GetPropertyType();
        }
    }
}
