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
using System.Collections.Generic;
using System.Text;

namespace Avro.Reflect.Model
{
    /// <summary>
    /// Interface that represent dotnet class
    /// </summary>
    public interface IDotnetClass
    {
        /// <summary>
        /// Return the type of the Class
        /// </summary>
        /// <returns>The </returns>
        Type GetClassType();

        /// <summary>
        /// Return the type of a property referenced by a field
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        Type GetPropertyType(Field field);

        /// <summary>
        /// Return the value of a property from an object referenced by a field
        /// </summary>
        /// <param name="o">the object</param>
        /// <param name="field">FieldSchema used to look up the property</param>
        /// <returns></returns>
        object GetValue(object o, Field field);

        /// <summary>
        /// Set the value of a property in a C# object
        /// </summary>
        /// <param name="o">the object</param>
        /// <param name="field">field schema</param>
        /// <param name="v">value for the property referenced by the field schema</param>
        void SetValue(object o, Field field, object v);
    }
}
