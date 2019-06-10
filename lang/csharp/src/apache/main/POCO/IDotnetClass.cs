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
using Avro;

namespace Avro.POCO
{
    /// <summary>
    /// Container for a C# class. Knows about attributes, converter etc.
    /// </summary>
    public interface IDotnetClass
    {
        /// <summary>
        /// Returns the type of the class that is wrapped
        /// </summary>
        /// <returns></returns>
        Type GetClassType();

        /// <summary>
        /// Returns the type of a property corresponding to schema field f (after applying any converters)
        /// </summary>
        /// <param name="f"></param>
        /// <returns></returns>
        Type GetPropertyType( Field f );

        /// <summary>
        /// Get the value of a property after converters are applied
        /// </summary>
        /// <param name="o"></param>
        /// <param name="f"></param>
        /// <returns></returns>
        object GetValue(object o, Field f);

        /// <summary>
        /// Set the value of a property after converters are applied
        /// </summary>
        /// <param name="o"></param>
        /// <param name="f"></param>
        /// <param name="v"></param>
        void SetValue(object o, Field f, object v);
    }
}
