/*
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

namespace Avro.POCO
{
    /// <summary>
    /// Attribute to indicate how the class is mapped to the schema. If true the mapping is by position
    /// and the class must have field attributes. If false the class is mapped by field name. Mapping by field
    /// position allows you to use properties who's name doesnt match the schema.
    ///
    /// If there is no attribute mapping by name is the default.
    /// </summary>
    public class AvroAttribute : Attribute
    {
        /// <summary>
        /// If true the class is mapped by position.
        /// </summary>
        /// <value></value>
        public bool ByPosition { get; set; }

        /// <summary>
        /// Attribute to indicate how the class is mapped to the schema. If true the mapping is by position
        /// and the class must have field attributes. If false the class is mapped by field name. Mapping by field
        /// position allows you to use properties who's name doesnt match the schema.
        ///
        /// If there is no attribute mapping by name is the default.
        /// </summary>
        /// <param name="byPosition"></param>
        public AvroAttribute(bool byPosition)
        {
            ByPosition = byPosition;
        }
    }
}
