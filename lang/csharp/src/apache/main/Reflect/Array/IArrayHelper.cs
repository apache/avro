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
using System.Text;

namespace Avro.Reflect.Array
{
    /// <summary>
    /// Interface to help serialize and deserialize arrays. Arrays need the following methods Count(), Add(), Clear().true
    /// This class allows these methods to be specified externally to the collection.
    /// </summary>
    public interface IArrayHelper
    {
        /// <summary>
        /// Type of the array to create when deserializing
        /// </summary>
        Type ArrayType { get; }

        /// <summary>
        /// Return the number of elements in the array.
        /// </summary>
        /// <value></value>
        int Count();

        /// <summary>
        /// Add an element to the array.
        /// </summary>
        /// <param name="o">Element to add to the array.</param>
        void Add(object o);

        /// <summary>
        /// Clear the array.
        /// </summary>
        /// <value></value>
        void Clear();
    }
}
