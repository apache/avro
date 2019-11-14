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

namespace Avro.Util
{
    /// <summary>
    /// Base for all logical type implementations that are based on the Unix Epoch date/time.
    /// </summary>
    public abstract class LogicalUnixEpochType<T> : LogicalType
    {
        /// <summary>
        /// The date and time of the Unix Epoch.
        /// </summary>
        protected static readonly DateTime UnixEpocDateTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// The date of the Unix Epoch
        /// </summary>
        protected static readonly DateTime UnixEpocDate = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).Date;

        /// <summary>
        /// Initializes the base logical type.
        /// </summary>
        /// <param name="name">The logical type name.</param>
        protected LogicalUnixEpochType(string name)
            : base(name)
        { }

        /// <summary>
        /// Retrieve the .NET type that is represented by the logical type implementation.
        /// </summary>
        /// <param name="nullible">A flag indicating whether it should be nullible.</param>
        public override string GetCSharpTypeName(bool nullible)
        {
            var typeName = typeof(T).ToString();
            return nullible ? "System.Nullable<" + typeName + ">" : typeName;
        }

        /// <summary>
        /// Determines if a given object is an instance of the logical Unix Epoch based date/time.
        /// </summary>
        /// <param name="logicalValue">The logical value to test.</param>
        public override bool IsInstanceOfLogicalType(object logicalValue)
        {
            return logicalValue is T;
        }
    }
}
