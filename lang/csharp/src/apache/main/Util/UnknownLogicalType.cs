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

namespace Avro.Util
{
    /// <summary>
    /// Class UnknownLogicalType.
    /// Implements the <see cref="Avro.Util.LogicalType" />
    /// </summary>
    /// <seealso cref="Avro.Util.LogicalType" />
    public class UnknownLogicalType : LogicalType
    {
        /// <summary>
        /// Gets the schema.
        /// </summary>
        /// <value>The schema.</value>
        public LogicalSchema Schema { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="UnknownLogicalType"/> class.
        /// </summary>
        /// <param name="schema">The schema.</param>
        public UnknownLogicalType(LogicalSchema schema) : base(schema.LogicalTypeName)
        {
            this.Schema = schema;
        }

        /// <summary>
        /// Converts a logical value to an instance of its base type.
        /// </summary>
        /// <param name="logicalValue">The logical value to convert.</param>
        /// <param name="schema">The schema that represents the target of the conversion.</param>
        /// <returns>An object representing the encoded value of the base type.</returns>
        public override object ConvertToBaseValue(object logicalValue, LogicalSchema schema)
        {
            switch (schema.Name)
            {
                case @"string":
                    return (System.String)logicalValue;
                case @"boolean":
                    return (System.Boolean)logicalValue;
                case @"int":
                    return (System.Int32)logicalValue;
                case @"long":
                    return (System.Int64)logicalValue;
                case @"float":
                    return (System.Single)logicalValue;
                case @"double":
                    return (System.Double)logicalValue;
                case @"bytes":
                    return (System.Byte[])logicalValue;
                default:
                    return logicalValue;
            }
        }

        /// <summary>
        /// Converts a base value to an instance of the logical type.
        /// </summary>
        /// <param name="baseValue">The base value to convert.</param>
        /// <param name="schema">The schema that represents the target of the conversion.</param>
        /// <returns>An object representing the encoded value of the logical type.</returns>
        public override object ConvertToLogicalValue(object baseValue, LogicalSchema schema)
        {
            switch (schema.Name)
            {
                case @"string":
                    return (System.String)baseValue;
                case @"boolean":
                    return (System.Boolean)baseValue;
                case @"int":
                    return (System.Int32)baseValue;
                case @"long":
                    return (System.Int64)baseValue;
                case @"float":
                    return (System.Single)baseValue;
                case @"double":
                    return (System.Double)baseValue;
                case @"bytes":
                    return (System.Byte[])baseValue;
                default:
                    return baseValue;
            }
        }

        /// <summary>
        /// Retrieve the .NET type that is represented by the logical type implementation.
        /// </summary>
        /// <param name="nullible">A flag indicating whether it should be nullible.</param>
        /// <returns>Type.</returns>
        public override Type GetCSharpType(bool nullible)
        {
            // handle all Primitive Types
            switch (this.Schema.BaseSchema.Name)
            {
                case @"string":
                    return typeof(System.String);
                case @"boolean":
                    return nullible ? typeof(System.Boolean?) : typeof(System.Boolean);
                case @"int":
                    return nullible ? typeof(System.Int32?) : typeof(System.Int32);
                case @"long":
                    return nullible ? typeof(System.Int64?) : typeof(System.Int64);
                case @"float":
                    return nullible ? typeof(System.Single?) : typeof(System.Single);
                case @"double":
                    return nullible ? typeof(System.Double?) : typeof(System.Double);
                case @"bytes":
                    return nullible ? typeof(System.Byte?[]) : typeof(System.Byte[]);
                default:
                    return typeof(System.Object);
            }
        }

        /// <summary>
        /// Determines if a given object is an instance of the logical type.
        /// </summary>
        /// <param name="logicalValue">The logical value to test.</param>
        /// <returns><c>true</c> if [is instance of logical type] [the specified logical value]; otherwise, <c>false</c>.</returns>
        public override bool IsInstanceOfLogicalType(object logicalValue)
        {
            // handle all Primitive Types
            switch (this.Schema.BaseSchema.Name)
            {
                case @"string":
                    return logicalValue is System.String;
                case @"boolean":
                    return logicalValue is System.Boolean;
                case @"int":
                    return logicalValue is System.Int32;
                case @"long":
                    return logicalValue is System.Int64;
                case @"float":
                    return logicalValue is System.Single;
                case @"double":
                    return logicalValue is System.Double;
                case @"bytes":
                    return logicalValue is System.Byte[];
                default:
                    return true;
            }
        }

    }
}
