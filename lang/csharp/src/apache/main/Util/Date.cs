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

namespace Avro.Util
{
    /// <summary>
    /// The 'date' logical type.
    /// </summary>
    public class Date : LogicalUnixEpochType<DateTime>
    {
        /// <summary>
        /// The logical type name for Date.
        /// </summary>
        public static readonly string LogicalTypeName = "date";

        /// <summary>
        /// Initializes a new Date logical type.
        /// </summary>
        public Date() : base(LogicalTypeName)
        { }


        /// <inheritdoc/>
        public override void ValidateSchema(LogicalSchema schema)
        {
            if (Schema.Type.Int != schema.BaseSchema.Tag)
                throw new AvroTypeException("'date' can only be used with an underlying int type");
        }

        /// <inheritdoc/>
        public override object ConvertToBaseValue(object logicalValue, LogicalSchema schema)
        {
            return ConvertToBaseValue<int>(logicalValue, schema);
        }

        /// <inheritdoc/>
        public override T ConvertToBaseValue<T>(object logicalValue, LogicalSchema schema)
        {
            var date = ((DateTime)logicalValue).Date;
            if (typeof(T) == typeof(DateTime))
            {
                return (T)(object)date;
            }
            if (typeof(T) == typeof(int))
            {
                return (T)(object)(date - UnixEpochDateTime).Days;
            }
            if (typeof(T) == typeof(string))
            {
                return (T)(object)(date.ToString("yyyy-MM-dd"));
            }
            throw new AvroTypeException($"Cannot convert logical type '{Name}' to '{typeof(T).Name}'");
        }

        /// <inheritdoc/>
        public override object ConvertToLogicalValue(object baseValue, LogicalSchema schema)
        {
            if (baseValue is int)
            {
                var noDays = (int)baseValue;
                return UnixEpochDateTime.AddDays(noDays);
            }
            else if (baseValue is DateTime)
            {
                return ((DateTime)baseValue).Date;
            }
            else if (baseValue is string)
            {
                return DateTime.Parse((string)baseValue).Date;
            }
            throw new AvroTypeException($"Cannot convert base value '{baseValue}' to logical type '{Name}'");            
        }
    }
}
