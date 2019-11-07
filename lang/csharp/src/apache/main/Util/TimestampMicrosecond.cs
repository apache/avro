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
    /// The 'timestamp-micros' logical type.
    /// </summary>
    public class TimestampMicrosecond : LogicalUnixEpochType<DateTime>
    {
        /// <summary>
        /// The logical type name for TimestampMicrosecond.
        /// </summary>
        public static readonly string LogicalTypeName = "timestamp-micros";

        /// <summary>
        /// Initializes a new TimestampMicrosecond logical type.
        /// </summary>
        public TimestampMicrosecond() : base(LogicalTypeName)
        { }

        /// <summary>
        /// Applies 'timestamp-micros' logical type validation for a given logical schema.
        /// </summary>
        /// <param name="schema">The schema to be validated.</param>
        public override void ValidateSchema(LogicalSchema schema)
        {
            if (Schema.Type.Long != schema.BaseSchema.Tag)
                throw new AvroTypeException("'timestamp-micros' can only be used with an underlying long type");
        }

        /// <summary>
        /// Converts a logical TimestampMicrosecond to a long representing the number of microseconds since the Unix Epoch.
        /// </summary>
        /// <param name="logicalValue">The logical date to convert.</param>
        /// <param name="schema">The schema that represents the target of the conversion.</param>
        public override object ConvertToBaseValue(object logicalValue, LogicalSchema schema)
        {
            var date = ((DateTime)logicalValue).ToUniversalTime();
            return (long)((date - UnixEpochDateTime).TotalMilliseconds * 1000);
        }

        /// <summary>
        /// Convers a long representing the number of microseconds since the Unix Epoch to a logical TimestampMicrosecond.
        /// </summary>
        /// <param name="baseValue">The number of microseconds since the Unix Epoch.</param>
        /// <param name="schema">The schema that represents the target of the conversion.</param>
        public override object ConvertToLogicalValue(object baseValue, LogicalSchema schema)
        {
            var noMs = (long)baseValue / 1000;
            return UnixEpochDateTime.AddMilliseconds(noMs);
        }
    }
}
