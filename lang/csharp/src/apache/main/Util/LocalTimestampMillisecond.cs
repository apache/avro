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
    /// The 'local-timestamp-millis' logical type.
    /// </summary>
    public class LocalTimestampMillisecond : LogicalUnixEpochType<DateTime>
    {
        /// <summary>
        /// The logical type name for LocalTimestampMillisecond.
        /// </summary>
        public static readonly string LogicalTypeName = "local-timestamp-millis";

        /// <summary>
        /// Initializes a new LocalTimestampMillisecond logical type.
        /// </summary>
        public LocalTimestampMillisecond()
            : base(LogicalTypeName)
        {
        }

        /// <inheritdoc/>
        public override void ValidateSchema(LogicalSchema schema)
        {
            if (Schema.Type.Long != schema.BaseSchema.Tag)
            {
                throw new AvroTypeException("'local-timestamp-millis' can only be used with an underlying long type");
            }
        }

        /// <inheritdoc/>
        public override object ConvertToBaseValue(object logicalValue, LogicalSchema schema)
        {
            DateTime date = ((DateTime)logicalValue).ToUniversalTime();
            return (long)(date - UnixEpochDateTime).TotalMilliseconds;
        }

        /// <inheritdoc/>
        public override object ConvertToLogicalValue(object baseValue, LogicalSchema schema)
        {
            return UnixEpochDateTime.AddMilliseconds((long)baseValue).ToLocalTime();
        }
    }
}
