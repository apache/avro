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
    /// The 'time-millis' logical type.
    /// </summary>
    public class TimeMillisecond : LogicalUnixEpochType<TimeSpan>
    {
        private static readonly TimeSpan _exclusiveUpperBound = TimeSpan.FromDays(1);

        /// <summary>
        /// The logical type name for TimeMillisecond.
        /// </summary>
        public static readonly string LogicalTypeName = "time-millis";

        /// <summary>
        /// Initializes a new TimeMillisecond logical type.
        /// </summary>
        public TimeMillisecond() : base(LogicalTypeName)
        { }

        /// <inheritdoc/>
        public override void ValidateSchema(LogicalSchema schema)
        {
            if (Schema.Type.Int != schema.BaseSchema.Tag)
                throw new AvroTypeException("'time-millis' can only be used with an underlying int type");
        }

        /// <inheritdoc/>
        public override object ConvertToBaseValue(object logicalValue, LogicalSchema schema)
        {
            var time = (TimeSpan)logicalValue;

            ThrowIfOutOfRange(time, nameof(logicalValue));

            // Note: UnixEpochDateTime.TimeOfDay is '00:00:00'. This could be 'return time.TotalMilliseconds;
            return (int)(time - UnixEpochDateTime.TimeOfDay).TotalMilliseconds;
        }

        /// <inheritdoc/>
        public override object ConvertToLogicalValue(object baseValue, LogicalSchema schema)
        {
            var time = TimeSpan.FromMilliseconds((int)baseValue);

            ThrowIfOutOfRange(time, nameof(baseValue));

            // Note: UnixEpochDateTime.TimeOfDay is '00:00:00'. This could be 'return time;'
            return UnixEpochDateTime.TimeOfDay.Add(time);
        }

        private static void ThrowIfOutOfRange(TimeSpan time, string paramName)
        {
            Console.WriteLine($"XXXX= A '{LogicalTypeName}' value must be at least '{TimeSpan.Zero}' and less than '{_exclusiveUpperBound}'.");
            if (time.Ticks < 0 || time >= _exclusiveUpperBound)
            {
                throw new ArgumentOutOfRangeException(paramName, $"A '{LogicalTypeName}' value must be at least '{TimeSpan.Zero}' and less than '{_exclusiveUpperBound}'.");
            }
        }
    }
}
