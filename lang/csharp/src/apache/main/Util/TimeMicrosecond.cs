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
    /// The 'time-micros' logical type.
    /// </summary>
    public class TimeMicrosecond : LogicalUnixEpochType<TimeSpan>
    {
        private static readonly TimeSpan _exclusiveUpperBound = TimeSpan.FromDays(1);
        private const long _ticksPerMicrosecond = TimeSpan.TicksPerMillisecond / 1000;

        /// <summary>
        /// The logical type name for TimeMicrosecond.
        /// </summary>
        public static readonly string LogicalTypeName = "time-micros";

        /// <summary>
        /// Initializes a new TimeMicrosecond logical type.
        /// </summary>
        public TimeMicrosecond() : base(LogicalTypeName)
        { }

        /// <inheritdoc/>
        public override void ValidateSchema(LogicalSchema schema)
        {
            if (Schema.Type.Long != schema.BaseSchema.Tag)
                throw new AvroTypeException("'time-micros' can only be used with an underlying long type");
        }

        /// <inheritdoc/>
        public override object ConvertToBaseValue(object logicalValue, LogicalSchema schema)
        {
            var time = (TimeSpan)logicalValue;

            ThrowIfOutOfRange(time, nameof(logicalValue));

            // Note: UnixEpochDateTime.TimeOfDay is '00:00:00'. This could be 'return time.Ticks / _ticksPerMicrosecond';
            return (time - UnixEpochDateTime.TimeOfDay).Ticks / _ticksPerMicrosecond;
        }

        /// <inheritdoc/>
        public override object ConvertToLogicalValue(object baseValue, LogicalSchema schema)
        {
            var time = TimeSpan.FromTicks((long)baseValue * _ticksPerMicrosecond);

            ThrowIfOutOfRange(time, nameof(baseValue));

            // Note: UnixEpochDateTime.TimeOfDay is '00:00:00', so the Add is meaningless. This could be 'return time;'
            return UnixEpochDateTime.TimeOfDay.Add(time);
        }

        private static void ThrowIfOutOfRange(TimeSpan time, string paramName)
        {
            if (time.Ticks < 0 || time >= _exclusiveUpperBound)
            {
                throw new ArgumentOutOfRangeException(paramName, $"A '{LogicalTypeName}' value must be at least '00:00:00' and less than '1.00:00:00'.");
            }
        }
    }
}
