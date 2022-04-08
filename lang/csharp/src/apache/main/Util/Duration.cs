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
    /// The 'duration' logical type.
    /// </summary>
    public class Duration : LogicalType
    {
        /// <summary>
        /// The logical type name for Duration.
        /// </summary>
        public static readonly string LogicalTypeName = "duration";

        /// <summary>
        /// Initializes a new Duration logical type.
        /// </summary>
        public Duration()
            : base(LogicalTypeName)
        {
        }

        /// <inheritdoc/>
        public override void ValidateSchema(LogicalSchema schema)
        {
            if (Schema.Type.Fixed != schema.BaseSchema.Tag)
            {
                throw new AvroTypeException($"'{LogicalTypeName}' can only be used with an underlying bytes or fixed type");
            }

            if (((FixedSchema)schema.BaseSchema).Size != 12)
            {
                throw new AvroTypeException($"'{LogicalTypeName}' requires a 'size' property that is set to 12");
            }
        }

        /// <inheritdoc/>      
        public override object ConvertToBaseValue(object logicalValue, LogicalSchema schema)
        {
            AvroDuration duration = (AvroDuration)logicalValue;

            byte[] baseValue = new byte[12];

            BitConverter.GetBytes(duration.Months).CopyTo(baseValue, 0);
            BitConverter.GetBytes(duration.Days).CopyTo(baseValue, 4);
            BitConverter.GetBytes(duration.Milliseconds).CopyTo(baseValue, 8);

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(baseValue, 0, 4);
                Array.Reverse(baseValue, 4, 4);
                Array.Reverse(baseValue, 8, 4);
            }

            return baseValue;
        }

        /// <inheritdoc/>
        public override object ConvertToLogicalValue(object baseValue, LogicalSchema schema)
        {
            byte[] buffer = (byte[])baseValue;

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(buffer, 0, 4);
                Array.Reverse(buffer, 4, 4);
                Array.Reverse(buffer, 8, 4);
            }

            int months = BitConverter.ToInt32(buffer, 0);
            int days = BitConverter.ToInt32(buffer, 4);
            int milliseconds = BitConverter.ToInt32(buffer, 8);

            return new AvroDuration(months, days, milliseconds);
        }

        /// <inheritdoc/>
        public override Type GetCSharpType(bool nullible)
        {
            return nullible ? typeof(AvroDuration?) : typeof(AvroDuration);
        }

        /// <inheritdoc/>
        public override bool IsInstanceOfLogicalType(object logicalValue)
        {
            return logicalValue is AvroDuration;
        }
    }
}
