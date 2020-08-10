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

namespace Avro.Reflect
{
    /// <summary>
    /// Convert C# DateTimeOffset properties to long unix time
    /// </summary>
    public partial class DateTimeOffsetToLongConverter : IAvroFieldConverter
    {
        /// <summary>
        /// Convert from DateTimeOffset to Unix long
        /// </summary>
        /// <param name="o">DateTimeOffset</param>
        /// <param name="s">Schema</param>
        /// <returns></returns>
        public object ToAvroType(object o, Schema s)
        {
            var dt = (DateTimeOffset)o;
            long milliseconds = dt.UtcDateTime.Ticks / TimeSpan.TicksPerMillisecond;
            const long unixEpochMilliseconds = 62_135_596_800_000; // Milliseconds to 1.1.1970
            return milliseconds - unixEpochMilliseconds;
        }

        /// <summary>
        /// Convert from Unix long to DateTimeOffset
        /// </summary>
        /// <param name="o">long</param>
        /// <param name="s">Schema</param>
        /// <returns></returns>
        public object FromAvroType(object o, Schema s)
        {
            const long unixEpochTicks = 621_355_968_000_000_000;// Ticks to 1.1.1970
            long milliseconds = (long)o;
            long ticks = milliseconds * TimeSpan.TicksPerMillisecond + unixEpochTicks;
            return new DateTimeOffset(ticks, TimeSpan.Zero);
        }
    }
}
