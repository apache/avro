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

namespace Avro
{
    /// <summary>
    /// Extensions for the Schema.Type enumeration
    /// </summary>
    public static class SchemaTypeExtensions
    {
        /// <summary>
        /// Converts to Schema.Type.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <returns>
        /// Schema.Type
        /// </returns>
        public static Schema.Type? ToSchemaType(this string value)
        {
            object parsedValue;

            try
            {
                parsedValue = Enum.Parse(typeof(Schema.Type), value, true);
            }
            catch (ArgumentException)
            {
                return null;
            }

            return (Schema.Type)parsedValue;
        }

        /// <summary>
        /// Converts to Schema.Type.
        /// </summary>
        /// <param name="value">The value to convert.</param>
        /// <param name="removeQuotes">if set to <c>true</c> [remove quotes].</param>
        /// <returns>
        /// Schema.Type
        /// </returns>
        public static Schema.Type? ToSchemaType(this string value, bool removeQuotes)
        {
            string newValue = value;

            if(removeQuotes)
            {
                newValue = value.Replace("\"", string.Empty);
            }

            return ToSchemaType(newValue);
        }
    }
}
