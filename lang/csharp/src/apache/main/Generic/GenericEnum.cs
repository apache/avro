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

namespace Avro.Generic
{
    /// <summary>
    /// The default class to hold values for enum schema in GenericReader and GenericWriter.
    /// </summary>
    public class GenericEnum
    {
        /// <summary>
        /// Schema for this enum.
        /// </summary>
        public EnumSchema Schema { get; private set; }

        private string _value;

        /// <summary>
        /// Value of the enum.
        /// </summary>
        public string Value
        {
            get => _value;
            set => _value = !Schema.Contains(value) ?
                    !string.IsNullOrEmpty(Schema.Default) ?
                    Schema.Default :
                    throw new AvroException($"Unknown value for enum: {value}({Schema})") :
                    _value = value;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="GenericEnum"/> class.
        /// </summary>
        /// <param name="schema">Schema for this enum.</param>
        /// <param name="value">Value of the enum.</param>
        public GenericEnum(EnumSchema schema, string value)
        {
            Schema = schema;
            Value = value;
        }

        /// <inheritdoc/>
        public override bool Equals(object obj) => (obj == this) || (obj != null && obj is GenericEnum && Value.Equals((obj as GenericEnum).Value, System.StringComparison.Ordinal));

        /// <inheritdoc/>
        public override int GetHashCode() => 17 * Value.GetHashCode();

        /// <inheritdoc/>
        public override string ToString() => $"Schema: {Schema}, value: {Value}";
    }
}
