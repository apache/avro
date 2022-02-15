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
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Avro.Generic
{
    /// <summary>
    /// The default type used by GenericReader and GenericWriter for RecordSchema.
    /// </summary>
    public class GenericRecord : IEquatable<GenericRecord>
    {
        /// <summary>
        /// Gets the schema for this record.
        /// </summary>
        /// <value>
        /// The schema.
        /// </value>
        public RecordSchema Schema { get; private set; }

        private readonly object[] _contents;

        /// <summary>
        /// Initializes a new instance of the <see cref="GenericRecord" /> class.
        /// </summary>
        /// <param name="schema">Schema for this record.</param>
        public GenericRecord(RecordSchema schema)
        {
            Schema = schema;
            _contents = new object[schema.Fields.Count];
        }

        /// <summary>
        /// Returns the value of the field with the given name.
        /// </summary>
        /// <value>
        /// The <see cref="object" />.
        /// </value>
        /// <param name="fieldName">Name of the field.</param>
        /// <returns>
        /// Value of the field with the given name.
        /// </returns>
        /// <exception cref="KeyNotFoundException">Key name: {fieldName}</exception>
        public object this[string fieldName] => Schema.TryGetField(fieldName, out Field field) ? _contents[field.Pos] :
                    throw new KeyNotFoundException($"Key name: {fieldName}");

        /// <summary>
        /// Sets the value for a field. You may call this method multiple times with the same
        /// field name to change its value. The given field name must exist in the schema.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="fieldValue">Value of the field.</param>
        /// <exception cref="AvroException">No such field: {fieldName}</exception>
        public void Add(string fieldName, object fieldValue)
        {
            if (Schema.TryGetField(fieldName, out Field field))
            {
                // TODO: Use a matcher to verify that object has the right type for the field.
                _contents[field.Pos] = fieldValue;
                return;
            }

            throw new AvroException($"No such field: {fieldName}");
        }

        /// <summary>
        /// Gets the value the specified field name.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="result">When this method returns true, contains the value of the specified field;
        /// otherwise, null.</param>
        /// <returns>
        /// True if the field was found in the record. This method will only return true if
        /// <see cref="Add(string, object)" /> has been called for the given field name.
        /// </returns>
        public bool TryGetValue(string fieldName, out object result)
        {
            if (!Schema.TryGetField(fieldName, out Field field))
            {
                result = null;
                return false;
            }

            result = _contents[field.Pos];
            return true;
        }

        /// <summary>
        /// Returns the value of the field with the given position.
        /// </summary>
        /// <param name="fieldPos">The position of the field.</param>
        /// <returns>
        /// Value of the field with the given position.
        /// </returns>
        /// <exception cref="IndexOutOfRangeException"><paramref name="fieldPos" /></exception>
        public object GetValue(int fieldPos) => _contents[fieldPos];

        /// <summary>
        /// Adds the value in the specified field position.
        /// </summary>
        /// <param name="fieldPos">Position of the field.</param>
        /// <param name="fieldValue">The value to add.</param>
        /// <exception cref="IndexOutOfRangeException"><paramref name="fieldPos" />.</exception>
        public void Add(int fieldPos, object fieldValue) => _contents[fieldPos] = fieldValue;

        /// <summary>
        /// Gets the value in the specified field position.
        /// </summary>
        /// <param name="fieldPos">Position of the field.</param>
        /// <param name="result">When this method returns true, contains the value of the specified field;
        /// otherwise, null.</param>
        /// <returns>
        /// True if the field position is valid.
        /// </returns>
        public bool TryGetValue(int fieldPos, out object result)
        {
            if (fieldPos < _contents.Length)
            {
                result = _contents[fieldPos];
                return true;
            }

            result = null;
            return false;
        }

        /// <inheritdoc/>
        public override bool Equals(object obj) => this == obj || (obj is GenericRecord genericRecord && Equals(genericRecord));

        /// <inheritdoc/>
        public bool Equals(GenericRecord other) => Schema.Equals(other.Schema) && ArraysEqual(_contents, other._contents);

        /// <summary>
        /// Validates the dictionaries contain the same values
        /// </summary>
        /// <param name="left">The left.</param>
        /// <param name="right">The right.</param>
        /// <returns>True, if the dictionaries contain the same values</returns>
        private static bool MapsEquals(IDictionary left, IDictionary right)
        {
            if (left.Count != right.Count)
            {
                return false;
            }

            foreach (DictionaryEntry kv in left)
            {
                if (!right.Contains(kv.Key) || !ObjectsEquals(right[kv.Key], kv.Value))
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Validates the objects are equal.
        /// </summary>
        /// <param name="left">The left.</param>
        /// <param name="right">The right.</param>
        /// <returns>true, if the objects are equal</returns>
        private static bool ObjectsEquals(object left, object right)
        {
            // Ignoring IDE0046 for readability

            if (left == null && right == null)
            {
                return true;
            }

            if (left == null || right == null)
            {
                return false;
            }

            if (left is Array leftArray)
            {
                return right is Array rightArray && ArraysEqual(leftArray, rightArray);
            }

            if (left is IDictionary leftDictionary)
            {
                return right is IDictionary rightDictionary && MapsEquals(leftDictionary, rightDictionary);
            }

            return left.Equals(right);
        }

        /// <summary>
        /// Validates arrays are equal
        /// </summary>
        /// <param name="left">The left.</param>
        /// <param name="right">The right.</param>
        /// <returns>true, if the arrays contain the same values</returns>
        private static bool ArraysEqual(Array left, Array right)
        {
            if (left.Length != right.Length)
            {
                return false;
            }

            for (int i = 0; i < left.Length; i++)
            {
                if (!ObjectsEquals(left.GetValue(i), right.GetValue(i)))
                {
                    return false;
                }
            }

            return true;
        }

        /// <inheritdoc/>
        public override int GetHashCode() => 31 * _contents.GetHashCode();

        /// <inheritdoc/>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("Schema: ");
            sb.Append(Schema);
            sb.Append(", contents: ");
            sb.Append("{ ");
            foreach (Field field in Schema.Fields)
            {
                sb.Append(field.Name);
                sb.Append(": ");
                sb.Append(_contents[field.Pos]);
                sb.Append(", ");
            }

            sb.Append('}');
            return sb.ToString();
        }
    }
}
