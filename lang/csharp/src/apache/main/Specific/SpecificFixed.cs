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

using System.Linq;
using Avro.Generic;

namespace Avro.Specific
{
    /// <summary>
    /// Base class for all generated classes
    /// </summary>
    public abstract class SpecificFixed : GenericFixed
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SpecificFixed"/> class.
        /// </summary>
        /// <param name="size"></param>
        public SpecificFixed(uint size) : base(size) { }

        /// <summary>
        /// Schema of this instance.
        /// </summary>
        public abstract new Schema Schema { get; }

        /// <summary>
        /// Determines whether the provided fixed is equivalent this instance.
        /// </summary>
        /// <param name="obj">Specific Fixed to compare.</param>
        /// <returns>
        /// True if the Specific Fixed instances have equal values.
        /// </returns>
        protected bool Equals(SpecificFixed obj)
        {
            if (this == obj)
            {
                return true;
            }

            return obj.Schema.Equals(Schema)
                && value.SequenceEqual(obj.value);
        }

        /// <inheritdoc/>
        public override bool Equals(object obj)
        {
            if(obj == this)
            {
                return true;
            }

            return obj != null
                && obj.GetType() == typeof(SpecificFixed)
                && Equals((SpecificFixed)obj);
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            int result = Schema.GetHashCode();
            foreach (byte b in value)
            {
                result += 23 * b;
            }
            return result;
        }
    }
}
