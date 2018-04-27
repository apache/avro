﻿/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Avro.Generic;

namespace Avro.Specific
{
    /// <summary>
    /// Base class for all generated classes
    /// </summary>
    public abstract class SpecificFixed : GenericFixed
    {
        public SpecificFixed(uint size) : base(size) { }
        public abstract new Schema Schema { get; }

        protected bool Equals(SpecificFixed obj)
        {
            if (this == obj) return true;
            if (obj != null && obj is SpecificFixed)
            {
                SpecificFixed that = obj as SpecificFixed;
                if (that.Schema.Equals(this.Schema))
                {
                    for (int i = 0; i < value.Length; i++) if (this.value[i] != that.Value[i]) return false;
                    return true;
                }
            }
            return false;

        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((SpecificFixed) obj);
        }

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
