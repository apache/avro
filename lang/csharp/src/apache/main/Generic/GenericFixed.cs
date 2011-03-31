/**
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

namespace Avro.Generic
{
    /// <summary>
    /// The default type used by GenericReader and GenericWriter for objects for FixedSchema
    /// </summary>
    public class GenericFixed
    {
        protected readonly byte[] value;
        private FixedSchema schema;

        public FixedSchema Schema
        {
            get
            {
                return schema;
            }

            set
            {
                if (!(value is FixedSchema))
                    throw new AvroException("Schema " + value.Name + " in set is not FixedSchema");

                if ((value as FixedSchema).Size != this.value.Length)
                    throw new AvroException("Schema " + value.Name + " Size " + (value as FixedSchema).Size + "is not equal to bytes length " + this.value.Length);

                schema = value;
            }
        }

        public GenericFixed(FixedSchema schema)
        {
            value = new byte[schema.Size];
            this.Schema = schema;
        }

        public GenericFixed(FixedSchema schema, byte[] value)
        {
            this.value = new byte[schema.Size];
            this.Schema = schema;
            Value = value;
        }

        protected GenericFixed(uint size) 
        {
            this.value = new byte[size];
        }

        public byte[] Value
        {
            get { return this.value; }
            set
            {
                if (value.Length == this.value.Length)
                {
                    Array.Copy(value, this.value, value.Length);
                    return;
                }
                throw new AvroException("Invalid length for fixed: " + value.Length + ", (" + Schema + ")");
            }
        }

        public override bool Equals(object obj)
        {
            if (this == obj) return true;
            if (obj != null && obj is GenericFixed)
            {
                GenericFixed that = obj as GenericFixed;
                if (that.Schema.Equals(this.Schema))
                {
                    for (int i = 0; i < value.Length; i++) if (this.value[i] != that.value[i]) return false;
                    return true;
                }
            }
            return false;
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
