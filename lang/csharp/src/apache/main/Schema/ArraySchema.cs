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
using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace Avro
{
    /// <summary>
    /// Class for array type schemas
    /// </summary>
    public class ArraySchema : UnnamedSchema
    {
        /// <summary>
        /// Schema for the array 'type' attribute
        /// </summary>
        public Schema ItemSchema { get; set; }

        /// <summary>
        /// Static class to return a new instance of ArraySchema
        /// </summary>
        /// <param name="jtok">JSON object for the array schema</param>
        /// <param name="props">dictionary that provides access to custom properties</param>
        /// <param name="names">list of named schemas already parsed</param>
        /// <param name="encspace">enclosing namespace for the array schema</param>
        /// <returns>New instance of Array Schema</returns>
        internal static ArraySchema NewInstance(JToken jtok, PropertyMap props, SchemaNames names, string encspace)
        {
            JToken jitem = jtok["items"];
            if (jitem == null)
            {
                throw new AvroTypeException($"Array does not have 'items' at '{jtok.Path}'");
            }

            Schema schema = Schema.ParseJson(jitem, names, encspace);
            return new ArraySchema(schema, props);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ArraySchema"/> class.
        /// </summary>
        /// <param name="items">schema for the array items type</param>
        /// <param name="props">dictionary that provides access to custom properties</param>
        private ArraySchema(Schema items, PropertyMap props) : base(Type.Array, props)
        {
            ItemSchema = items;
        }

        /// <summary>
        /// Writes the array schema in JSON format
        /// </summary>
        /// <param name="writer">JSON writer</param>
        /// <param name="names">list of named schemas already written</param>
        /// <param name="encspace">enclosing namespace</param>
        protected internal override void WriteJsonFields(Newtonsoft.Json.JsonTextWriter writer, SchemaNames names, string encspace)
        {
            writer.WritePropertyName("items");
            ItemSchema.WriteJson(writer, names, encspace);
        }

        /// <summary>
        /// Checks if this schema can read data written by the given schema. Used for decoding data.
        /// </summary>
        /// <param name="writerSchema">writer schema</param>
        /// <returns>true if this and writer schema are compatible based on the AVRO specification, false otherwise</returns>
        public override bool CanRead(Schema writerSchema)
        {
            if (writerSchema.Tag != Tag)
            {
                return false;
            }

            ArraySchema arraySchema = writerSchema as ArraySchema;
            return ItemSchema.CanRead(arraySchema.ItemSchema);
        }

        /// <summary>
        /// Determines whether the specified <see cref="object" />, is equal to this instance.
        /// </summary>
        /// <param name="obj">The <see cref="object" /> to compare with this instance.</param>
        /// <returns>
        ///   <c>true</c> if the specified <see cref="object" /> is equal to this instance; otherwise, <c>false</c>.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (obj == null || !(obj is ArraySchema))
            {
                return false;
            }

            if (this == obj)
            {
                return true;
            }

            return ItemSchema.Equals(((ArraySchema)obj).ItemSchema) && areEqual(((ArraySchema)obj).Props, this.Props);
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
        /// </returns>
        public override int GetHashCode()
        {
            return 29 * ItemSchema.GetHashCode() + getHashCode(Props);
        }
    }
}
