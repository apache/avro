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
using System.Reflection;

namespace Avro.POCO
{
    public class AvroFieldAttribute : Attribute
    {
        /// <summary>
        /// Sequence number of the field in the Avro Schema
        /// </summary>
        /// <value></value>
        public int FieldPos { get; set; }

        /// <summary>
        /// Convert the property into a standard Avro type - e.g. DateTimeOffset to long
        /// </summary>
        /// <value></value>
        public IAvroFieldConverter Converter { get; set; }

        /// <summary>
        /// Attribute to hold field position and optionally a converter
        /// </summary>
        /// <param name="fieldPos"></param>
        /// <param name="converter"></param>
        public AvroFieldAttribute(int fieldPos, Type converter = null)
        {
            FieldPos = fieldPos;
            if (converter != null)
            {
                Converter = (IAvroFieldConverter)Activator.CreateInstance(converter);
            }
        }
        public AvroFieldAttribute(Type converter)
        {
            FieldPos = -1;
            if (converter != null)
            {
                Converter = (IAvroFieldConverter)Activator.CreateInstance(converter);
            }
        }
    }
}
