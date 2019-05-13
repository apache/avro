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
using Avro;
using Avro.IO;
using Avro.Generic;
using Avro.Specific;

namespace Avro.POCO
{
    /// <summary>
    /// Generic wrapper class for writing data from specific objects
    /// </summary>
    /// <typeparam name="T">type name of specific object</typeparam>
    public class POCOWriter<T> : SpecificWriter<T>
    {
        public POCOWriter(Schema schema) : base(new POCODefaultWriter(typeof(T), schema)) { }
        public POCOWriter(POCODefaultWriter writer) : base(writer) { }
    }
    /// <summary>
    /// Class for writing data from any specific objects
    /// </summary>
    public class POCODefaultWriter : SpecificDefaultWriter
    {

         /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="schema">schema of the object to be written</param>
        public POCODefaultWriter(Type objType, Schema schema)
            : base(schema)
        {
            var rs = schema as RecordSchema;
            if (rs != null)
            {
                ClassCache.LoadClassCache(objType, rs);
            }
        }

        /// <summary>
        /// Serialized a record using the given RecordSchema. It uses GetField method
        /// to extract the field value from the given object.
        /// </summary>
        /// <param name="schema">The RecordSchema to use for serialization</param>
        /// <param name="value">The value to be serialized</param>
        /// <param name="encoder">The Encoder for serialization</param>

        protected override void WriteRecord(RecordSchema schema, object value, Encoder encoder)
        {

            foreach (Field field in schema)
            {
                try
                {
                    var v = ClassCache.GetClass(schema).GetValue(value, field);

                    Write(field.Schema, v, encoder);
                }
                catch (Exception ex)
                {
                    throw new AvroException(ex.Message + " in field " + field.Name, ex);
                }
            }
        }

        /// <summary>
        /// Validates that the record is a fixed record object and that the schema in the object is the
        /// same as the given writer schema. Writes the given fixed record into the given encoder
        /// </summary>
        /// <param name="schema">writer schema</param>
        /// <param name="value">fixed object to write</param>
        /// <param name="encoder">encoder to write to</param>
        protected override void WriteFixed(FixedSchema schema, object value, Encoder encoder)
        {
            var fixedrec = value as byte[];
            if (fixedrec == null)
            {
                throw new AvroTypeException("Fixed object is not derived from byte[]");
            }

            if (fixedrec.Length != schema.Size)
            {
                throw new AvroTypeException($"Fixed object length is not the same as schema length {schema.Size}");
            }

            encoder.WriteFixed(fixedrec);
        }

        /// <summary>
        /// Serialized an array. The default implementation calls EnsureArrayObject() to ascertain that the
        /// given value is an array. It then calls GetArrayLength() and GetArrayElement()
        /// to access the members of the array and then serialize them.
        /// </summary>
        /// <param name="schema">The ArraySchema for serialization</param>
        /// <param name="value">The value being serialized</param>
        /// <param name="encoder">The encoder for serialization</param>
        protected override void WriteArray(ArraySchema schema, object value, Encoder encoder)
        {
            var arr = value as System.Collections.IList;
            if (arr == null)
                throw new AvroTypeException("Array does not implement IList");

            long l = arr.Count;
            encoder.WriteArrayStart();
            encoder.SetItemCount(l);
            for (int i = 0; i < l; i++)
            {
                encoder.StartItem();
                Write(schema.ItemSchema, arr[i], encoder);
            }

            encoder.WriteArrayEnd();
        }

        /// <summary>
        /// Writes the given map into the given encoder.
        /// </summary>
        /// <param name="schema">writer schema</param>
        /// <param name="value">map to write</param>
        /// <param name="encoder">encoder to write to</param>
        protected override void WriteMap(MapSchema schema, object value, Encoder encoder)
        {
            if (value == null)
            {
                throw new AvroTypeException("Map is null - use a union for nullable types");
            }

            var map = value as System.Collections.IDictionary;
            if (map == null)
            {
                throw new AvroTypeException("Map does not implement IDictionary");
            }

            encoder.WriteArrayStart();
            encoder.SetItemCount(map.Count);
            foreach (System.Collections.DictionaryEntry de in map)
            {
                encoder.StartItem();
                encoder.WriteString(de.Key as string);
                Write(schema.ValueSchema, de.Value, encoder);
            }

            encoder.WriteMapEnd();
        }

        /// <summary>
        /// Resolves the given value against the given UnionSchema and serializes the object against
        /// the resolved schema member. The default implementation of this method uses
        /// ResolveUnion to find the member schema within the UnionSchema.
        /// </summary>
        /// <param name="us">The UnionSchema to resolve against</param>
        /// <param name="value">The value to be serialized</param>
        /// <param name="encoder">The encoder for serialization</param>
        protected override void WriteUnion(UnionSchema us, object value, Encoder encoder)
        {
            for (int i = 0; i < us.Count; i++)
            {
                if (Matches(us[i], value))
                {
                    encoder.WriteUnionIndex(i);
                    Write(us[i], value, encoder);
                    return;
                }
            }

            throw new AvroException("Cannot find a match for " + value.GetType() + " in " + us);
        }

        protected override bool Matches(Schema sc, object obj)
        {
            if (obj == null && sc.Tag != Schema.Type.Null)
            {
                return false;
            }

            switch (sc.Tag)
            {
                case Schema.Type.Null:
                    return obj == null;
                case Schema.Type.Boolean:
                    return obj is bool;
                case Schema.Type.Int:
                    return obj is int;
                case Schema.Type.Long:
                    return obj is long;
                case Schema.Type.Float:
                    return obj is float;
                case Schema.Type.Double:
                    return obj is double;
                case Schema.Type.Bytes:
                    return obj is byte[];
                case Schema.Type.String:
                    return obj is string;
                case Schema.Type.Error:
                case Schema.Type.Record:
                    return ClassCache.GetClass((sc as RecordSchema)).GetClassType() == obj.GetType();
                case Schema.Type.Enumeration:
                    return EnumCache.GetEnumeration((sc as EnumSchema)) == obj.GetType();
                case Schema.Type.Array:
                    return obj is System.Collections.IList;
                case Schema.Type.Map:
                    return obj is System.Collections.IDictionary;
                case Schema.Type.Union:
                    return false;   // Union directly within another union not allowed!
                case Schema.Type.Fixed:
                    return obj is byte[];
                default:
                    throw new AvroException("Unknown schema type: " + sc.Tag);
            }
        }
    }
}
