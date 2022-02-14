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
using System.Text;
using Newtonsoft.Json.Linq;

namespace Avro.IO
{
    internal static class Resolver
    {
        /// <summary>
        /// Reads the passed JToken default value field and writes it in the specified encoder
        /// </summary>
        /// <param name="encoder">encoder to use for writing</param>
        /// <param name="schema">schema object for the current field</param>
        /// <param name="jtok">default value as JToken</param>
        /// <exception cref="AvroException">
        /// Default boolean value {jtok} is invalid, expected is json boolean.
        /// or
        /// Default int value {jtok} is invalid, expected is json integer.
        /// or
        /// Default long value {jtok} is invalid, expected is json integer.
        /// or
        /// Default float value {jtok} is invalid, expected is json number.
        /// or
        /// Default double value {jtok} is invalid, expected is json number.
        /// or
        /// Default bytes value {jtok} is invalid, expected is json string.
        /// or
        /// Default fixed value {jtok} is invalid, expected is json string.
        /// or
        /// Default fixed value {jtok} is not of expected length {len}
        /// or
        /// Default string value {jtok} is invalid, expected is json string.
        /// or
        /// Default enum value {jtok} is invalid, expected is json string.
        /// or
        /// Default null value {jtok} is invalid, expected is json null.
        /// or
        /// Default array value {jtok} is invalid, expected is json array.
        /// or
        /// Default record value {jtok} is invalid, expected is json object.
        /// or
        /// No default value for field {field.Name}
        /// or
        /// Default map value {jtok} is invalid, expected is json object.
        /// or
        /// Unsupported schema type {schema.Tag}
        /// </exception>
        public static void EncodeDefaultValue(Encoder encoder, Schema schema, JToken jtok)
        {
            if (jtok == null)
            {
                return;
            }

            // Set default encoding
            Encoding encoding = Encoding.GetEncoding("iso-8859-1");

            switch (schema.Tag)
            {
                case Schema.Type.Boolean:
                    if (jtok.Type != JTokenType.Boolean)
                    {
                        throw new AvroException($"Default boolean value {jtok} is invalid, expected is json boolean.");
                    }

                    encoder.WriteBoolean((bool)jtok);
                    break;

                case Schema.Type.Int:
                    if (jtok.Type != JTokenType.Integer)
                    {
                        throw new AvroException($"Default int value {jtok} is invalid, expected is json integer.");
                    }

                    encoder.WriteInt(Convert.ToInt32((int)jtok));
                    break;

                case Schema.Type.Long:
                    if (jtok.Type != JTokenType.Integer)
                    {
                        throw new AvroException($"Default long value {jtok} is invalid, expected is json integer.");
                    }

                    encoder.WriteLong(Convert.ToInt64((long)jtok));
                    break;

                case Schema.Type.Float:
                    if (jtok.Type != JTokenType.Float)
                    {
                        throw new AvroException($"Default float value {jtok} is invalid, expected is json number.");
                    }

                    encoder.WriteFloat((float)jtok);
                    break;

                case Schema.Type.Double:
                    if (jtok.Type == JTokenType.Integer)
                    {
                        encoder.WriteDouble(Convert.ToDouble((int)jtok));
                    }
                    else if (jtok.Type == JTokenType.Float)
                    {
                        encoder.WriteDouble(Convert.ToDouble((float)jtok));
                    }
                    else
                    {
                        throw new AvroException($"Default double value {jtok} is invalid, expected is json number.");
                    }

                    break;

                case Schema.Type.Bytes:
                    if (jtok.Type != JTokenType.String)
                    {
                        throw new AvroException($"Default bytes value {jtok} is invalid, expected is json string.");
                    }

                    encoder.WriteBytes(encoding.GetBytes((string)jtok));
                    break;

                case Schema.Type.Fixed:
                    if (jtok.Type != JTokenType.String)
                    {
                        throw new AvroException($"Default fixed value {jtok} is invalid, expected is json string.");
                    }

                    int len = (schema as FixedSchema).Size;
                    byte[] jtokByteString = encoding.GetBytes((string)jtok);
                    if (jtokByteString.Length != len)
                    {
                        throw new AvroException($"Default fixed value {jtok} is not of expected length {len}");
                    }

                    encoder.WriteFixed(jtokByteString);
                    break;

                case Schema.Type.String:
                    if (jtok.Type != JTokenType.String)
                    {
                        throw new AvroException($"Default string value {jtok} is invalid, expected is json string.");
                    }

                    encoder.WriteString((string)jtok);
                    break;

                case Schema.Type.Enumeration:
                    if (jtok.Type != JTokenType.String)
                    {
                        throw new AvroException($"Default enum value {jtok} is invalid, expected is json string.");
                    }

                    encoder.WriteEnum((schema as EnumSchema).Ordinal((string)jtok));
                    break;

                case Schema.Type.Null:
                    if (jtok.Type != JTokenType.Null)
                    {
                        throw new AvroException($"Default null value {jtok} is invalid, expected is json null.");
                    }

                    encoder.WriteNull();
                    break;

                case Schema.Type.Array:
                    if (jtok.Type != JTokenType.Array)
                    {
                        throw new AvroException($"Default array value {jtok} is invalid, expected is json array.");
                    }

                    JArray jarr = jtok as JArray;
                    encoder.WriteArrayStart();
                    encoder.SetItemCount(jarr.Count);
                    foreach (JToken jitem in jarr)
                    {
                        encoder.StartItem();
                        EncodeDefaultValue(encoder, (schema as ArraySchema).ItemSchema, jitem);
                    }

                    encoder.WriteArrayEnd();
                    break;

                case Schema.Type.Record:
                case Schema.Type.Error:
                    if (jtok.Type != JTokenType.Object)
                    {
                        throw new AvroException($"Default record value {jtok} is invalid, expected is json object.");
                    }

                    RecordSchema rcs = schema as RecordSchema;
                    JObject jtokObject = jtok as JObject;
                    foreach (Field field in rcs)
                    {
                        JToken tokenValue = jtokObject[field.Name];
                        if (tokenValue == null)
                        {
                            tokenValue = field.DefaultValue;
                        }

                        if (tokenValue == null)
                        {
                            throw new AvroException($"No default value for field {field.Name}");
                        }

                        EncodeDefaultValue(encoder, field.Schema, tokenValue);
                    }
                    break;

                case Schema.Type.Map:
                    if (jtok.Type != JTokenType.Object)
                    {
                        throw new AvroException($"Default map value {jtok} is invalid, expected is json object.");
                    }

                    jtokObject = jtok as JObject;
                    encoder.WriteMapStart();
                    encoder.SetItemCount(jtokObject.Count);
                    foreach (KeyValuePair<string, JToken> jtokPair in jtokObject)
                    {
                        encoder.StartItem();
                        encoder.WriteString(jtokPair.Key);
                        EncodeDefaultValue(encoder, (schema as MapSchema).ValueSchema, jtokPair.Value);
                    }

                    encoder.WriteMapEnd();
                    break;

                case Schema.Type.Union:
                    encoder.WriteUnionIndex(0);
                    EncodeDefaultValue(encoder, (schema as UnionSchema).Schemas[0], jtok);
                    break;

                default:
                    throw new AvroException($"Unsupported schema type {schema.Tag}");
            }
        }
    }
}
