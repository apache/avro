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
using System.Text;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace Avro
{
    /// <summary>
    /// Base class for all schema types
    /// </summary>
    public abstract class Schema
    {
        /// <summary>
        /// Enum for schema types
        /// </summary>
        public enum Type
        {
            Null,
            Boolean,
            Int,
            Long,
            Float,
            Double,
            Bytes,
            String,
            Record,
            Enumeration,
            Array,
            Map,
            Union,
            Fixed,
            Error
        }

        /// <summary>
        /// Schema type property
        /// </summary>
        public Type Tag { get; private set; }

        /// <summary>
        /// Additional JSON attributes apart from those defined in the AVRO spec
        /// </summary>
        internal PropertyMap Props { get; private set; }

        /// <summary>
        /// Constructor for schema class
        /// </summary>
        /// <param name="type"></param>
        protected Schema(Type type, PropertyMap props)
        {
            this.Tag = type;
            this.Props = props;
        }

        /// <summary>
        /// The name of this schema. If this is a named schema such as an enum, it returns the fully qualified
        /// name for the schema. For other schemas, it returns the type of the schema.
        /// </summary>
        public abstract string Name { get; }

        /// <summary>
        /// Static class to return new instance of schema object
        /// </summary>
        /// <param name="jtok">JSON object</param>
        /// <param name="names">list of named schemas already read</param>
        /// <param name="encspace">enclosing namespace of the schema</param>
        /// <returns>new Schema object</returns>
        internal static Schema ParseJson(JToken jtok, SchemaNames names, string encspace)
        {
            if (null == jtok) throw new ArgumentNullException("j", "j cannot be null.");

            if (jtok.Type == JTokenType.String) // primitive schema with no 'type' property or primitive or named type of a record field
            {
                string value = (string)jtok;

                PrimitiveSchema ps = PrimitiveSchema.NewInstance(value);
                if (null != ps) return ps;

                NamedSchema schema = null;
                if (names.TryGetValue(value, null, encspace, out schema)) return schema;

                throw new SchemaParseException("Undefined name: " + value);
            }

            if (jtok is JArray) // union schema with no 'type' property or union type for a record field
                return UnionSchema.NewInstance(jtok as JArray, null, names, encspace);

            if (jtok is JObject) // JSON object with open/close parenthesis, it must have a 'type' property
            {
                JObject jo = jtok as JObject;

                JToken jtype = jo["type"];
                if (null == jtype)
                    throw new SchemaParseException("Property type is required");

                var props = Schema.GetProperties(jtok);

                if (jtype.Type == JTokenType.String)
                {
                    string type = (string)jtype;

                    if (type.Equals("array"))
                        return ArraySchema.NewInstance(jtok, props, names, encspace);
                    if (type.Equals("map"))
                        return MapSchema.NewInstance(jtok, props, names, encspace);

                    Schema schema = PrimitiveSchema.NewInstance((string)type, props);
                    if (null != schema) return schema;

                    return NamedSchema.NewInstance(jo, props, names, encspace);
                }
                else if (jtype.Type == JTokenType.Array)
                    return UnionSchema.NewInstance(jtype as JArray, props, names, encspace);
            }
            throw new AvroTypeException("Invalid JSON for schema: " + jtok);
        }

        /// <summary>
        /// Parses a given JSON string to create a new schema object
        /// </summary>
        /// <param name="json">JSON string</param>
        /// <returns>new Schema object</returns>
        public static Schema Parse(string json)
        {
            if (string.IsNullOrEmpty(json)) throw new ArgumentNullException("json", "json cannot be null.");
            return Parse(json.Trim(), new SchemaNames(), null); // standalone schema, so no enclosing namespace
        }

        /// <summary>
        /// Parses a JSON string to create a new schema object
        /// </summary>
        /// <param name="json">JSON string</param>
        /// <param name="names">list of named schemas already read</param>
        /// <param name="encspace">enclosing namespace of the schema</param>
        /// <returns>new Schema object</returns>
        internal static Schema Parse(string json, SchemaNames names, string encspace)
        {
            Schema sc = PrimitiveSchema.NewInstance(json);
            if (null != sc) return sc;

            try
            {
                bool IsArray = json.StartsWith("[") && json.EndsWith("]");
                JContainer j = IsArray ? (JContainer)JArray.Parse(json) : (JContainer)JObject.Parse(json);

                return ParseJson(j, names, encspace);
            }
            catch (Newtonsoft.Json.JsonSerializationException ex)
            {
                throw new SchemaParseException("Could not parse. " + ex.Message + Environment.NewLine + json);
            }
        }

        /// <summary>
        /// Static function to parse custom properties (not defined in the Avro spec) from the given JSON object
        /// </summary>
        /// <param name="jtok">JSON object to parse</param>
        /// <returns>Property map if custom properties were found, null if no custom properties found</returns>
        internal static PropertyMap GetProperties(JToken jtok)
        {
            var props = new PropertyMap();
            props.Parse(jtok);
            if (props.Count > 0)
                return props;
            else
                return null;
        }

        /// <summary>
        /// Returns the canonical JSON representation of this schema.
        /// </summary>
        /// <returns>The canonical JSON representation of this schema.</returns>
        public override string ToString()
        {
            System.IO.StringWriter sw = new System.IO.StringWriter();
            Newtonsoft.Json.JsonTextWriter writer = new Newtonsoft.Json.JsonTextWriter(sw);

            if (this is PrimitiveSchema || this is UnionSchema)
            {
                writer.WriteStartObject();
                writer.WritePropertyName("type");
            }

            WriteJson(writer, new SchemaNames(), null); // stand alone schema, so no enclosing name space

            if (this is PrimitiveSchema || this is UnionSchema)
                writer.WriteEndObject();

            return sw.ToString();
        }

        /// <summary>
        /// Writes opening { and 'type' property
        /// </summary>
        /// <param name="writer">JSON writer</param>
        private void writeStartObject(JsonTextWriter writer)
        {
            writer.WriteStartObject();
            writer.WritePropertyName("type");
            writer.WriteValue(GetTypeString(this.Tag));
        }

        /// <summary>
        /// Returns symbol name for the given schema type
        /// </summary>
        /// <param name="type">schema type</param>
        /// <returns>symbol name</returns>
        public static string GetTypeString(Type type)
        {
            if (type != Type.Enumeration) return type.ToString().ToLower();
            return "enum";
        }

        /// <summary>
        /// Default implementation for writing schema properties in JSON format
        /// </summary>
        /// <param name="writer">JSON writer</param>
        /// <param name="names">list of named schemas already written</param>
        /// <param name="encspace">enclosing namespace of the schema</param>
        protected internal virtual void WriteJsonFields(JsonTextWriter writer, SchemaNames names, string encspace)
        {
        }

        /// <summary>
        /// Writes schema object in JSON format
        /// </summary>
        /// <param name="writer">JSON writer</param>
        /// <param name="names">list of named schemas already written</param>
        /// <param name="encspace">enclosing namespace of the schema</param>
        protected internal virtual void WriteJson(JsonTextWriter writer, SchemaNames names, string encspace)
        {
            writeStartObject(writer);
            WriteJsonFields(writer, names, encspace);
            if (null != this.Props) Props.WriteJson(writer);
            writer.WriteEndObject();
        }

        /// <summary>
        /// Returns the schema's custom property value given the property name
        /// </summary>
        /// <param name="key">custom property name</param>
        /// <returns>custom property value</returns>
        public string GetProperty(string key)
        {
            if (null == this.Props) return null;
            string v;
            return (this.Props.TryGetValue(key, out v)) ? v : null;
        }

        /// <summary>
        /// Hash code function
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return Tag.GetHashCode() + getHashCode(Props);
        }

        /// <summary>
        /// Returns true if and only if data written using writerSchema can be read using the current schema
        /// according to the Avro resolution rules.
        /// </summary>
        /// <param name="writerSchema">The writer's schema to match against.</param>
        /// <returns>True if and only if the current schema matches the writer's.</returns>
        public virtual bool CanRead(Schema writerSchema) { return Tag == writerSchema.Tag; }

        /// <summary>
        /// Compares two objects, null is equal to null
        /// </summary>
        /// <param name="o1">first object</param>
        /// <param name="o2">second object</param>
        /// <returns>true if two objects are equal, false otherwise</returns>
        protected static bool areEqual(object o1, object o2)
        {
            return o1 == null ? o2 == null : o1.Equals(o2);
        }

        /// <summary>
        /// Hash code helper function
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        protected static int getHashCode(object obj)
        {
            return obj == null ? 0 : obj.GetHashCode();
        }
    }
}
