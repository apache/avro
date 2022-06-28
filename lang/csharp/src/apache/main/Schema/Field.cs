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
using Newtonsoft.Json;

namespace Avro
{

    /// <summary>
    /// Class for fields defined in a record
    /// </summary>
    public class Field
    {
        /// <summary>
        /// Enum for the sorting order of record fields
        /// </summary>
        public enum SortOrder
        {
            /// <summary>
            /// Ascending order.
            /// </summary>
            ascending,

            /// <summary>
            /// Descending order.
            /// </summary>
            descending,

            /// <summary>
            /// Ignore sort order.
            /// </summary>
            ignore
        }

        /// <summary>
        /// Name of the field.
        /// </summary>
        public readonly string Name;

        /// <summary>
        /// List of aliases for the field name.
        /// </summary>
        public IList<string> Aliases { get; private set; }

        /// <summary>
        /// Position of the field within its record.
        /// </summary>
        public int Pos { get; private set; }

        /// <summary>
        /// Documentation for the field, if any. Null if there is no documentation.
        /// </summary>
        public string Documentation { get; private set; }

        /// <summary>
        /// The default value for the field stored as JSON object, if defined. Otherwise, null.
        /// </summary>
        public JToken DefaultValue { get; private set; }

        /// <summary>
        /// Order of the field
        /// </summary>
        public SortOrder? Ordering { get; private set; }

        /// <summary>
        /// Field type's schema
        /// </summary>
        public Schema Schema { get; private set; }

        /// <summary>
        /// Custom properties for the field. We don't store the fields custom properties in
        /// the field type's schema because if the field type is only a reference to the schema
        /// instead of an actual schema definition, then the schema could already have it's own set
        /// of custom properties when it was previously defined.
        /// </summary>
        private readonly PropertyMap Props;

        /// <summary>
        /// Static comparer object for JSON objects such as the fields default value
        /// </summary>
        internal readonly static JTokenEqualityComparer JtokenEqual = new JTokenEqualityComparer();

        /// <summary>
        /// Initializes a new instance of the <see cref="Field"/> class.
        /// </summary>
        /// <param name="schema">schema for the field type.</param>
        /// <param name="name">name of the field.</param>
        /// <param name="aliases">list of aliases for the name of the field.</param>
        /// <param name="pos">position of the field.</param>
        /// <param name="doc">documentation for the field.</param>
        /// <param name="defaultValue">field's default value if it exists.</param>
        /// <param name="sortorder">sort order of the field.</param>
        /// <param name="customProperties">dictionary that provides access to custom properties.</param>
        public Field(Schema schema,
            string name,
            int pos,
            IList<string> aliases = null,
            string doc = null,
            JToken defaultValue = null,
            SortOrder sortorder = SortOrder.ignore,
            PropertyMap customProperties = null)
            : this(schema, name, aliases, pos, doc, defaultValue, sortorder, customProperties)
        {
        }

        /// <summary>
        /// Creates a new field based on the specified field, with a different position.
        /// </summary>
        /// <returns>A clone of this field with new position.</returns>
        internal Field ChangePosition(int newPosition)
        {
            return new Field(Schema, Name, newPosition, Aliases, Documentation, DefaultValue, Ordering ?? SortOrder.ignore, Props);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Field"/> class.
        /// </summary>
        /// <param name="schema">schema for the field type</param>
        /// <param name="name">name of the field</param>
        /// <param name="aliases">list of aliases for the name of the field</param>
        /// <param name="pos">position of the field</param>
        /// <param name="doc">documentation for the field</param>
        /// <param name="defaultValue">field's default value if it exists</param>
        /// <param name="sortorder">sort order of the field</param>
        /// <param name="props">dictionary that provides access to custom properties</param>
        /// <exception cref="ArgumentNullException">
        /// name - name cannot be null.
        /// or
        /// type - type cannot be null.
        /// </exception>
        internal Field(Schema schema, string name, IList<string> aliases, int pos, string doc,
                        JToken defaultValue, SortOrder sortorder, PropertyMap props)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException(nameof(name), "name cannot be null.");
            }

            Schema = schema ?? throw new ArgumentNullException("type", "type cannot be null.");
            Name = name;
            Aliases = aliases;
            Pos = pos;
            Documentation = doc;
            DefaultValue = defaultValue;
            Ordering = sortorder;
            Props = props;
        }

        /// <summary>
        /// Writes the Field class in JSON format
        /// </summary>
        /// <param name="writer">JSON writer</param>
        /// <param name="names">list of named schemas already written</param>
        /// <param name="encspace">enclosing namespace for the field</param>
        protected internal void writeJson(JsonTextWriter writer, SchemaNames names, string encspace)
        {
            writer.WriteStartObject();
            JsonHelper.writeIfNotNullOrEmpty(writer, "name", this.Name);
            JsonHelper.writeIfNotNullOrEmpty(writer, "doc", this.Documentation);

            if (null != this.DefaultValue)
            {
                writer.WritePropertyName("default");
                this.DefaultValue.WriteTo(writer, null);
            }
            if (null != this.Schema)
            {
                writer.WritePropertyName("type");
                Schema.WriteJson(writer, names, encspace);
            }

            if (null != this.Props)
                this.Props.WriteJson(writer);

            if (null != Aliases)
            {
                writer.WritePropertyName("aliases");
                writer.WriteStartArray();
                foreach (string name in Aliases)
                {
                    writer.WriteValue(name);
                }

                writer.WriteEndArray();
            }

            writer.WriteEndObject();
        }

        /// <summary>
        /// Parses the 'aliases' property from the given JSON token
        /// </summary>
        /// <param name="jtok">JSON object to read</param>
        /// <returns>List of string that represents the list of alias. If no 'aliases' specified, then it returns null.</returns>
        internal static IList<string> GetAliases(JToken jtok)
        {
            JToken jaliases = jtok["aliases"];
            if (null == jaliases)
                return null;

            if (jaliases.Type != JTokenType.Array)
                throw new SchemaParseException($"Aliases must be of format JSON array of strings at '{jtok.Path}'");

            var aliases = new List<string>();
            foreach (JToken jalias in jaliases)
            {
                if (jalias.Type != JTokenType.String)
                    throw new SchemaParseException($"Aliases must be of format JSON array of strings at '{jtok.Path}'");

                aliases.Add((string)jalias);
            }
            return aliases;
        }

        /// <summary>
        /// Returns the field's custom property value given the property name
        /// </summary>
        /// <param name="key">custom property name</param>
        /// <returns>custom property value</returns>
        public string GetProperty(string key)
        {
            if (null == this.Props) return null;
            string v;
            return this.Props.TryGetValue(key, out v) ? v : null;
        }

        /// <summary>
        /// Compares two field objects
        /// </summary>
        /// <param name="obj">field to compare with this field</param>
        /// <returns>true if two fields are equal, false otherwise</returns>
        public override bool Equals(object obj)
        {
            if (obj == this) return true;
            if (obj != null && obj is Field)
            {
                Field that = obj as Field;
                return areEqual(that.Name, Name) && that.Pos == Pos && areEqual(that.Documentation, Documentation)
                    && areEqual(that.Ordering, Ordering) && JtokenEqual.Equals(that.DefaultValue, DefaultValue)
                    && that.Schema.Equals(Schema) && areEqual(that.Props, this.Props);
            }
            return false;
        }

        /// <summary>
        /// Compares two objects
        /// </summary>
        /// <param name="o1">first object</param>
        /// <param name="o2">second object</param>
        /// <returns>true if two objects are equal, false otherwise</returns>
        private static bool areEqual(object o1, object o2)
        {
            return o1 == null ? o2 == null : o1.Equals(o2);
        }

        /// <summary>
        /// Hash code function
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
#pragma warning disable CA1307 // Specify StringComparison
            return 17 * Name.GetHashCode() + Pos + 19 * getHashCode(Documentation) +
#pragma warning restore CA1307 // Specify StringComparison
                   23 * getHashCode(Ordering) + 29 * getHashCode(DefaultValue) + 31 * Schema.GetHashCode() +
                   37 * getHashCode(Props);
        }

        /// <summary>
        /// Hash code helper function
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        private static int getHashCode(object obj)
        {
            return obj == null ? 0 : obj.GetHashCode();
        }
    }
}
