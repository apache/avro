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
using System.Globalization;
using System.Text;

namespace Avro
{
    /// <summary>
    /// Class to store schema name, namespace, enclosing namespace and documentation
    /// </summary>
    public class SchemaName
    {
        /// <summary>
        /// Validates that a simple (unqualified) name conforms to the Avro name
        /// grammar: a non-empty string whose first character is a letter or '_'
        /// and whose remaining characters are letters, digits or '_'. Letters and
        /// digits are recognized in a Unicode-aware way (including supplementary
        /// characters represented by surrogate pairs), matching the default
        /// behavior of the Java SDK (and the C# SDK's existing support for
        /// non-ASCII field names). Throws <see cref="SchemaParseException"/> when
        /// the name is invalid.
        /// </summary>
        /// <param name="name">the simple name to validate</param>
        /// <param name="what">description of the kind of name, used in error messages</param>
        internal static void ValidateName(string name, string what)
        {
            if (string.IsNullOrEmpty(name)
                || !(name[0] == '_' || char.IsLetter(name, 0)))
            {
                throw new SchemaParseException($"Invalid {what} name: {Quote(name)}");
            }

            // Iterate by Unicode scalar value so supplementary-plane letters and
            // digits (encoded as surrogate pairs) are handled correctly.
            int i = char.IsSurrogatePair(name, 0) ? 2 : 1;
            while (i < name.Length)
            {
                if (name[i] == '_' || char.IsLetterOrDigit(name, i))
                {
                    i += char.IsSurrogatePair(name, i) ? 2 : 1;
                }
                else
                {
                    throw new SchemaParseException($"Invalid {what} name: {Quote(name)}");
                }
            }
        }

        /// <summary>
        /// Returns a quoted representation of the given value with control
        /// characters escaped, so that an invalid name embedded in an error
        /// message cannot inject newlines or other control characters.
        /// </summary>
        private static string Quote(string value)
        {
            if (value == null)
            {
                return "null";
            }

            var sb = new StringBuilder(value.Length + 2);
            sb.Append('"');
            foreach (char c in value)
            {
                switch (c)
                {
                    case '"': sb.Append("\\\""); break;
                    case '\\': sb.Append("\\\\"); break;
                    case '\r': sb.Append("\\r"); break;
                    case '\n': sb.Append("\\n"); break;
                    case '\t': sb.Append("\\t"); break;
                    default:
                        if (char.IsControl(c))
                        {
                            sb.AppendFormat(CultureInfo.InvariantCulture, "\\u{0:x4}", (int)c);
                        }
                        else
                        {
                            sb.Append(c);
                        }

                        break;
                }
            }

            sb.Append('"');
            return sb.ToString();
        }

        // cache the full name, so it won't allocate new strings on each call
        private String fullName;
        
        /// <summary>
        /// Name of the schema
        /// </summary>
        public String Name { get; private set; }

        /// <summary>
        /// Namespace specified within the schema
        /// </summary>
        public String Space { get; private set; }

        /// <summary>
        /// Namespace from the most tightly enclosing schema
        /// </summary>
        public String EncSpace { get; private set; }

        /// <summary>
        /// Documentation for the schema
        /// </summary>
        public String Documentation { get; private set; }

        /// <summary>
        /// Namespace.Name of the schema
        /// </summary>
        public String Fullname { get { return fullName; } }

        /// <summary>
        /// Namespace of the schema
        /// </summary>
        public String Namespace { get { return string.IsNullOrEmpty(this.Space) ? this.EncSpace : this.Space; } }

        /// <summary>
        /// Constructor for SchemaName
        /// </summary>
        /// <param name="name">name of the schema</param>
        /// <param name="space">namespace of the schema</param>
        /// <param name="encspace">enclosing namespace of the schema</param>
        /// <param name="documentation">documentation of the schema</param>
        public SchemaName(string name, string space, string encspace, string documentation)
        {
            if (name == null)
            {                         // anonymous
                Name = Space = null;
                EncSpace = encspace;   // need to save enclosing namespace for anonymous types, so named types within the anonymous type can be resolved
            }
            else if (!name.Contains("."))
            {                          // unqualified name
                Space = space;    // use default space
                Name = name;
                EncSpace = encspace;
            }
            else
            {
                string[] parts = name.Split('.');
                Space = string.Join(".", parts, 0, parts.Length - 1);
                Name = parts[parts.Length - 1];
                EncSpace = encspace;
            }

            Documentation = documentation;
            fullName = string.IsNullOrEmpty(Namespace) ? Name : Namespace + "." + Name;

            // Validate the simple name only. Namespaces are intentionally not
            // validated here: the code generator's namespace-mapping feature
            // rewrites schema namespaces to C#-specific values (for example
            // "@return" for reserved words) and re-parses the schema, so a
            // namespace component may legitimately not match the Avro name grammar.
            if (Name != null)
            {
                ValidateName(Name, "schema");
            }
        }

        /// <summary>
        /// Returns the full name of the schema
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return Fullname;
        }

        /// <summary>
        /// Writes the schema name in JSON format
        /// </summary>
        /// <param name="writer">JSON writer</param>
        /// <param name="names">list of named schemas already written</param>
        /// <param name="encspace">enclosing namespace of the schema</param>
        internal void WriteJson(Newtonsoft.Json.JsonTextWriter writer, SchemaNames names, string encspace)
        {
            if (null != this.Name)  // write only if not anonymous
            {
                JsonHelper.writeIfNotNullOrEmpty(writer, "name", this.Name);
                JsonHelper.writeIfNotNull(writer, "doc", this.Documentation);
                if (!String.IsNullOrEmpty(this.Space))
                    JsonHelper.writeIfNotNullOrEmpty(writer, "namespace", this.Space);
                else if (!String.IsNullOrEmpty(this.EncSpace)) // need to put enclosing name space for code generated classes
                    JsonHelper.writeIfNotNullOrEmpty(writer, "namespace", this.EncSpace);
            }
        }

        /// <summary>
        /// Compares two schema names
        /// </summary>
        /// <param name="obj">SchameName object to compare against this object</param>
        /// <returns>true or false</returns>
        public override bool Equals(Object obj)
        {
            if (obj == this) return true;
            if (obj != null && obj is SchemaName)
            {
                SchemaName that = (SchemaName)obj;
                return areEqual(that.Name, Name) && areEqual(that.Namespace, Namespace);
            }
            return false;
        }

        /// <summary>
        /// Compares two objects
        /// </summary>
        /// <param name="obj1">first object</param>
        /// <param name="obj2">second object</param>
        /// <returns>true or false</returns>
        private static bool areEqual(object obj1, object obj2)
        {
            return obj1 == null ? obj2 == null : obj1.Equals(obj2);
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return string.IsNullOrEmpty(Fullname) ? 0 : 29 * Fullname.GetHashCode();
        }
    }

    /// <summary>
    /// A class that contains a list of named schemas. This is used when reading or writing a schema/protocol.
    /// This prevents reading and writing of duplicate schema definitions within a protocol or schema file
    /// </summary>
    public class SchemaNames
    {
        /// <summary>
        /// Map of schema name and named schema objects
        /// </summary>
        public IDictionary<SchemaName, NamedSchema> Names { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public SchemaNames()
        {
            Names = new Dictionary<SchemaName, NamedSchema>();
        }

        /// <summary>
        /// Checks if given name is in the map
        /// </summary>
        /// <param name="name">schema name</param>
        /// <returns>true or false</returns>
        public bool Contains(SchemaName name)
        {
            if (Names.ContainsKey(name))
                return true;
            return false;
        }

        /// <summary>
        /// Adds a schema name to the map if it doesn't exist yet
        /// </summary>
        /// <param name="name">schema name</param>
        /// <param name="schema">schema object</param>
        /// <returns>true if schema was added to the list, false if schema is already in the list</returns>
        public bool Add(SchemaName name, NamedSchema schema)
        {
            if (Names.ContainsKey(name))
                return false;

            Names.Add(name, schema);
            return true;
        }

        /// <summary>
        /// Adds a named schema to the list
        /// </summary>
        /// <param name="schema">schema object</param>
        /// <returns>true if schema was added to the list, false if schema is already in the list</returns>
        public bool Add(NamedSchema schema)
        {
            SchemaName name = schema.SchemaName;
            return Add(name, schema);
        }

        /// <summary>
        /// Tries to get the value for the given name fields
        /// </summary>
        /// <param name="name">name of the schema</param>
        /// <param name="space">namespace of the schema</param>
        /// <param name="encspace">enclosing namespace of the schema</param>
        /// <param name="documentation">documentation for the schema</param>
        /// <param name="schema">schema object found</param>
        /// <returns>true if name is found in the map, false otherwise</returns>
        public bool TryGetValue(string name, string space, string encspace, string documentation, out NamedSchema schema)
        {
            SchemaName schemaname = new SchemaName(name, space, encspace, documentation);
            return Names.TryGetValue(schemaname, out schema);
        }

        /// <summary>
        /// Returns the enumerator for the map
        /// </summary>
        /// <returns></returns>
        public IEnumerator<KeyValuePair<SchemaName, NamedSchema>> GetEnumerator()
        {
            return Names.GetEnumerator();
        }
    }
}
