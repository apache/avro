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
using System.Linq;
using System.Text.RegularExpressions;
using Newtonsoft.Json.Linq;

namespace Avro
{
    /// <summary>
    /// Class for enum type schemas
    /// </summary>
    public class EnumSchema : NamedSchema
    {
        /// <summary>
        /// List of strings representing the enum symbols
        /// </summary>
        public IList<string> Symbols { get; private set; }

        /// <summary>
        /// Map of maps of alternate names for the enum symbols
        /// </summary>
        public IDictionary<string, IDictionary<string, string>> AlternateSymbols { get; private set; }

        /// <summary>
        /// The default token to use when deserializing an enum when the provided token is not found
        /// </summary>
        public string Default { get; private set; }

        /// <summary>
        /// Map of enum symbols and it's corresponding ordinal number
        /// </summary>
        private readonly IDictionary<string, int> symbolMap;

        /// <summary>
        /// Count of enum symbols
        /// </summary>
        public int Count { get { return Symbols.Count; } }

        /// <summary>
        /// Initializes a new instance of the <see cref="EnumSchema"/> class.
        /// </summary>
        /// <param name="name">Name of enum</param>
        /// <param name="space">Namespace of enum</param>
        /// <param name="aliases">List of aliases for the name</param>
        /// <param name="alternateNames">Dictionary of alternate names for this schema name</param>
        /// <param name="symbols">List of enum symbols</param>
        /// <param name="customProperties">Custom properties on this schema</param>
        /// <param name="doc">Documentation for this named schema</param>
        /// <param name="defaultSymbol"></param>
        /// <param name="altsymbols">Map of alternate names for the enum symbols</param>
        public static EnumSchema Create(string name,
            IEnumerable<string> symbols,
            string space = null,
            IEnumerable<string> aliases = null,
            IDictionary<string, string> alternateNames = null,
            PropertyMap customProperties = null,
            string doc = null,
            string defaultSymbol = null,
            IDictionary<string, IDictionary<string, string>> altsymbols = null)
        {
            return new EnumSchema(new SchemaName(name, space, null, doc, alternateNames),
                  Aliases.GetSchemaNames(aliases, name, space),
                  symbols.ToList(),
                  CreateSymbolsMap(symbols),
                  altsymbols,
                  customProperties,
                  new SchemaNames(),
                  doc,
                  defaultSymbol);
        }

        /// <summary>
        /// Static function to return new instance of EnumSchema
        /// </summary>
        /// <param name="jtok">JSON object for enum schema</param>
        /// <param name="props">dictionary that provides access to custom properties</param>
        /// <param name="names">list of named schema already parsed in</param>
        /// <param name="encspace">enclosing namespace for the enum schema</param>
        /// <returns>new instance of enum schema</returns>
        internal static EnumSchema NewInstance(JToken jtok, PropertyMap props, SchemaNames names, string encspace)
        {
            SchemaName name = NamedSchema.GetName(jtok, encspace);
            var aliases = NamedSchema.GetAliases(jtok, name.Space, name.EncSpace);
            
            JArray jsymbols = jtok["symbols"] as JArray;
            if (null == jsymbols)
                throw new SchemaParseException($"Enum has no symbols: {name} at '{jtok.Path}'");
            
            var altsymbols = GetAlternateSymbols(jtok);

            List<string> symbols = new List<string>();
            IDictionary<string, int> symbolMap = new Dictionary<string, int>();
            int i = 0;
            foreach (JValue jsymbol in jsymbols)
            {
                string s = (string)jsymbol.Value;
                if (symbolMap.ContainsKey(s))
                    throw new SchemaParseException($"Duplicate symbol: {s} at '{jtok.Path}'");

                symbolMap[s] = i++;
                symbols.Add(s);
            }
            try
            {
                return new EnumSchema(name, aliases, symbols, symbolMap, altsymbols, props, names,
                    JsonHelper.GetOptionalString(jtok, "doc"), JsonHelper.GetOptionalString(jtok, "default"));
            }
            catch (AvroException e)
            {
                throw new SchemaParseException($"{e.Message} at '{jtok.Path}'", e);
            }
        }

        /// <summary>
        /// Constructor for enum schema
        /// </summary>
        /// <param name="name">name of enum</param>
        /// <param name="aliases">list of aliases for the name</param>
        /// <param name="symbols">list of enum symbols</param>
        /// <param name="symbolMap">map of enum symbols and value</param>
        /// <param name="altsymbols">map of alternate names for the enum symbols</param>
        /// <param name="props">custom properties on this schema</param>
        /// <param name="names">list of named schema already read</param>
        /// <param name="doc">documentation for this named schema</param>
        /// <param name="defaultSymbol">default symbol</param>
        private EnumSchema(SchemaName name, IList<SchemaName> aliases, List<string> symbols,
                            IDictionary<String, int> symbolMap, 
                            IDictionary<string, IDictionary<string, string>> altsymbols, 
                            PropertyMap props, SchemaNames names,
                            string doc, string defaultSymbol)
                            : base(Type.Enumeration, name, aliases, props, names, doc)
        {
            if (null == name.Name) throw new AvroException("name cannot be null for enum schema.");
            this.Symbols = symbols;
            this.symbolMap = symbolMap;
            this.AlternateSymbols = altsymbols;

            if (null != defaultSymbol && !symbolMap.ContainsKey(defaultSymbol))
                throw new AvroException($"Default symbol: {defaultSymbol} not found in symbols");
            Default = defaultSymbol;
        }

        /// <summary>
        /// Creates symbols map from specified list of symbols.
        /// Symbol map contains the names of the symbols and their index.
        /// </summary>
        /// <param name="symbols">List of symbols</param>
        /// <returns>Symbol map</returns>
        /// <exception cref="AvroException">Is thrown if the symbols list contains invalid symbol name or duplicate symbols</exception>
        private static IDictionary<string, int> CreateSymbolsMap(IEnumerable<string> symbols)
        {
            IDictionary<string, int> symbolMap = new Dictionary<string, int>();
            int i = 0;
            foreach (var symbol in symbols)
            {
                ValidateSymbolName(symbol);

                if (symbolMap.ContainsKey(symbol))
                {
                    throw new AvroException($"Duplicate symbol: {symbol}");
                }

                symbolMap[symbol] = i++;
            }

            return symbolMap;
        }

        private static void ValidateSymbolName(string symbol)
        {
            if(string.IsNullOrEmpty(symbol) || !Regex.IsMatch(symbol, "^([A-Za-z_][A-Za-z0-9_]*)$"))
            {
                throw new AvroException($"Invalid symbol name: {symbol}");
            }
        }


        /// <summary>
        /// Parses the 'altnames' property from the given JSON token
        /// </summary>
        /// <param name="jtok">JSON object to read</param>
        /// <returns>Dictionary of alternate names. If no 'altnames' specified, then it returns null.</returns>
        protected static IDictionary<string, IDictionary<string, string>> GetAlternateSymbols(JToken jtok)
        {
            JToken jaltsymbols = jtok["altsymbols"];
            if (null == jaltsymbols)
                return null;

            if (jaltsymbols.Type != JTokenType.Object)
                throw new SchemaParseException($"Aliases must be of format JSON object at '{jtok.Path}'");

            var altsymbols = new Dictionary<string, IDictionary<string, string>>();
            foreach (JProperty jmap in jaltsymbols.Children())
            {
                if (jmap.Value.Type != JTokenType.Object)
                    throw new SchemaParseException($"Aliases must be of format JSON object at '{jtok.Path}'");
                Dictionary<string, string> altmap = new Dictionary<string, string>();
                foreach (JProperty jalt in jmap.Value.Children())
                {
                    if (jalt.Value.Type != JTokenType.String)
                        throw new SchemaParseException($"Alternate symbol must be of type string at '{jtok.Path}'");

                    altmap.Add(jmap.Name, (string)jalt.Value);
                }
                altsymbols.Add(jmap.Name, altmap);
            }
            return altsymbols;
        }

        /// <summary>
        /// Writes enum schema in JSON format
        /// </summary>
        /// <param name="writer">JSON writer</param>
        /// <param name="names">list of named schema already written</param>
        /// <param name="encspace">enclosing namespace of the enum schema</param>
        protected internal override void WriteJsonFields(Newtonsoft.Json.JsonTextWriter writer,
                                                            SchemaNames names, string encspace)
        {
            base.WriteJsonFields(writer, names, encspace);
            writer.WritePropertyName("symbols");
            writer.WriteStartArray();
            foreach (string s in this.Symbols)
                writer.WriteValue(s);
            writer.WriteEndArray();
            if (null != AlternateSymbols)
            {
                writer.WritePropertyName("altsymbols");
                writer.WriteStartObject();
                foreach (var entry in AlternateSymbols)
                {
                    writer.WritePropertyName(entry.Key);
                    writer.WriteStartObject();
                    foreach (var alt in entry.Value)
                    {
                        writer.WritePropertyName(alt.Key);
                        writer.WriteValue(alt.Value);
                    }
                    writer.WriteEndObject();
                }
                writer.WriteEndObject();
            }
            if (null != Default)
            {
                writer.WritePropertyName("default");
                writer.WriteValue(Default);
            }
        }

        /// <summary>
        /// Returns the position of the given symbol within this enum.
        /// Throws AvroException if the symbol is not found in this enum.
        /// </summary>
        /// <param name="symbol">name of the symbol to find</param>
        /// <returns>
        /// position of the given symbol in this enum schema
        /// </returns>
        /// <exception cref="Avro.AvroException">No such symbol: {symbol}</exception>
        public int Ordinal(string symbol)
        {
            int result;
            if (symbolMap.TryGetValue(symbol, out result))
            {
                return result;
            }

            if (Default != null && symbolMap.TryGetValue(Default, out result))
            {
                return result;
            }

            throw new AvroException($"No such symbol: {symbol}");
        }

        /// <summary>
        /// Returns the enum symbol of the given index to the list
        /// </summary>
        /// <param name="index">symbol index</param>
        /// <returns>symbol name</returns>
        public string this[int index]
        {
            get
            {
                if (index < Symbols.Count) return Symbols[index];
                throw new AvroException("Enumeration out of range. Must be less than " + Symbols.Count + ", but is " + index);
            }
        }

        /// <summary>
        /// Checks if given symbol is in the list of enum symbols
        /// </summary>
        /// <param name="symbol">symbol to check</param>
        /// <returns>true if symbol exist, false otherwise</returns>
        public bool Contains(string symbol)
        {
            return symbolMap.ContainsKey(symbol);
        }

        /// <summary>
        /// Returns an enumerator that enumerates the symbols in this enum schema in the order of their definition.
        /// </summary>
        /// <returns>Enumeration over the symbols of this enum schema</returns>
        public IEnumerator<string> GetEnumerator()
        {
            return Symbols.GetEnumerator();
        }

        /// <summary>
        /// Checks equality of two enum schema
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (obj == this) return true;
            if (obj != null && obj is EnumSchema)
            {
                EnumSchema that = obj as EnumSchema;
                if (SchemaName.Equals(that.SchemaName) && Count == that.Count)
                {
                    for (int i = 0; i < Count; i++)
                    {
                        if (!Symbols[i].Equals(that.Symbols[i], StringComparison.Ordinal))
                        {
                            return false;
                        }
                    }

                    return areEqual(that.Props, this.Props);
                }
            }
            return false;
        }

        /// <summary>
        /// Hashcode function
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            int result = SchemaName.GetHashCode() + getHashCode(Props);
            foreach (string s in Symbols)
            {
                result += 23 * s.GetHashCode();
            }

            return result;
        }

        /// <summary>
        /// Checks if this schema can read data written by the given schema. Used for decoding data.
        /// </summary>
        /// <param name="writerSchema">writer schema</param>
        /// <returns>true if this and writer schema are compatible based on the AVRO specification, false otherwise</returns>
        public override bool CanRead(Schema writerSchema)
        {
            if (writerSchema.Tag != Tag) return false;

            EnumSchema that = writerSchema as EnumSchema;
            if (!that.SchemaName.Equals(SchemaName))
                if (!InAliases(that.SchemaName)) return false;

            // we defer checking of symbols. Writer may have a symbol missing from the reader,
            // but if writer never used the missing symbol, then reader should still be able to read the data

            return true;
        }
    }
}
