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
using System.Text.RegularExpressions;

namespace Avro
{
    /// <summary>
    /// Generates C# code from Avro schemas and protocols.
    /// </summary>
    public static class AvroGen
    {
        /// <summary>
        /// Generate files for protocol.
        /// </summary>
        /// <param name="input">input protocol definition.</param>
        /// <param name="outputDir">output directory for generated files.</param>
        /// <param name="namespaceMapping">namespace mappings object.</param>
        public static void GenerateProtocol(string input, string outputDir, IEnumerable<KeyValuePair<string, string>> namespaceMapping = null)
        {
            if (namespaceMapping != null)
                input = ReplaceMappedNamespacesInSchema(input, namespaceMapping);

            Protocol protocol = Protocol.Parse(input);

            CodeGen codegen = new CodeGen();
            codegen.AddProtocol(protocol);

            codegen.GenerateCode();
            codegen.WriteTypes(outputDir);
        }

        /// <summary>
        /// Generate files for protocol.
        /// </summary>
        /// <param name="inputFile">input protocol definition file.</param>
        /// <param name="outputDir">output directory for generated files.</param>
        /// <param name="namespaceMapping">namespace mappings object.</param>
        public static void GenerateProtocolFromFile(string inputFile, string outputDir, IEnumerable<KeyValuePair<string, string>> namespaceMapping = null)
        {
            string text = System.IO.File.ReadAllText(inputFile);
            GenerateProtocol(text, outputDir, namespaceMapping);
        }

        /// <summary>
        /// Generate files for schema.
        /// </summary>
        /// <param name="input">input schema definition.</param>
        /// <param name="outputDir">output directory for generated files.</param>
        /// <param name="namespaceMapping">namespace mappings object.</param>
        public static void GenerateSchema(string input, string outputDir, IEnumerable<KeyValuePair<string, string>> namespaceMapping = null)
        {
            if (namespaceMapping != null)
                input = ReplaceMappedNamespacesInSchema(input, namespaceMapping);

            Schema schema = Schema.Parse(input);

            CodeGen codegen = new CodeGen();
            codegen.AddSchema(schema);

            codegen.GenerateCode();
            codegen.WriteTypes(outputDir);
        }

        /// <summary>
        /// Generate files for schema.
        /// </summary>
        /// <param name="inputFile">input schema definition file.</param>
        /// <param name="outputDir">output directory for generated files.</param>
        /// <param name="namespaceMapping">namespace mappings object.</param>
        public static void GenerateSchemaFromFile(string inputFile, string outputDir, IEnumerable<KeyValuePair<string, string>> namespaceMapping = null)
        {
            string text = System.IO.File.ReadAllText(inputFile);
            GenerateSchema(text, outputDir, namespaceMapping);
        }

        /// <summary>
        /// Replace namespace(s) in schema or protocol definition.
        /// </summary>
        /// <param name="input">input schema or protocol definition.</param>
        /// <param name="namespaceMapping">namespace mappings object.</param>
        public static string ReplaceMappedNamespacesInSchema(string input, IEnumerable<KeyValuePair<string, string>> namespaceMapping)
        {
            if (namespaceMapping == null || input == null)
                return input;

            // Replace namespace in "namespace" definitions: 
            //    "namespace": "originalnamespace" -> "namespace": "mappednamespace"
            //    "namespace": "originalnamespace.whatever" -> "namespace": "mappednamespace.whatever"
            // Note: It keeps the original whitespaces
            return Regex.Replace(input, @"""namespace""(\s*):(\s*)""([^""]*)""", m =>
            {
                // m.Groups[1]: whitespaces before ':'
                // m.Groups[2]: whitespaces after ':'
                // m.Groups[3]: the namespace

                string ns = m.Groups[3].Value;

                foreach (var mapping in namespaceMapping)
                {
                    // Full match
                    if (mapping.Key == ns)
                    {
                        ns = mapping.Value;
                        break;
                    }
                    else
                    // Partial match
                    if (ns.StartsWith($"{mapping.Key}."))
                    {
                        ns = $"{mapping.Value}.{ns.Substring(mapping.Key.Length + 1)}";
                        break;
                    }
                }
                return $@"""namespace""{m.Groups[1].Value}:{m.Groups[2].Value}""{ns}""";
            });
        }
    }
}
