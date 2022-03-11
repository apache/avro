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

namespace Avro
{
    public static class Generator
    {
        /// <summary>
        /// Generates the protocol.
        /// </summary>
        /// <param name="protocolArguments">The protocol arguments.</param>
        public static void GenerateProtocol(ProtocolArguments protocolArguments)
        {
            ValidateArguments(protocolArguments);
            string text = GetFileContent(protocolArguments.ProtocolFile);
            Protocol protocol = Protocol.Parse(text);
            CodeGen codeGen = new CodeGen();
            codeGen.AddProtocol(protocol);
            AddNamespaceMapping(protocolArguments.NamespaceMapping, ref codeGen);
            codeGen.GenerateCode();
            codeGen.WriteTypes(protocolArguments.OutputDirectory);
        }

        /// <summary>
        /// Generates the schema.
        /// </summary>
        /// <param name="schemaArguments">The schema arguments.</param>
        public static void GenerateSchema(SchemaArguments schemaArguments)
        {
            ValidateArguments(schemaArguments);
            string text = GetFileContent(schemaArguments.SchemaFile);
            Schema schema = Schema.Parse(text);
            CodeGen codeGen = new CodeGen();
            codeGen.AddSchema(schema);
            AddNamespaceMapping(schemaArguments.NamespaceMapping, ref codeGen);
            codeGen.GenerateCode();
            codeGen.WriteTypes(schemaArguments.OutputDirectory, schemaArguments.CreateNamespaceDirectories);
        }

        /// <summary>
        /// Adds the namespace mapping.
        /// </summary>
        /// <param name="namespaces">The namespaces.</param>
        /// <param name="codeGen">The code gen instance.</param>
        /// <exception cref="ArgumentNullException">codeGen</exception>
        private static void AddNamespaceMapping(Dictionary<string, string> namespaces, ref CodeGen codeGen)
        {
            if (codeGen == null)
            {
                throw new ArgumentNullException(nameof(codeGen));
            }

            foreach (KeyValuePair<string, string> entry in namespaces)
            {
                codeGen.NamespaceMapping[entry.Key] = entry.Value;
            }
        }

        /// <summary>
        /// Gets the content of the file.
        /// </summary>
        /// <param name="filePath">The file path.</param>
        /// <returns></returns>
        /// <exception cref="Avro.AvroException">Content is empty for {filePath}</exception>
        private static string GetFileContent(string filePath)
        {
            string text = System.IO.File.ReadAllText(filePath);

            if (string.IsNullOrWhiteSpace(text))
            {
                throw new AvroException($"Content is empty for {filePath}");
            }

            return text;
        }

        /// <summary>
        /// Validates the arguments.
        /// </summary>
        /// <param name="schemaArguments">The schema arguments.</param>
        /// <exception cref="ArgumentNullException">schemaArguments</exception>
        private static void ValidateArguments(SchemaArguments schemaArguments)
        {
            if (schemaArguments == null)
            {
                throw new ArgumentNullException(nameof(schemaArguments));
            }

            ValidateFilePath(schemaArguments.SchemaFile);
            ValidateDirectory(schemaArguments.OutputDirectory);
        }

        /// <summary>
        /// Validates the arguments.
        /// </summary>
        /// <param name="protocolArguments">The protocol arguments.</param>
        /// <exception cref="ArgumentNullException">protocolArguments</exception>
        private static void ValidateArguments(ProtocolArguments protocolArguments)
        {
            if (protocolArguments == null)
            {
                throw new ArgumentNullException(nameof(protocolArguments));
            }

            ValidateFilePath(protocolArguments.ProtocolFile);
            ValidateDirectory(protocolArguments.OutputDirectory);
        }

        /// <summary>
        /// Validates the directory.
        /// </summary>
        /// <param name="directory">The directory.</param>
        /// <exception cref="System.IO.DirectoryNotFoundException">Directory can not be found {directory}</exception>
        private static void ValidateDirectory(string directory)
        {
            if (!System.IO.Directory.Exists(directory))
            {
                throw new System.IO.DirectoryNotFoundException($"Directory can not be found {directory}");
            }
        }

        /// <summary>
        /// Validates the file path.
        /// </summary>
        /// <param name="filePath">The file path.</param>
        /// <exception cref="System.IO.FileNotFoundException">Path to input was not found {filePath}</exception>
        private static void ValidateFilePath(string filePath)
        {
            if (!System.IO.File.Exists(filePath))
            {
                throw new System.IO.FileNotFoundException($"Path to input was not found {filePath}");
            }
        }
    }
}
