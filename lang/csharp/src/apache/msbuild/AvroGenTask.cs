/**
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
using System.IO;
using System.Linq;
using System.Collections.Generic;
using Microsoft.Build.Framework;
using Microsoft.Build.Utilities;

namespace Avro.msbuild
{
    public class AvroGenTask : Task
    {
        public override bool Execute()
        {
            generatedFiles.Clear();

            try
            {
                Dictionary<string, string> namespaceMapping = new Dictionary<string, string>();

                MessageImportance messageImportance = string.IsNullOrEmpty(MessageLevel) ? 
                    MessageImportance.Normal :
                    (MessageImportance)Enum.Parse(typeof(MessageImportance), MessageLevel);

                var codegen = new CodeGen();

                if (!string.IsNullOrEmpty(NamespaceMapping))
                {
                    foreach(var mapping in NamespaceMapping.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries))
                    {
                        var parts = mapping.Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries);
                        if (parts.Length != 2)
                        {
                            throw new ArgumentException("Malformed namespace mapping. Required format is \"avro.namespace:csharp.namespace\"");
                        }
                        namespaceMapping[parts[0]] = parts[1];
                    }
                }

                if (SchemaFiles != null)
                {
                    foreach (var schemaFile in SchemaFiles)
                    {
                        var schemaText = System.IO.File.ReadAllText(schemaFile.ItemSpec);
                        codegen.AddSchema(schemaText, namespaceMapping);
                    }
                }
                if (ProtocolFiles != null)
                {
                    foreach (var protocolFile in ProtocolFiles)
                    {
                        var protocolText = System.IO.File.ReadAllText(protocolFile.ItemSpec);
                        codegen.AddProtocol(protocolText, namespaceMapping);
                    }
                }

                var generateCode = codegen.GenerateCode();
                var namespaces = generateCode.Namespaces;
                for (var i = namespaces.Count - 1; i >= 0; i--)
                {
                    var types = namespaces[i].Types;
                    for (var j = types.Count - 1; j >= 0; j--)
                    {
                        Log.LogMessage(messageImportance, "Generating {0}.{1}", namespaces[i].Name, types[j].Name);
                        if (SkipDirectories) 
                        { 
                            generatedFiles.Add(new TaskItem(Path.Combine(OutDir.ItemSpec, types[j].Name + ".cs")));
                        } 
                        else 
                        {
                            generatedFiles.Add(new TaskItem(Path.Combine(Path.Combine(OutDir.ItemSpec, namespaces[i].Name.Replace('.', Path.DirectorySeparatorChar)), types[j].Name + ".cs")));
                        }
                    }
                }

                codegen.WriteTypes(OutDir.ItemSpec, SkipDirectories);
            }
            catch(Exception ex)
            {
                Log.LogErrorFromException(ex);
            }

            return !Log.HasLoggedErrors;
        }
        
        HashSet<ITaskItem> generatedFiles = new HashSet<ITaskItem>();

        public ITaskItem[] SchemaFiles { get; set; }
        public ITaskItem[] ProtocolFiles { get; set; }
        
        public string NamespaceMapping { get; set; }
        public bool SkipDirectories { get; set; }

        public string MessageLevel { get; set; }

        /// <summary>
        /// Returns the list of generated files - useful to chain with other build tasks.
        /// </summary>
        [Output]
        public ITaskItem[] GeneratedFiles { get { return generatedFiles.ToArray(); } }

        [Required]
        public ITaskItem OutDir { get; set; }
    }
}
