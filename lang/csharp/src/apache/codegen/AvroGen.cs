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
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Avro
{
    public class AvroGen
    {
        public static int Main(string[] args)
        {
            // Print usage if no arguments provided
            if (args.Length == 0)
            {
                Usage();
                return 1;
            }

            // Print usage if help requested
            if (args.Contains("-h") || args.Contains("--help"))
            {
                Usage();
                return 0;
            }

            // Parse command line arguments
            bool? isProtocol = null;
            string inputFile = null;
            string outputDir = null;
            var namespaceMapping = new Dictionary<string, string>();
            for (int i = 0; i < args.Length; ++i)
            {
                if (args[i] == "-p")
                {
                    if (i + 1 >= args.Length)
                    {
                        Console.Error.WriteLine("Missing path to protocol file");
                        Usage();
                        return 1;
                    }

                    isProtocol = true;
                    inputFile = args[++i];
                }
                else if (args[i] == "-s")
                {
                    if (i + 1 >= args.Length)
                    {
                        Console.Error.WriteLine("Missing path to schema file");
                        Usage();
                        return 1;
                    }

                    isProtocol = false;
                    inputFile = args[++i];
                }
                else if (args[i] == "--namespace")
                {
                    if (i + 1 >= args.Length)
                    {
                        Console.Error.WriteLine("Missing namespace mapping");
                        Usage();
                        return 1;
                    }

                    var parts = args[++i].Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length != 2)
                    {
                        Console.Error.WriteLine("Malformed namespace mapping. Required format is \"avro.namespace:csharp.namespace\"");
                        Usage();
                        return 1;
                    }

                    namespaceMapping[parts[0]] = parts[1];
                }
                else if (outputDir == null)
                {
                    outputDir = args[i];
                }
                else
                {
                    Console.Error.WriteLine("Unexpected command line argument: {0}", args[i]);
                    Usage();
                }
            }

            // Ensure we got all the command line arguments we need
            bool isValid = true;
            if (!isProtocol.HasValue || inputFile == null)
            {
                Console.Error.WriteLine("Must provide either '-p <protocolfile>' or '-s <schemafile>'");
                isValid = false;
            }
            else if (outputDir == null)
            {
                Console.Error.WriteLine("Must provide 'outputdir'");
                isValid = false;
            }

            if (!isValid)
            {
                Usage();
                return 1;
            }

            try
            {
                if (isProtocol.Value)
                    GenProtocol(inputFile, outputDir, namespaceMapping);
                else
                    GenSchema(inputFile, outputDir, namespaceMapping);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Exception occurred. " + ex.Message);
                return 1;
            }

            return 0;
        }

        static void Usage()
        {
            Console.WriteLine("{0}\n\n" +
                "Usage:\n" +
                "  avrogen -p <protocolfile> <outputdir> [--namespace <my.avro.ns:my.csharp.ns>]\n" +
                "  avrogen -s <schemafile> <outputdir> [--namespace <my.avro.ns:my.csharp.ns>]\n\n" +
                "Options:\n" +
                "  -h --help   Show this screen.\n" +
                "  --namespace Map an Avro schema/protocol namespace to a C# namespace.\n" +
                "              The format is \"my.avro.namespace:my.csharp.namespace\".\n" +
                "              May be specified multiple times to map multiple namespaces.\n",
                AppDomain.CurrentDomain.FriendlyName);
            return;
        }

        static void GenProtocol(string infile, string outdir,
            IEnumerable<KeyValuePair<string, string>> namespaceMapping)
        {
            string text = System.IO.File.ReadAllText(infile);

            if (namespaceMapping != null)
                text = ReplaceMappedNamespacesInSchema(text, namespaceMapping);

            Protocol protocol = Protocol.Parse(text);

            CodeGen codegen = new CodeGen();
            codegen.AddProtocol(protocol);

            codegen.GenerateCode();
            codegen.WriteTypes(outdir);
        }

        static void GenSchema(string infile, string outdir,
            IEnumerable<KeyValuePair<string, string>> namespaceMapping)
        {
            string text = System.IO.File.ReadAllText(infile);

            if (namespaceMapping != null)
                text = ReplaceMappedNamespacesInSchema(text, namespaceMapping);

            Schema schema = Schema.Parse(text);

            CodeGen codegen = new CodeGen();
            codegen.AddSchema(schema);

            codegen.GenerateCode();
            codegen.WriteTypes(outdir);
        }

        public static string ReplaceMappedNamespacesInSchema(string schema, IEnumerable<KeyValuePair<string, string>> namespaceMapping)
        {
            if (namespaceMapping == null)
                return schema;

            // Replace namespace in "namespace" definitions: 
            //    "namespace": "originalnamespace" -> "namespace": "mappednamespace"
            //    "namespace": "originalnamespace.whatever" -> "namespace": "mappednamespace.whatever"
            // Note: It keeps the original whitespaces
            return Regex.Replace(schema, @"""namespace""(\s*):(\s*)""([^""]*)""", m =>
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
