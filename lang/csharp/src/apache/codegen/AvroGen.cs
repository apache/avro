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

namespace Avro
{
    public static class AvroGen
    {
        private static readonly CommandLineArgument _createNamespaceDirectoriesParameter;
        private static readonly CommandLineArgument _helpParameter;
        private static readonly CommandLineArgument _namespaceParameter;
        private static readonly CommandLineArgument _protocolParameter;
        private static readonly CommandLineArgument _schemaParameter;

        static AvroGen()
        {
            // Primary usage parameters
            _schemaParameter = new CommandLineArgument
            {
                Parameter = "-s",
                FriendlyName = "Schema",
                Usage = "avrogen -s <schemafile> <outputdir> [--namespace <my.avro.ns:my.csharp.ns>]",
                IsOptional = false,
            };

            _protocolParameter = new CommandLineArgument
            {
                Parameter = "-p",
                FriendlyName = "Protocol",
                Usage = "avrogen -p <protocolfile> <outputdir> [--namespace <my.avro.ns:my.csharp.ns>]",
                IsOptional = false,
            };

            // Optional parameters
            _helpParameter = new CommandLineArgument
            {
                Parameter = "-h",
                Aliases = new List<string>() { "--help" },
                FriendlyName = "Help",
                Usage = "Show this screen.",
                IsOptional = true,
            };

            _namespaceParameter = new CommandLineArgument
            {
                Parameter = "--namespace",
                FriendlyName = "Namespace",
                Usage = "Map an Avro schema/protocol namespace to a C# namespace. " +
                "The format is \"my.avro.namespace:my.csharp.namespace\". " +
                "May be specified multiple times to map multiple namespaces. ",
                IsOptional = true,
            };

            _createNamespaceDirectoriesParameter = new CommandLineArgument
            {
                Parameter = "--create-namespace-directories",
                FriendlyName = "Create Namespace Directories",
                Usage = "Optional parameter to change how the files are output to the outputdir. " +
                "Default is set to true. False will output the files to the root of the outputdir",
                IsOptional = true,
            };
        }

        private static int Main(string[] args)
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

            try
            {
                if (args.Contains(_protocolParameter.Parameter))
                {
                    ProtocolArguments protocolArguments = ParseProtocolInput(args);
                    Generator.GenerateProtocol(protocolArguments);
                }

                if (args.Contains(_schemaParameter.Parameter))
                {
                    SchemaArguments schemaArguments = ParseSchemaInput(args);
                    Generator.GenerateSchema(schemaArguments);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                Usage();
                return 1;
            }

            return 0;
        }

        private static void Usage()
        {
            Console.WriteLine(
                $"{AppDomain.CurrentDomain.FriendlyName}\n\n" +
                "Usage:\n" +
                $"  {_protocolParameter.Usage}\n" +
                $"  {_schemaParameter.Usage}\n\n" +
                "Options:\n" +
                FormatUsage(_helpParameter) +
                FormatUsage(_namespaceParameter) +
                FormatUsage(_createNamespaceDirectoriesParameter)
                );
            return;
        }

        private static string FormatUsage(CommandLineArgument parameter)
        {
            // Use the longest parameter.  Add 2 spaces at front and 1 space at the end.
            int length = _createNamespaceDirectoriesParameter.Parameter.Length + 3;

            string paramWithAliases = parameter.Parameter + " " + string.Join(' ', parameter.Aliases);

            string paramText = $"  {paramWithAliases}{new string(' ', length - paramWithAliases.Length - 2)}";

            List<string> sentences = WordWrap(parameter.Usage, 60).ToList();

            string fullText = paramText;

            for (int i = 0; i < sentences.Count; i++)
            {
                // spacing after parameter
                if (i == 0)
                {
                    fullText += sentences[i] + System.Environment.NewLine;
                    continue;
                }

                // Add sentence on new line
                fullText += new string(' ', paramText.Length) + sentences[i] + System.Environment.NewLine;
            }

            return fullText + System.Environment.NewLine;
        }

        private static (string, string) GetThreePartArgument(string[] args, string option)
        {
            IEnumerable<string> arguments = args.SkipWhile(i => i != option).Skip(1).Take(2);

            return (arguments.First(), arguments.Last());
        }

        private static string GetTwoPartArgument(string[] args, string option)
            => args.SkipWhile(i => i != option).Skip(1).Take(1).FirstOrDefault();

        private static List<string> GetTwoPartArguments(string[] args, string option)
        {
            List<string> arguments = new List<string>();
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i] == option)
                {
                    arguments.Add(args[i + 1]);
                    i++;
                }
            }

            return arguments;
        }

        private static bool ParseCreateNamespaceDirectories(string[] args)
        {
            string createDirectoriesValue = GetTwoPartArgument(args, _createNamespaceDirectoriesParameter.Parameter);

            if (string.IsNullOrWhiteSpace(createDirectoriesValue) |
                (!createDirectoriesValue.Equals("true", StringComparison.InvariantCultureIgnoreCase) &
                !createDirectoriesValue.Equals("false", StringComparison.InvariantCultureIgnoreCase)))
            {
                throw new AvroException($"{_createNamespaceDirectoriesParameter} parameters must have a value of true or false");
            }

            return createDirectoriesValue.Equals("true", StringComparison.InvariantCultureIgnoreCase);
        }

        private static Dictionary<string, string> ParseNamespace(string[] args)
        {
            List<string> namespaces = GetTwoPartArguments(args, _namespaceParameter.Parameter);

            if (namespaces.Count == 0)
            {
                throw new AvroException("Missing namespace mapping");
            }

            Dictionary<string, string> arguments = new Dictionary<string, string>();

            foreach (string ns in namespaces)
            {
                string[] parts = ns.Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length != 2)
                {
                    Console.Error.WriteLine($"Malformed namespace mapping. Required format is \"avro.namespace:csharp.namespace\" Actual: {ns}");
                }

                arguments[parts[0]] = parts[1];
            }

            return arguments;
        }

        private static ProtocolArguments ParseProtocolInput(string[] args)
        {
            ProtocolArguments arguments = new ProtocolArguments();

            (string, string) argument = GetThreePartArgument(args, _protocolParameter.Parameter);

            arguments.ProtocolFile = argument.Item1;
            arguments.OutputDirectory = argument.Item2;

            if (args.Contains(_namespaceParameter.Parameter))
            {
                arguments.NamespaceMapping = ParseNamespace(args);
            }

            return arguments;
        }

        private static SchemaArguments ParseSchemaInput(string[] args)
        {
            SchemaArguments arguments = new SchemaArguments();

            (string, string) argument = GetThreePartArgument(args, _schemaParameter.Parameter);

            arguments.SchemaFile = argument.Item1;
            arguments.OutputDirectory = argument.Item2;

            if (args.Contains(_namespaceParameter.Parameter))
            {
                arguments.NamespaceMapping = ParseNamespace(args);
            }

            if (args.Contains(_createNamespaceDirectoriesParameter.Parameter))
            {
                arguments.CreateNamespaceDirectories = ParseCreateNamespaceDirectories(args);
            }

            return arguments;
        }

        private static IEnumerable<string> WordWrap(string text, int width)
        {
            string forcedBreakZonePattern = @"\n";
            string normalBreakZonePattern = @"\s+|(?<=[-,.;])|$";

            var forcedZones = Regex.Matches(text, forcedBreakZonePattern).Cast<Match>().ToList();
            var normalZones = Regex.Matches(text, normalBreakZonePattern).Cast<Match>().ToList();

            int start = 0;

            while (start < text.Length)
            {
                var zone =
                    forcedZones.Find(z => z.Index >= start && z.Index <= start + width) ??
                    normalZones.FindLast(z => z.Index >= start && z.Index <= start + width);

                if (zone == null)
                {
                    yield return text.Substring(start, width);
                    start += width;
                }
                else
                {
                    yield return text.Substring(start, zone.Index - start);
                    start = zone.Index + zone.Length;
                }
            }
        }
    }
}
