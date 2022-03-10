using System;
using System.Collections.Generic;
using System.Linq;

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

namespace Avro
{
    public class AvroGen
    {
        private const string _createNamespaceDirectoriesParameter = "--create-namespace-directories";
        private const string _namespaceParameter = "--namespace";
        private const string _outputDirectoryParameter = "--outputdir";
        private const string _protocolName = "Protocol";
        private const string _protocolParameter = "-p";
        private const string _schemaName = "Schema";
        private const string _schemaParameter = "-s";
        private static Arguments _arguments = new Arguments();

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

            // Parse command line arguments
            if (args.Contains(_protocolParameter))
            {
                if (ParseInput(args, _protocolParameter, _protocolName) == 0)
                {
                    return GenProtocol();
                }
                else
                {
                    Usage();
                    return 1;
                }
            }

            if (args.Contains(_schemaParameter))
            {
                if (ParseInput(args, _schemaParameter, _schemaName) == 0)
                {
                    return GenSchema();
                }
                else
                {
                    Usage();
                    return 1;
                }
            }

            Console.Error.WriteLine($"Must provide either '{_protocolParameter} <protocolfile>' " +
                $"or '{_schemaParameter} <schemafile>'");
            Usage();
            return 1;
        }

        private static void Usage()
        {
            Console.WriteLine(
                $"{AppDomain.CurrentDomain.FriendlyName}\n\n" +
                "Usage:\n" +
                $"  avrogen {_protocolParameter} <protocolfile> <outputdir> [{_namespaceParameter} <my.avro.ns:my.csharp.ns>]\n" +
                $"  avrogen {_schemaParameter} <schemafile> <outputdir> [{_namespaceParameter} <my.avro.ns:my.csharp.ns>]\n\n" +
                "Options:\n" +
                $"{FormatUsage("-h --help")}Show this screen.\n\n" +
                $"{FormatUsage(_namespaceParameter)}Map an Avro schema/protocol namespace to a C# namespace.\n" +
                $"{FormatUsage(null)}the format is \"my.avro.namespace:my.csharp.namespace\".\n" +
                $"{FormatUsage(null)}May be specified multiple times to map multiple namespaces.\n\n" +
                $"{FormatUsage(_outputDirectoryParameter)}Optional parameter for setting output directory.\n" +
                $"{FormatUsage(null)}{_outputDirectoryParameter} <outputdir> instead of <outputdir>\n\n" +
                $"{FormatUsage(_createNamespaceDirectoriesParameter)}Optional parameter to change how the files \n" +
                $"{FormatUsage(null)}are output to the outputdir.  Default is set to true.\n" +
                $"{FormatUsage(null)}False will output the files to the root of the outputdir\n"
                );
            return;
        }

        private static int GenProtocol()
        {
            try
            {
                string text = System.IO.File.ReadAllText(_arguments.Input);
                Protocol protocol = Protocol.Parse(text);

                CodeGen codegen = new CodeGen();
                codegen.AddProtocol(protocol);

                foreach (var entry in _arguments.NamespaceMapping)
                {
                    codegen.NamespaceMapping[entry.Key] = entry.Value;
                }

                codegen.GenerateCode();
                codegen.WriteTypes(_arguments.OutputDirectory);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Exception occurred. " + ex.Message);
                return 1;
            }

            return 0;
        }

        private static int GenSchema()
        {
            try
            {
                string text = System.IO.File.ReadAllText(_arguments.Input);
                Schema schema = Schema.Parse(text);

                CodeGen codegen = new CodeGen();
                codegen.AddSchema(schema);

                foreach (var entry in _arguments.NamespaceMapping)
                {
                    codegen.NamespaceMapping[entry.Key] = entry.Value;
                }

                codegen.GenerateCode();
                codegen.WriteTypes(_arguments.OutputDirectory, _arguments.CreateNamespaceDirectories);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Exception occurred. " + ex.Message);
                return 1;
            }

            return 0;
        }

        /// <summary>
        /// Generates the protocol.
        /// </summary>
        /// <param name="arguments">The arguments.</param>
        /// <returns>True if successfully executed</returns>
        public static bool GenerateProtocol(Arguments arguments)
        {
            ValidateArguments(arguments);
            _arguments = arguments;
            int success = GenProtocol();
            return success == 0 ? true : false;
        }

        /// <summary>
        /// Generates the schema.
        /// </summary>
        /// <param name="arguments">The arguments.</param>
        /// <returns>True if successfully executed</returns>
        public static bool GenerateSchema(Arguments arguments)
        {
            ValidateArguments(arguments);
            _arguments = arguments;
            int success = GenSchema();
            return success == 0 ? true : false;
        }

        /// <summary>
        /// Validates the arguments.
        /// </summary>
        /// <param name="arguments">The arguments.</param>
        /// <exception cref="ArgumentNullException">arguments</exception>
        /// <exception cref="System.IO.FileNotFoundException">Path to input was not found {arguments.Input}</exception>
        /// <exception cref="System.IO.DirectoryNotFoundException">Directory can not be found {arguments.OutputDirectory}</exception>
        private static void ValidateArguments(Arguments arguments)
        {
            if (arguments == null)
            {
                throw new ArgumentNullException(nameof(arguments));
            }

            if(!System.IO.File.Exists(arguments.Input))
            {
                throw new System.IO.FileNotFoundException($"Path to input was not found {arguments.Input}");
            }

            if(!System.IO.Directory.Exists(arguments.OutputDirectory))
            {
                throw new System.IO.DirectoryNotFoundException($"Directory can not be found {arguments.OutputDirectory}");
            }
        }

        private static string FormatUsage(string parameter)
        {
            // Use the longest parameter.  Add 2 spaces at front and 1 space at the end.
            int length = _createNamespaceDirectoriesParameter.Length + 3;

            if (string.IsNullOrEmpty(parameter))
            {
                return new string(' ', length);
            }

            return $"  {parameter}{new string(' ', length - parameter.Length - 2)}";
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

        private static int ParseCreateNamespaceDirectories(string[] args)
        {
            string createDirectoriesValue = GetTwoPartArgument(args, _createNamespaceDirectoriesParameter);

            if (string.IsNullOrWhiteSpace(createDirectoriesValue) |
                (!createDirectoriesValue.Equals("true", StringComparison.InvariantCultureIgnoreCase) &
                !createDirectoriesValue.Equals("false", StringComparison.InvariantCultureIgnoreCase)))
            {
                Console.Error.WriteLine($"{_createNamespaceDirectoriesParameter} parameters must have a value of true or false");
                return 1;
            }

            _arguments.CreateNamespaceDirectories = createDirectoriesValue.Equals("true", StringComparison.InvariantCultureIgnoreCase);

            return 0;
        }

        private static int ParseInput(string[] args, string parameter, string parameterName)
        {
            _arguments.Input = GetTwoPartArgument(args, parameter);
            if (_arguments.HasInput)
            {
                Console.Error.WriteLine($"Missing path to {parameterName.ToLower()} file");
                return 1;
            }

            if (!System.IO.File.Exists(_arguments.Input))
            {
                Console.Error.WriteLine($"{parameterName} file does not exist");
                return 1;
            }

            if (ParseOutputDirectory(args, Array.IndexOf(args, parameter) + 2) == 1)
            {
                return 1;
            }

            if (args.Contains(_namespaceParameter) && ParseNamespace(args) == 1)
            {
                return 1;
            }

            if (args.Contains(_createNamespaceDirectoriesParameter) && ParseCreateNamespaceDirectories(args) == 1)
            {
                return 1;
            }

            return 0;
        }

        private static int ParseNamespace(string[] args)
        {
            List<string> namespaces = GetTwoPartArguments(args, _namespaceParameter);

            if (namespaces.Count == 0)
            {
                Console.Error.WriteLine("Missing namespace mapping");
                return 1;
            }

            foreach (string ns in namespaces)
            {
                string[] parts = ns.Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length != 2)
                {
                    Console.Error.WriteLine($"Malformed namespace mapping. Required format is \"avro.namespace:csharp.namespace\" Actual: {ns}");
                    return 1;
                }

                _arguments.NamespaceMapping[parts[0]] = parts[1];
            }

            return 0;
        }

        private static int ParseOutputDirectory(string[] args, int? expectedPosition)
        {
            // Was outputdir set via parameter
            if (args.Contains(_outputDirectoryParameter))
            {
                _arguments.OutputDirectory = GetTwoPartArgument(args, _outputDirectoryParameter);

                if (!_arguments.HasOutputDirectory | !System.IO.Directory.Exists(_arguments.OutputDirectory))
                {
                    Console.Error.WriteLine($"Must provide {_outputDirectoryParameter} <outputdir>");
                    return 1;
                }

                return 0;
            }

            if (expectedPosition.HasValue)
            {
                _arguments.OutputDirectory = args[expectedPosition.Value];

                if (!_arguments.HasOutputDirectory | !System.IO.Directory.Exists(_arguments.OutputDirectory))
                {
                    Console.Error.WriteLine("Must provide 'outputdir'");
                    return 1;
                }
            }

            return 0;
        }
    }
}
