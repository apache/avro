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
using System.Reflection;
using System.Reflection.Emit;
using Avro.Specific;
using BenchmarkDotNet.Attributes;

namespace Avro.Benchmark
{
    /// <summary>
    /// Measures how long <see cref="ObjectCreator"/> takes to resolve the generated classes of a schema
    /// the first time, depending on how many types the application has loaded. A fresh
    /// <see cref="ObjectCreator"/> per operation keeps its type cache cold, which is what every
    /// process pays once for every named schema type it deserializes.
    /// </summary>
    [SimpleJob(launchCount: 1, warmupCount: 2, iterationCount: 10)]
    [MemoryDiagnoser]
    public class ObjectCreatorBenchmarks
    {
        private const int NamedSchemaTypes = 50;
        private const int TypesPerApplicationAssembly = 1_000;

        private string[] _namedSchemaTypeNames;

        /// <summary>
        /// Number of types loaded in addition to the ones of the benchmark process, spread over
        /// assemblies of 1,000 types each. An ASP.NET Core application loads roughly 25,000 types,
        /// large applications several 100,000.
        /// </summary>
        [Params(0, 25_000, 100_000, 250_000)]
        public int AdditionalLoadedTypes { get; set; }

        [GlobalSetup]
        public void Setup()
        {
            for (int i = 0; i < AdditionalLoadedTypes / TypesPerApplicationAssembly; i++)
            {
                DefineAssembly($"Bench.Application{i}", TypesPerApplicationAssembly);
            }

            // The generated classes live in their own assembly, like generated contracts referenced by a service
            _namedSchemaTypeNames = DefineAssembly("Bench.Contracts.V1", NamedSchemaTypes)
                .Select(type => type.FullName)
                .ToArray();

            Console.WriteLine($"// Loaded assemblies: {AppDomain.CurrentDomain.GetAssemblies().Length}, types: {CountLoadedTypes()}");
        }

        [Benchmark]
        public void ResolveNamedSchemaTypes()
        {
            ObjectCreator objectCreator = new ObjectCreator();
            foreach (string name in _namedSchemaTypeNames)
            {
                objectCreator.GetType(name, Schema.Type.Record);
            }
        }

        private static IEnumerable<Type> DefineAssembly(string assemblyName, int typeCount)
        {
            AssemblyBuilder assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(
                new AssemblyName(assemblyName),
                AssemblyBuilderAccess.Run
            );
            ModuleBuilder moduleBuilder = assemblyBuilder.DefineDynamicModule(assemblyName);
            List<Type> types = new List<Type>(typeCount);
            for (int i = 0; i < typeCount; i++)
            {
                types.Add(
                    moduleBuilder
                        .DefineType(
                            $"{assemblyName}.Type{i}",
                            TypeAttributes.Public | TypeAttributes.Class
                        )
                        .CreateType()
                );
            }

            return types;
        }

        private static int CountLoadedTypes()
        {
            int count = 0;
            foreach (Assembly assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                try
                {
                    count += assembly.GetTypes().Length;
                }
                catch (ReflectionTypeLoadException e)
                {
                    count += e.Types.Count(type => type != null);
                }
            }

            return count;
        }
    }
}
