/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System;
using System.Collections.Generic;
using System.Text;

namespace Avro
{
    class AvroGen
    {
        static void Main(string[] args)
        {
            if (args.Length != 3)
            {
                Usage();
                return;
            }
            if (args[0] == "-p")
                GenProtocol(args[1], args[2]);
            else if (args[0] == "-s")
                GenSchema(args[1], args[2]);
            else
                Usage();
        }

        static void Usage()
        {
            Console.WriteLine("Usage:\navrogen -p <protocolfile> <outputdir>\navrogen -s <schemafile> <outputdir>");
            return;
        }
        static void GenProtocol(string infile, string outdir)
        {
            try
            {
                string text = System.IO.File.ReadAllText(infile);
                Protocol protocol = Protocol.Parse(text);

                CodeGen codegen = new CodeGen();
                codegen.AddProtocol(protocol);

                codegen.GenerateCode();
                codegen.WriteTypes(outdir);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception occurred. " + ex.Message);
            }
        }
        static void GenSchema(string infile, string outdir)
        {
            try
            {
                string text = System.IO.File.ReadAllText(infile);
                Schema schema = Schema.Parse(text);

                CodeGen codegen = new CodeGen();
                codegen.AddSchema(schema);

                codegen.GenerateCode();
                codegen.WriteTypes(outdir);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception occurred. " + ex.Message);
            }
        }
    }
}
