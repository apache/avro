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

namespace Avro.IO.Parsing
{
    /// <summary>
    /// The class that generates a grammar suitable to parse Avro data in JSON
    /// format.
    /// </summary>
    public class JsonGrammarGenerator : ValidatingGrammarGenerator
    {
        /// <summary>
        /// Returns the non-terminal that is the start symbol for the grammar for the
        /// grammar for the given schema <tt>schema</tt>.
        /// </summary>
        public override Symbol Generate(Schema schema)
        {
            return Symbol.NewRoot(Generate(schema, new Dictionary<LitS, Symbol>()));
        }

        /// <summary>
        /// Returns the non-terminal that is the start symbol for grammar of the given
        /// schema <tt>sc</tt>. If there is already an entry for the given schema in the
        /// given map <tt>seen</tt> then that entry is returned. Otherwise a new symbol
        /// is generated and an entry is inserted into the map.
        /// </summary>
        /// <param name="sc">   The schema for which the start symbol is required </param>
        /// <param name="seen"> A map of schema to symbol mapping done so far. </param>
        /// <returns> The start symbol for the schema </returns>
        protected override Symbol Generate(Schema sc, IDictionary<LitS, Symbol> seen)
        {
            switch (sc.Tag)
            {
                case Schema.Type.Null:
                case Schema.Type.Boolean:
                case Schema.Type.Int:
                case Schema.Type.Long:
                case Schema.Type.Float:
                case Schema.Type.Double:
                case Schema.Type.String:
                case Schema.Type.Bytes:
                case Schema.Type.Fixed:
                case Schema.Type.Union:
                    return base.Generate(sc, seen);
                case Schema.Type.Enumeration:
                    return Symbol.NewSeq(new Symbol.EnumLabelsAction(((EnumSchema)sc).Symbols), Symbol.Enum);
                case Schema.Type.Array:
                    return Symbol.NewSeq(
                        Symbol.NewRepeat(Symbol.ArrayEnd, Symbol.ItemEnd, Generate(((ArraySchema)sc).ItemSchema, seen)),
                        Symbol.ArrayStart);
                case Schema.Type.Map:
                    return Symbol.NewSeq(
                        Symbol.NewRepeat(Symbol.MapEnd, Symbol.ItemEnd, Generate(((MapSchema)sc).ValueSchema, seen),
                            Symbol.MapKeyMarker, Symbol.String), Symbol.MapStart);
                case Schema.Type.Record:
                    {
                        LitS wsc = new LitS(sc);
                        if (!seen.TryGetValue(wsc, out Symbol rresult))
                        {
                            Symbol[] production = new Symbol[((RecordSchema)sc).Fields.Count * 3 + 2];
                            rresult = Symbol.NewSeq(production);
                            seen[wsc] = rresult;

                            int i = production.Length;
                            int n = 0;
                            production[--i] = Symbol.RecordStart;
                            foreach (Field f in ((RecordSchema)sc).Fields)
                            {
                                production[--i] = new Symbol.FieldAdjustAction(n, f.Name, f.Aliases);
                                production[--i] = Generate(f.Schema, seen);
                                production[--i] = Symbol.FieldEnd;
                                n++;
                            }

                            production[i - 1] = Symbol.RecordEnd;
                        }

                        return rresult;
                    }
                case Schema.Type.Logical:
                    return Generate((sc as LogicalSchema).BaseSchema, seen);
                default:
                    throw new Exception("Unexpected schema type");
            }
        }
    }
}
