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
using Avro.Generic;

namespace Avro.IO.Parsing
{
    /// <summary>
    /// The class that generates validating grammar.
    /// </summary>
    public class ValidatingGrammarGenerator
    {
        /// <summary>
        /// Returns the non-terminal that is the start symbol for the grammar for the
        /// given schema <tt>sc</tt>.
        /// </summary>
        public virtual Symbol Generate(Schema schema)
        {
            return Symbol.NewRoot(Generate(schema, new Dictionary<LitS, Symbol>()));
        }

        /// <summary>
        /// Returns the non-terminal that is the start symbol for the grammar for the
        /// given schema <tt>sc</tt>. If there is already an entry for the given schema
        /// in the given map <tt>seen</tt> then that entry is returned. Otherwise a new
        /// symbol is generated and an entry is inserted into the map.
        /// </summary>
        /// <param name="sc">   The schema for which the start symbol is required </param>
        /// <param name="seen"> A map of schema to symbol mapping done so far. </param>
        /// <returns> The start symbol for the schema </returns>
        protected virtual Symbol Generate(Schema sc, IDictionary<LitS, Symbol> seen)
        {
            switch (sc.Tag)
            {
                case Schema.Type.Null:
                    return Symbol.Null;
                case Schema.Type.Boolean:
                    return Symbol.Boolean;
                case Schema.Type.Int:
                    return Symbol.Int;
                case Schema.Type.Long:
                    return Symbol.Long;
                case Schema.Type.Float:
                    return Symbol.Float;
                case Schema.Type.Double:
                    return Symbol.Double;
                case Schema.Type.String:
                    return Symbol.String;
                case Schema.Type.Bytes:
                    return Symbol.Bytes;
                case Schema.Type.Fixed:
                    return Symbol.NewSeq(new Symbol.IntCheckAction(((FixedSchema)sc).Size), Symbol.Fixed);
                case Schema.Type.Enumeration:
                    return Symbol.NewSeq(new Symbol.IntCheckAction(((EnumSchema)sc).Symbols.Count), Symbol.Enum);
                case Schema.Type.Array:
                    return Symbol.NewSeq(
                        Symbol.NewRepeat(Symbol.ArrayEnd, Generate(((ArraySchema)sc).ItemSchema, seen)),
                        Symbol.ArrayStart);
                case Schema.Type.Map:
                    return Symbol.NewSeq(
                        Symbol.NewRepeat(Symbol.MapEnd, Generate(((MapSchema)sc).ValueSchema, seen), Symbol.String),
                        Symbol.MapStart);
                case Schema.Type.Record:
                    {
                        LitS wsc = new LitS(sc);
                        if (!seen.TryGetValue(wsc, out Symbol rresult))
                        {
                            Symbol[] production = new Symbol[((RecordSchema)sc).Fields.Count];

                            // We construct a symbol without filling the array. Please see
                            // <see cref="Symbol.production"/> for the reason.
                            rresult = Symbol.NewSeq(production);
                            seen[wsc] = rresult;

                            int j = production.Length;
                            foreach (Field f in ((RecordSchema)sc).Fields)
                            {
                                production[--j] = Generate(f.Schema, seen);
                            }
                        }

                        return rresult;
                    }
                case Schema.Type.Union:
                    IList<Schema> subs = ((UnionSchema)sc).Schemas;
                    Symbol[] symbols = new Symbol[subs.Count];
                    string[] labels = new string[subs.Count];

                    int i = 0;
                    foreach (Schema b in ((UnionSchema)sc).Schemas)
                    {
                        symbols[i] = Generate(b, seen);
                        labels[i] = b.Fullname;
                        i++;
                    }

                    return Symbol.NewSeq(Symbol.NewAlt(symbols, labels), Symbol.Union);
                case Schema.Type.Logical:
                    return Generate((sc as LogicalSchema).BaseSchema, seen);
                default:
                    throw new Exception("Unexpected schema type");
            }
        }

        /// <summary>
        /// A wrapper around Schema that does "==" equality.
        /// </summary>
        protected class LitS
        {
            private readonly Schema actual;

            /// <summary>
            /// Initializes a new instance of the <see cref="LitS"/> class.
            /// </summary>
            public LitS(Schema actual)
            {
                this.actual = actual;
            }

            /// <summary>
            /// Two LitS are equal if and only if their underlying schema is the same (not
            /// merely equal).
            /// </summary>
            public override bool Equals(object o)
            {
                if (o is null)
                {
                    return false;
                }

                if (Object.ReferenceEquals(this, o))
                {
                    return true;
                }

                if (GetType() != o.GetType())
                {
                    return false;
                }

                return actual.Equals(((LitS)o).actual);
            }

            /// <summary>
            /// Returns the hash code for the current <see cref="LitS" />.
            /// </summary>
            public override int GetHashCode()
            {
                return actual.GetHashCode();
            }
        }
    }
}
