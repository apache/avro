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
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Avro.IO.Parsing
{
    /// <summary>
    /// Symbol is the base of all symbols (terminals and non-terminals) of the
    /// grammar.
    /// </summary>
    public abstract class Symbol
    {
        /// <summary>
        /// The type of symbol.
        /// </summary>
        public enum Kind
        {
            /// <summary>
            /// terminal symbols which have no productions </summary>
            Terminal,

            /// <summary>
            /// Start symbol for some grammar </summary>
            Root,

            /// <summary>
            /// non-terminal symbol which is a sequence of one or more other symbols </summary>
            Sequence,

            /// <summary>
            /// non-terminal to represent the contents of an array or map </summary>
            Repeater,

            /// <summary>
            /// non-terminal to represent the union </summary>
            Alternative,

            /// <summary>
            /// non-terminal action symbol which are automatically consumed </summary>
            ImplicitAction,

            /// <summary>
            /// non-terminal action symbol which is explicitly consumed </summary>
            ExplicitAction
        }

        /// The kind of this symbol.
        public readonly Kind SymKind;

        /// <summary>
        /// The production for this symbol. If this symbol is a terminal this is
        /// <tt>null</tt>. Otherwise this holds the the sequence of the symbols that
        /// forms the production for this symbol. The sequence is in the reverse order of
        /// production. This is useful for easy copying onto parsing stack.
        ///
        /// Please note that this is a final. So the production for a symbol should be
        /// known before that symbol is constructed. This requirement cannot be met for
        /// those symbols which are recursive (e.g. a record that holds union a branch of
        /// which is the record itself). To resolve this problem, we initialize the
        /// symbol with an array of nulls. Later we fill the symbols. Not clean, but
        /// works. The other option is to not have this field a final. But keeping it
        /// final and thus keeping symbol immutable gives some comfort. See various
        /// generators how we generate records.
        /// </summary>
        public readonly Symbol[] Production;

        /// <summary>
        /// Constructs a new symbol of the given kind.
        /// </summary>
        protected Symbol(Kind kind) : this(kind, null)
        {
        }

        /// <summary>
        /// Constructs a new symbol of the given kind and production.
        /// </summary>
        protected Symbol(Kind kind, Symbol[] production)
        {
            this.Production = production;
            this.SymKind = kind;
        }

        /// <summary>
        /// A convenience method to construct a root symbol.
        /// </summary>
        public static Symbol NewRoot(params Symbol[] symbols)
        {
            return new Root(symbols);
        }

        /// <summary>
        /// A convenience method to construct a sequence.
        /// </summary>
        /// <param name="production"> The constituent symbols of the sequence. </param>
        public static Symbol NewSeq(params Symbol[] production)
        {
            return new Sequence(production);
        }

        /// <summary>
        /// A convenience method to construct a repeater.
        /// </summary>
        /// <param name="endSymbol"> The end symbol. </param>
        /// <param name="symsToRepeat"> The symbols to repeat in the repeater. </param>
        public static Symbol NewRepeat(Symbol endSymbol, params Symbol[] symsToRepeat)
        {
            return new Repeater(endSymbol, symsToRepeat);
        }

        /// <summary>
        /// A convenience method to construct a union.
        /// </summary>
        public static Symbol NewAlt(Symbol[] symbols, string[] labels)
        {
            return new Alternative(symbols, labels);
        }

        /// <summary>
        /// A convenience method to construct an ErrorAction.
        /// </summary>
        /// <param name="e"> </param>
        protected static Symbol Error(string e)
        {
            return new ErrorAction(e);
        }

        /// <summary>
        /// A convenience method to construct a ResolvingAction.
        /// </summary>
        /// <param name="w"> The writer symbol </param>
        /// <param name="r"> The reader symbol </param>
        protected static Symbol Resolve(Symbol w, Symbol r)
        {
            return new ResolvingAction(w, r);
        }

        /// <summary>
        /// Fixup symbol.
        /// </summary>
        protected class Fixup
        {
            private readonly Symbol[] symbols;

            /// <summary>
            /// The symbols.
            /// </summary>
            public Symbol[] Symbols
            {
                get { return (Symbol[])symbols.Clone(); }
            }
            /// <summary>
            /// The position.
            /// </summary>
            public readonly int Pos;

            /// <summary>
            /// Initializes a new instance of the <see cref="Fixup"/> class.
            /// </summary>
            public Fixup(Symbol[] symbols, int pos)
            {
                this.symbols = (Symbol[])symbols.Clone();
                this.Pos = pos;
            }
        }

        /// <summary>
        /// Flatten the given sub-array of symbols into a sub-array of symbols.
        /// </summary>
        protected virtual Symbol Flatten(IDictionary<Sequence, Sequence> map, IDictionary<Sequence, IList<Fixup>> map2)
        {
            return this;
        }

        /// <summary>
        /// Returns the flattened size.
        /// </summary>
        public virtual int FlattenedSize()
        {
            return 1;
        }

        /// <summary>
        /// Flattens the given sub-array of symbols into an sub-array of symbols. Every
        /// <tt>Sequence</tt> in the input are replaced by its production recursively.
        /// Non-<tt>Sequence</tt> symbols, they internally have other symbols those
        /// internal symbols also get flattened. When flattening is done, the only place
        /// there might be Sequence symbols is in the productions of a Repeater,
        /// Alternative, or the symToParse and symToSkip in a UnionAdjustAction or
        /// SkipAction.
        ///
        /// Why is this done? We want our parsers to be fast. If we left the grammars
        /// unflattened, then the parser would be constantly copying the contents of
        /// nested Sequence productions onto the parsing stack. Instead, because of
        /// flattening, we have a long top-level production with no Sequences unless the
        /// Sequence is absolutely needed, e.g., in the case of a Repeater or an
        /// Alternative.
        ///
        /// Well, this is not exactly true when recursion is involved. Where there is a
        /// recursive record, that record will be "inlined" once, but any internal (ie,
        /// recursive) references to that record will be a Sequence for the record. That
        /// Sequence will not further inline itself -- it will refer to itself as a
        /// Sequence. The same is true for any records nested in this outer recursive
        /// record. Recursion is rare, and we want things to be fast in the typical case,
        /// which is why we do the flattening optimization.
        ///
        ///
        /// The algorithm does a few tricks to handle recursive symbol definitions. In
        /// order to avoid infinite recursion with recursive symbols, we have a map of
        /// Symbol->Symbol. Before fully constructing a flattened symbol for a
        /// <tt>Sequence</tt> we insert an empty output symbol into the map and then
        /// start filling the production for the <tt>Sequence</tt>. If the same
        /// <tt>Sequence</tt> is encountered due to recursion, we simply return the
        /// (empty) output <tt>Sequence</tt> from the map. Then we actually fill out
        /// the production for the <tt>Sequence</tt>. As part of the flattening process
        /// we copy the production of <tt>Sequence</tt>s into larger arrays. If the
        /// original <tt>Sequence</tt> has not not be fully constructed yet, we copy a
        /// bunch of <tt>null</tt>s. Fix-up remembers all those <tt>null</tt> patches.
        /// The fix-ups gets finally filled when we know the symbols to occupy those
        /// patches.
        /// </summary>
        /// <param name="input">    The array of input symbols to flatten </param>
        /// <param name="start"> The position where the input sub-array starts. </param>
        /// <param name="output">   The output that receives the flattened list of symbols. The
        ///              output array should have sufficient space to receive the
        ///              expanded sub-array of symbols. </param>
        /// <param name="skip">  The position where the output input sub-array starts. </param>
        /// <param name="map">   A map of symbols which have already been expanded. Useful for
        ///              handling recursive definitions and for caching. </param>
        /// <param name="map2">  A map to to store the list of fix-ups. </param>
        protected static void Flatten(Symbol[] input, int start, Symbol[] output, int skip,
            IDictionary<Sequence, Sequence> map, IDictionary<Sequence, IList<Fixup>> map2)
        {
            for (int i = start, j = skip; i < input.Length; i++)
            {
                Symbol s = input[i].Flatten(map, map2);
                if (s is Sequence)
                {
                    Symbol[] p = s.Production;
                    IList<Fixup> l;
                    if (!map2.TryGetValue((Sequence)s, out l))
                    {
                        Array.Copy(p, 0, output, j, p.Length);
                        // Copy any fixups that will be applied to p to add missing symbols
                        foreach (IList<Fixup> fixups in map2.Values)
                        {
                            copyFixups(fixups, output, j, p);
                        }
                    }
                    else
                    {
                        l.Add(new Fixup(output, j));
                    }

                    j += p.Length;
                }
                else
                {
                    output[j++] = s;
                }
            }
        }

        private static void copyFixups(IList<Fixup> fixups, Symbol[] output, int outPos, Symbol[] toCopy)
        {
            for (int i = 0, n = fixups.Count; i < n; i += 1)
            {
                Fixup fixup = fixups[i];
                if (fixup.Symbols == toCopy)
                {
                    fixups.Add(new Fixup(output, fixup.Pos + outPos));
                }
            }
        }

        /// <summary>
        /// Returns the amount of space required to flatten the given sub-array of
        /// symbols.
        /// </summary>
        /// <param name="symbols"> The array of input symbols. </param>
        /// <param name="start">   The index where the subarray starts. </param>
        /// <returns> The number of symbols that will be produced if one expands the given
        ///         input. </returns>
        protected static int FlattenedSize(Symbol[] symbols, int start)
        {
            int result = 0;
            for (int i = start; i < symbols.Length; i++)
            {
                if (symbols[i] is Sequence)
                {
                    Sequence s = (Sequence)symbols[i];
                    result += s.FlattenedSize();
                }
                else
                {
                    result += 1;
                }
            }

            return result;
        }

        /// <summary>
        /// Terminal symbol.
        /// </summary>
        protected class Terminal : Symbol
        {
            /// <summary>
            /// Printable name.
            /// </summary>
            public readonly string PrintName;

            /// <summary>
            /// Initializes a new instance of the <see cref="Symbol.Terminal"/> class.
            /// </summary>
            public Terminal(string printName) : base(Kind.Terminal)
            {
                this.PrintName = printName;
            }

            /// <inheritdoc />
            public override string ToString()
            {
                return PrintName;
            }
        }

        /// <summary>
        /// Implicit action.
        /// </summary>
        public class ImplicitAction : Symbol
        {
            /// <summary>
            /// Set to <tt>true</tt> if and only if this implicit action is a trailing
            /// action. That is, it is an action that follows real symbol. E.g
            /// <seealso cref="Symbol.DefaultEndAction"/>.
            /// </summary>
            public readonly bool IsTrailing;

            /// <summary>
            /// Initializes a new instance of the <see cref="Symbol.ImplicitAction"/> class.
            /// </summary>
            public ImplicitAction() : this(false)
            {
            }

            /// <summary>
            /// Initializes a new instance of the <see cref="Symbol.ImplicitAction"/> class.
            /// </summary>
            public ImplicitAction(bool isTrailing) : base(Kind.ImplicitAction)
            {
                this.IsTrailing = isTrailing;
            }
        }

        /// <summary>
        /// Root symbol.
        /// </summary>
        protected class Root : Symbol
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="Symbol.Root"/> class.
            /// </summary>
            public Root(params Symbol[] symbols) : base(Kind.Root, makeProduction(symbols))
            {
                Production[0] = this;
            }

            private static Symbol[] makeProduction(Symbol[] symbols)
            {
                Symbol[] result = new Symbol[FlattenedSize(symbols, 0) + 1];
                Flatten(symbols, 0, result, 1, new Dictionary<Sequence, Sequence>(),
                    new Dictionary<Sequence, IList<Fixup>>());
                return result;
            }
        }

        /// <summary>
        /// Sequence symbol.
        /// </summary>
        protected class Sequence : Symbol, IEnumerable<Symbol>
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="Symbol.Sequence"/> class.
            /// </summary>
            public Sequence(Symbol[] productions) : base(Kind.Sequence, productions)
            {
            }

            /// <summary>
            /// Get the symbol at the given index.
            /// </summary>
            public virtual Symbol Get(int index)
            {
                return Production[index];
            }

            /// <summary>
            /// Returns the number of symbols.
            /// </summary>
            public virtual int Size()
            {
                return Production.Length;
            }

            /// <inheritdoc />
            public IEnumerator<Symbol> GetEnumerator()
            {
                return Enumerable.Reverse(Production).GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return this.GetEnumerator();
            }

            /// <inheritdoc />
            protected override Symbol Flatten(IDictionary<Sequence, Sequence> map,
                IDictionary<Sequence, IList<Fixup>> map2)
            {
                Sequence result;
                if (!map.TryGetValue(this, out result))
                {
                    result = new Sequence(new Symbol[FlattenedSize()]);
                    map[this] = result;
                    IList<Fixup> l = new List<Fixup>();
                    map2[result] = l;

                    Flatten(Production, 0, result.Production, 0, map, map2);
                    foreach (Fixup f in l)
                    {
                        Array.Copy(result.Production, 0, f.Symbols, f.Pos, result.Production.Length);
                    }

                    map2.Remove(result);
                }

                return result;
            }

            /// <inheritdoc />
            public override int FlattenedSize()
            {
                return FlattenedSize(Production, 0);
            }
        }

        /// <summary>
        /// Repeater symbol.
        /// </summary>
        public class Repeater : Symbol
        {
            /// <summary>
            /// The end symbol.
            /// </summary>
            public readonly Symbol End;

            /// <summary>
            /// Initializes a new instance of the <see cref="Symbol.Repeater"/> class.
            /// </summary>
            public Repeater(Symbol end, params Symbol[] sequenceToRepeat) : base(Kind.Repeater,
                makeProduction(sequenceToRepeat))
            {
                this.End = end;
                Production[0] = this;
            }

            private static Symbol[] makeProduction(Symbol[] p)
            {
                Symbol[] result = new Symbol[p.Length + 1];
                Array.Copy(p, 0, result, 1, p.Length);
                return result;
            }

            /// <inheritdoc />
            protected override Symbol Flatten(IDictionary<Sequence, Sequence> map,
                IDictionary<Sequence, IList<Fixup>> map2)
            {
                Repeater result = new Repeater(End, new Symbol[FlattenedSize(Production, 1)]);
                Flatten(Production, 1, result.Production, 1, map, map2);
                return result;
            }
        }

        /// <summary>
        /// Returns true if the Parser contains any Error symbol, indicating that it may
        /// fail for some inputs.
        /// </summary>
        private static bool hasErrors(Symbol symbol)
        {
            return hasErrors(symbol, new HashSet<Symbol>());
        }

        private static bool hasErrors(Symbol symbol, ISet<Symbol> visited)
        {
            // avoid infinite recursion
            if (visited.Contains(symbol))
            {
                return false;
            }

            visited.Add(symbol);

            switch (symbol.SymKind)
            {
                case Kind.Alternative:
                    return hasErrors(symbol, ((Alternative)symbol).Symbols, visited);
                case Kind.ExplicitAction:
                    return false;
                case Kind.ImplicitAction:
                    if (symbol is ErrorAction)
                    {
                        return true;
                    }

                    if (symbol is UnionAdjustAction)
                    {
                        return hasErrors(((UnionAdjustAction)symbol).SymToParse, visited);
                    }

                    return false;
                case Kind.Repeater:
                    Repeater r = (Repeater)symbol;
                    return hasErrors(r.End, visited) || hasErrors(symbol, r.Production, visited);
                case Kind.Root:
                case Kind.Sequence:
                    return hasErrors(symbol, symbol.Production, visited);
                case Kind.Terminal:
                    return false;
                default:
                    throw new Exception("unknown symbol kind: " + symbol.SymKind);
            }
        }

        private static bool hasErrors(Symbol root, Symbol[] symbols, ISet<Symbol> visited)
        {
            if (null != symbols)
            {
                foreach (Symbol s in symbols)
                {
                    if (s == root)
                    {
                        continue;
                    }

                    if (hasErrors(s, visited))
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        /// <summary>
        /// Alternative symbol.
        /// </summary>
        public class Alternative : Symbol
        {
            /// <summary>
            /// The symbols.
            /// </summary>
            public readonly Symbol[] Symbols;
            /// <summary>
            /// The labels.
            /// </summary>
            public readonly string[] Labels;

            /// <summary>
            /// Initializes a new instance of the <see cref="Symbol.Alternative"/> class.
            /// </summary>
            public Alternative(Symbol[] symbols, string[] labels) : base(Kind.Alternative)
            {
                this.Symbols = symbols;
                this.Labels = labels;
            }

            /// <summary>
            /// Returns the symbol at the given index.
            /// </summary>
            public virtual Symbol GetSymbol(int index)
            {
                return Symbols[index];
            }

            /// <summary>
            /// Returns the label at the given index.
            /// </summary>
            public virtual string GetLabel(int index)
            {
                return Labels[index];
            }

            /// <summary>
            /// Returns the size.
            /// </summary>
            public virtual int Size()
            {
                return Symbols.Length;
            }

            /// <summary>
            /// Returns the index of the given label.
            /// </summary>
            public virtual int FindLabel(string label)
            {
                if (!ReferenceEquals(label, null))
                {
                    for (int i = 0; i < Labels.Length; i++)
                    {
                        if (label.Equals(Labels[i]))
                        {
                            return i;
                        }
                    }
                }

                return -1;
            }

            /// <inheritdoc />
            protected override Symbol Flatten(IDictionary<Sequence, Sequence> map,
                IDictionary<Sequence, IList<Fixup>> map2)
            {
                Symbol[] ss = new Symbol[Symbols.Length];
                for (int i = 0; i < ss.Length; i++)
                {
                    ss[i] = Symbols[i].Flatten(map, map2);
                }

                return new Alternative(ss, Labels);
            }
        }

        /// <summary>
        /// The error action.
        /// </summary>
        public class ErrorAction : ImplicitAction
        {
            /// <summary>
            /// The error message.
            /// </summary>
            public readonly string Msg;

            /// <summary>
            /// Initializes a new instance of the <see cref="Symbol.ErrorAction"/> class.
            /// </summary>
            public ErrorAction(string msg)
            {
                this.Msg = msg;
            }
        }

        /// <summary>
        /// Int check action.
        /// </summary>
        public class IntCheckAction : Symbol
        {
            /// <summary>
            /// The size.
            /// </summary>
            public readonly int Size;

            /// <summary>
            /// Initializes a new instance of the <see cref="Symbol.IntCheckAction"/> class.
            /// </summary>
            public IntCheckAction(int size) : base(Kind.ExplicitAction)
            {
                this.Size = size;
            }
        }

        /// <summary>
        /// The enum adjust action.
        /// </summary>
        public class EnumAdjustAction : IntCheckAction
        {
            /// <summary>
            /// Whether no adjustments are needed.
            /// </summary>
            public readonly bool NoAdjustments;
            /// <summary>
            /// The adjustments.
            /// </summary>
            public readonly object[] Adjustments;

            /// <summary>
            /// Initializes a new instance of the <see cref="Symbol.EnumAdjustAction"/> class.
            /// </summary>
            public EnumAdjustAction(int rsymCount, object[] adjustments) : base(rsymCount)
            {
                this.Adjustments = adjustments;
                bool noAdj = true;
                if (adjustments != null)
                {
                    int count = Math.Min(rsymCount, adjustments.Length);
                    noAdj = (adjustments.Length <= rsymCount);
                    for (int i = 0; noAdj && i < count; i++)
                    {
                        noAdj &= ((adjustments[i] is int) && i == ((int?)adjustments[i]).Value);
                    }
                }

                this.NoAdjustments = noAdj;
            }
        }

        /// <summary>
        /// The writer union action.
        /// </summary>
        public class WriterUnionAction : ImplicitAction
        {
        }

        /// <summary>
        /// The resolving action.
        /// </summary>
        public class ResolvingAction : ImplicitAction
        {
            /// <summary>
            /// The writer.
            /// </summary>
            public readonly Symbol Writer;
            /// <summary>
            /// The reader.
            /// </summary>
            public readonly Symbol Reader;

            /// <summary>
            /// Initializes a new instance of the <see cref="Symbol.ResolvingAction"/> class.
            /// </summary>
            public ResolvingAction(Symbol writer, Symbol reader)
            {
                this.Writer = writer;
                this.Reader = reader;
            }

            /// <inheritdoc />
            protected override Symbol Flatten(IDictionary<Sequence, Sequence> map,
                IDictionary<Sequence, IList<Fixup>> map2)
            {
                return new ResolvingAction(Writer.Flatten(map, map2), Reader.Flatten(map, map2));
            }
        }

        /// <summary>
        /// The skip action.
        /// </summary>
        public class SkipAction : ImplicitAction
        {
            /// <summary>
            /// The symbol to skip.
            /// </summary>
            public readonly Symbol SymToSkip;

            /// <summary>
            /// Initializes a new instance of the <see cref="Symbol.SkipAction"/> class.
            /// </summary>
            public SkipAction(Symbol symToSkip) : base(true)
            {
                this.SymToSkip = symToSkip;
            }

            /// <inheritdoc />
            protected override Symbol Flatten(IDictionary<Sequence, Sequence> map,
                IDictionary<Sequence, IList<Fixup>> map2)
            {
                return new SkipAction(SymToSkip.Flatten(map, map2));
            }
        }

        /// <summary>
        /// The field adjust action.
        /// </summary>
        public class FieldAdjustAction : ImplicitAction
        {
            /// <summary>
            /// The index.
            /// </summary>
            public readonly int RIndex;
            /// <summary>
            /// The field name.
            /// </summary>
            public readonly string FName;
            /// <summary>
            /// The field aliases.
            /// </summary>
            public readonly IList<string> Aliases;

            /// <summary>
            /// Initializes a new instance of the <see cref="Symbol.FieldAdjustAction"/> class.
            /// </summary>
            public FieldAdjustAction(int rindex, string fname, IList<string> aliases)
            {
                this.RIndex = rindex;
                this.FName = fname;
                this.Aliases = aliases;
            }
        }

        /// <summary>
        /// THe field order action.
        /// </summary>
        public sealed class FieldOrderAction : ImplicitAction
        {
            /// <summary>
            /// Whether no reorder is needed.
            /// </summary>
            public readonly bool NoReorder;
            /// <summary>
            /// The fields.
            /// </summary>
            public readonly Field[] Fields;

            /// <summary>
            /// Initializes a new instance of the <see cref="Symbol.FieldOrderAction"/> class.
            /// </summary>
            public FieldOrderAction(Field[] fields)
            {
                this.Fields = fields;
                bool noReorder = true;
                for (int i = 0; noReorder && i < fields.Length; i++)
                {
                    noReorder &= (i == fields[i].Pos);
                }

                this.NoReorder = noReorder;
            }
        }

        /// <summary>
        /// The default start action.
        /// </summary>
        public class DefaultStartAction : ImplicitAction
        {
            /// <summary>
            /// The contents.
            /// </summary>
            public readonly byte[] Contents;

            /// <summary>
            /// Initializes a new instance of the <see cref="Symbol.DefaultStartAction"/> class.
            /// </summary>
            public DefaultStartAction(byte[] contents)
            {
                this.Contents = contents;
            }
        }

        /// <summary>
        /// The union adjust action.
        /// </summary>
        public class UnionAdjustAction : ImplicitAction
        {
            /// <summary>
            /// The index.
            /// </summary>
            public readonly int RIndex;
            /// <summary>
            /// The symbol to parser.
            /// </summary>
            public readonly Symbol SymToParse;

            /// <summary>
            /// Initializes a new instance of the <see cref="Symbol.UnionAdjustAction"/> class.
            /// </summary>
            public UnionAdjustAction(int rindex, Symbol symToParse)
            {
                this.RIndex = rindex;
                this.SymToParse = symToParse;
            }

            /// <inheritdoc />
            protected override Symbol Flatten(IDictionary<Sequence, Sequence> map,
                IDictionary<Sequence, IList<Fixup>> map2)
            {
                return new UnionAdjustAction(RIndex, SymToParse.Flatten(map, map2));
            }
        }

        /// <summary>
        /// The enum labels action.
        /// </summary>
        public class EnumLabelsAction : IntCheckAction
        {
            /// <summary>
            /// The symbols.
            /// </summary>
            public readonly IList<string> Symbols;

            /// <summary>
            /// Initializes a new instance of the <see cref="Symbol.EnumLabelsAction"/> class.
            /// </summary>
            public EnumLabelsAction(IList<string> symbols) : base(symbols.Count)
            {
                this.Symbols = symbols;
            }

            /// <summary>
            /// Returns the label at the given index.
            /// </summary>
            public virtual string GetLabel(int n)
            {
                return Symbols[n];
            }

            /// <summary>
            /// Returns index of the given label.
            /// </summary>
            public virtual int FindLabel(string l)
            {
                if (!ReferenceEquals(l, null))
                {
                    for (int i = 0; i < Symbols.Count; i++)
                    {
                        if (l.Equals(Symbols[i]))
                        {
                            return i;
                        }
                    }
                }

                return -1;
            }
        }

        /// <summary>
        /// The terminal symbols for the grammar.
        /// </summary>
        public static readonly Symbol Null = new Terminal("null");

        /// <summary>
        /// Boolean
        /// </summary>
        public static readonly Symbol Boolean = new Terminal("boolean");
        /// <summary>
        /// Int
        /// </summary>
        public static readonly Symbol Int = new Terminal("int");
        /// <summary>
        /// Long
        /// </summary>
        public static readonly Symbol Long = new Terminal("long");
        /// <summary>
        /// Float
        /// </summary>
        public static readonly Symbol Float = new Terminal("float");
        /// <summary>
        /// Double
        /// </summary>
        public static readonly Symbol Double = new Terminal("double");
        /// <summary>
        /// String
        /// </summary>
        public static readonly Symbol String = new Terminal("string");
        /// <summary>
        /// Bytes
        /// </summary>
        public static readonly Symbol Bytes = new Terminal("bytes");
        /// <summary>
        /// Fixed
        /// </summary>
        public static readonly Symbol Fixed = new Terminal("fixed");
        /// <summary>
        /// Enum
        /// </summary>
        public static readonly Symbol Enum = new Terminal("enum");
        /// <summary>
        /// Union
        /// </summary>
        public static readonly Symbol Union = new Terminal("union");

        /// <summary>
        /// ArrayStart
        /// </summary>
        public static readonly Symbol ArrayStart = new Terminal("array-start");
        /// <summary>
        /// ArrayEnd
        /// </summary>
        public static readonly Symbol ArrayEnd = new Terminal("array-end");
        /// <summary>
        /// MapStart
        /// </summary>
        public static readonly Symbol MapStart = new Terminal("map-start");
        /// <summary>
        /// MapEnd
        /// </summary>
        public static readonly Symbol MapEnd = new Terminal("map-end");
        /// <summary>
        /// ItemEnd
        /// </summary>
        public static readonly Symbol ItemEnd = new Terminal("item-end");

        /// <summary>
        /// WriterUnion
        /// </summary>
        public static readonly Symbol WriterUnion = new WriterUnionAction();

        /// <summary>
        /// FieldAction - a pseudo terminal used by parsers
        /// </summary>
        public static readonly Symbol FieldAction = new Terminal("field-action");

        /// <summary>
        /// RecordStart
        /// </summary>
        public static readonly Symbol RecordStart = new ImplicitAction(false);
        /// <summary>
        /// RecordEnd
        /// </summary>
        public static readonly Symbol RecordEnd = new ImplicitAction(true);
        /// <summary>
        /// UnionEnd
        /// </summary>
        public static readonly Symbol UnionEnd = new ImplicitAction(true);
        /// <summary>
        /// FieldEnd
        /// </summary>
        public static readonly Symbol FieldEnd = new ImplicitAction(true);

        /// <summary>
        /// DefaultEndAction
        /// </summary>
        public static readonly Symbol DefaultEndAction = new ImplicitAction(true);
        /// <summary>
        /// MapKeyMarker
        /// </summary>
        public static readonly Symbol MapKeyMarker = new Terminal("map-key-marker");
    }
}
