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

namespace Avro.IO.Parsing
{
    /// <summary>
    /// Parser is the class that maintains the stack for parsing. This class is used
    /// by encoders, which are not required to skip.
    /// </summary>
    public class Parser
    {
        /// <summary>
        /// The parser knows how to handle the terminal and non-terminal symbols. But it
        /// needs help from outside to handle implicit and explicit actions. The clients
        /// implement this interface to provide this help.
        /// </summary>
        public interface IActionHandler
        {
            /// <summary>
            /// Handle the action symbol <tt>top</tt> when the <tt>input</tt> is sought to be
            /// taken off the stack.
            /// </summary>
            /// <param name="input"> The input symbol from the caller of Advance </param>
            /// <param name="top">   The symbol at the top the stack. </param>
            /// <returns> <tt>null</tt> if Advance() is to continue processing the stack. If
            ///         not <tt>null</tt> the return value will be returned by Advance(). </returns>
            Symbol DoAction(Symbol input, Symbol top);
        }

        private readonly IActionHandler symbolHandler;
        /// <summary>
        /// Stack of symbols.
        /// </summary>
        protected Symbol[] Stack;
        /// <summary>
        /// Position of the stack.
        /// </summary>
        protected int Pos;

        /// <summary>
        /// Initializes a new instance of the <see cref="Parser"/> class.
        /// </summary>
        public Parser(Symbol root, IActionHandler symbolHandler)
        {
            this.symbolHandler = symbolHandler;
            Stack = new Symbol[5]; // Start small to make sure expansion code works
            Stack[0] = root;
            Pos = 1;
        }

        /// <summary>
        /// If there is no sufficient room in the stack, use this expand it.
        /// </summary>
        private void ExpandStack()
        {
            Array.Resize(ref Stack, Stack.Length + Math.Max(Stack.Length, 1024));
        }

        /// <summary>
        /// Recursively replaces the symbol at the top of the stack with its production,
        /// until the top is a terminal. Then checks if the top symbol matches the
        /// terminal symbol supplied <tt>input</tt>.
        /// </summary>
        /// <param name="input"> The symbol to match against the terminal at the top of the
        ///              stack. </param>
        /// <returns> The terminal symbol at the top of the stack unless an implicit action
        ///         resulted in another symbol, in which case that symbol is returned. </returns>
        public Symbol Advance(Symbol input)
        {
            for (;;)
            {
                Symbol top = Stack[--Pos];
                if (top == input)
                {
                    return top; // A common case
                }

                Symbol.Kind k = top.SymKind;
                if (k == Symbol.Kind.ImplicitAction)
                {
                    Symbol result = symbolHandler.DoAction(input, top);
                    if (result != null)
                    {
                        return result;
                    }
                }
                else if (k == Symbol.Kind.Terminal)
                {
                    throw new AvroTypeException("Attempt to process a " + input + " when a " + top + " was expected.");
                }
                else if (k == Symbol.Kind.Repeater && input == ((Symbol.Repeater)top).End)
                {
                    return input;
                }
                else
                {
                    PushProduction(top);
                }
            }
        }

        /// <summary>
        /// Performs any implicit actions at the top the stack, expanding any production
        /// (other than the root) that may be encountered. This method will fail if there
        /// are any repeaters on the stack.
        /// </summary>
        public void ProcessImplicitActions()
        {
            while (Pos > 1)
            {
                Symbol top = Stack[Pos - 1];
                if (top.SymKind == Symbol.Kind.ImplicitAction)
                {
                    Pos--;
                    symbolHandler.DoAction(null, top);
                }
                else if (top.SymKind != Symbol.Kind.Terminal)
                {
                    Pos--;
                    PushProduction(top);
                }
                else
                {
                    break;
                }
            }
        }

        /// <summary>
        /// Performs any "trailing" implicit actions at the top the stack.
        /// </summary>
        public void ProcessTrailingImplicitActions()
        {
            while (Pos >= 1)
            {
                Symbol top = Stack[Pos - 1];
                if (top.SymKind == Symbol.Kind.ImplicitAction && ((Symbol.ImplicitAction)top).IsTrailing)
                {
                    Pos--;
                    symbolHandler.DoAction(null, top);
                }
                else
                {
                    break;
                }
            }
        }

        /// <summary>
        /// Pushes the production for the given symbol <tt>sym</tt>. If <tt>sym</tt> is a
        /// repeater and <tt>input</tt> is either <see cref="Symbol.ArrayEnd"/> or
        /// <see cref="Symbol.MapEnd"/> pushes nothing.
        /// </summary>
        /// <param name="sym"> </param>
        public void PushProduction(Symbol sym)
        {
            Symbol[] p = sym.Production;
            while (Pos + p.Length > Stack.Length)
            {
                ExpandStack();
            }

            Array.Copy(p, 0, Stack, Pos, p.Length);
            Pos += p.Length;
        }

        /// <summary>
        /// Pops and returns the top symbol from the stack.
        /// </summary>
        public virtual Symbol PopSymbol()
        {
            return Stack[--Pos];
        }

        /// <summary>
        /// Returns the top symbol from the stack.
        /// </summary>
        public virtual Symbol TopSymbol()
        {
            return Stack[Pos - 1];
        }

        /// <summary>
        /// Pushes <tt>sym</tt> on to the stack.
        /// </summary>
        public virtual void PushSymbol(Symbol sym)
        {
            if (Pos == Stack.Length)
            {
                ExpandStack();
            }

            Stack[Pos++] = sym;
        }

        /// <summary>
        /// Returns the depth of the stack.
        /// </summary>
        public virtual int Depth()
        {
            return Pos;
        }

        /// <summary>
        /// Resets the stack.
        /// </summary>
        public virtual void Reset()
        {
            Pos = 1;
        }
    }
}
