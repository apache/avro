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

using System.Diagnostics;

namespace Avro.IO.Parsing
{
    /// <summary>
    /// A parser that capable of skipping as well read and write. This class is used
    /// by decoders who (unlink encoders) are required to implement methods to skip.
    /// </summary>
    public class SkipParser : Parser
    {
        /// <summary>
        /// The clients implement this interface to skip symbols and actions.
        /// </summary>
        public interface SkipHandler
        {
            /// <summary>
            /// Skips the action at the top of the stack.
            /// </summary>
            void SkipAction();

            /// <summary>
            /// Skips the symbol at the top of the stack.
            /// </summary>
            void SkipTopSymbol();
        }

        private readonly SkipHandler skipHandler;

        public SkipParser(Symbol root, ActionHandler symbolHandler, SkipHandler skipHandler) : base(root, symbolHandler)
        {
            this.skipHandler = skipHandler;
        }

        /// <summary>
        /// Skips data by calling <code>skipXyz</code> or <code>readXyz</code> methods on
        /// <code>this</code>, until the parser stack reaches the target level.
        /// </summary>
        public void SkipTo(int target)
        {
            while (target < Pos)
            {
                Symbol top = Stack[Pos - 1];
                while (top.SymKind != Symbol.Kind.Terminal)
                {
                    if (top.SymKind == Symbol.Kind.ImplicitAction || top.SymKind == Symbol.Kind.ExplicitAction)
                    {
                        skipHandler.SkipAction();
                    }
                    else
                    {
                        --Pos;
                        PushProduction(top);
                    }

                    goto outerContinue;
                }

                skipHandler.SkipTopSymbol();
                outerContinue: ;
            }
        }

        /// <summary>
        /// Skips the repeater at the top the stack.
        /// </summary>
        public void SkipRepeater()
        {
            int target = Pos;
            Symbol repeater = Stack[--Pos];
            Debug.Assert(repeater.SymKind == Symbol.Kind.Repeater);
            PushProduction(repeater);
            SkipTo(target);
        }

        /// <summary>
        /// Pushes the given symbol on to the skip and skips it.
        /// </summary>
        /// <param name="symToSkip"> The symbol that should be skipped. </param>
        public void SkipSymbol(Symbol symToSkip)
        {
            int target = Pos;
            PushSymbol(symToSkip);
            SkipTo(target);
        }
    }
}
