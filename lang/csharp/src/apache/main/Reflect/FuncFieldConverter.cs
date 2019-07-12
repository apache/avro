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
using Avro.Generic;

namespace Avro.Reflect
{
    public class FuncFieldConverter<A, P> : TypedFieldConverter<A, P>
    {
        public FuncFieldConverter(Func<A, Schema, P> from, Func<P, Schema, A> to)
        {
            _from = from;
            _to = to;
        }

        private Func<A, Schema, P> _from;

        private Func<P, Schema, A> _to;

        public override P From(A o, Schema s)
        {
            return _from(o, s);
        }

        public override A To(P o, Schema s)
        {
            return _to(o, s);
        }
    }
}
