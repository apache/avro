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
using System.Reflection;

namespace Avro.Reflect
{
    abstract public class TypedFieldConverter<A,P> : IAvroFieldConverter
    {
        public abstract P From(A o, Schema s);
        public abstract A To(P o, Schema s);

        public object FromAvroType(object o, Schema s)
        {
            if(!typeof(A).IsAssignableFrom(o.GetType()))
            {
                throw new AvroException($"Converter from {typeof(A).Name} to {typeof(P).Name} cannot convert object of type {o.GetType().Name} to {typeof(A).Name}, object {o.ToString()}");
            }
            return From((A)o, s);
        }

        public Type GetAvroType()
        {
            return typeof(A);
        }

        public Type GetPropertyType()
        {
            return typeof(P);
        }

        public object ToAvroType(object o, Schema s)
        {
            if(!typeof(P).IsAssignableFrom(o.GetType()))
            {
                throw new AvroException($"Converter from {typeof(A).Name} to {typeof(P).Name} cannot convert object of type {o.GetType().Name} to {typeof(P).Name}, object {o.ToString()}");
            }
            return To((P)o, s);
        }
    }
}
