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
using System.Collections;
using Avro.Generic;
using Avro.IO;

namespace Avro.Specific
{
    /// PreresolvingDatumReader for reading data to ISpecificRecord classes.
    /// <see cref="PreresolvingDatumReader{T}">For more information about performance considerations for choosing this implementation</see>
    public class SpecificDatumReader<T> : PreresolvingDatumReader<T>
    {
        public SpecificDatumReader(Schema writerSchema, Schema readerSchema) : base(writerSchema, readerSchema)
        {
        }

        protected override bool IsReusable(Schema.Type tag)
        {
            switch (tag)
            {
                case Schema.Type.Double:
                case Schema.Type.Boolean:
                case Schema.Type.Int:
                case Schema.Type.Long:
                case Schema.Type.Float:
                case Schema.Type.Bytes:
                case Schema.Type.Enumeration:
                case Schema.Type.String:
                case Schema.Type.Null:
                    return false;
            }
            return true;
        }

        protected override ArrayAccess GetArrayAccess(ArraySchema readerSchema)
        {
            return new SpecificArrayAccess(readerSchema);
        }

        protected override EnumAccess GetEnumAccess(EnumSchema readerSchema)
        {
            return new SpecificEnumAccess();
        }

        protected override MapAccess GetMapAccess(MapSchema readerSchema)
        {
            return new SpecificMapAccess(readerSchema);
        }

        protected override RecordAccess GetRecordAccess(RecordSchema readerSchema)
        {
            if (readerSchema.Name == null)
            {
                // ipc support
                return new GenericDatumReader<T>.GenericRecordAccess(readerSchema);
            }
            return new SpecificRecordAccess(readerSchema);
        }

        protected override FixedAccess GetFixedAccess(FixedSchema readerSchema)
        {
            return new SpecificFixedAccess(readerSchema);
        }

        private static ObjectCreator.CtorDelegate GetConstructor(string name, Schema.Type schemaType)
        {
            var creator = ObjectCreator.Instance;
            return creator.GetConstructor(name, schemaType, creator.GetType(name, schemaType));
        }

        private class SpecificEnumAccess : EnumAccess
        {
            public object CreateEnum(object reuse, int ordinal)
            {
                return ordinal;
            }
        }

        private class SpecificRecordAccess : RecordAccess
        {
            private ObjectCreator.CtorDelegate objCreator;

            public SpecificRecordAccess(RecordSchema readerSchema)
            {
                objCreator = GetConstructor(readerSchema.Fullname, Schema.Type.Record);
            }

            public object CreateRecord(object reuse)
            {
                return reuse ?? objCreator();
            }

            public object GetField(object record, string fieldName, int fieldPos)
            {
                return ((ISpecificRecord)record).Get(fieldPos);
            }

            public void AddField(object record, string fieldName, int fieldPos, object fieldValue)
            {
                ((ISpecificRecord)record).Put(fieldPos, fieldValue);
            }
        }

        private class SpecificFixedAccess : FixedAccess
        {
            private ObjectCreator.CtorDelegate objCreator;

            public SpecificFixedAccess(FixedSchema readerSchema)
            {
                objCreator = GetConstructor(readerSchema.Fullname, Schema.Type.Fixed);
            }

            public object CreateFixed(object reuse)
            {
                return reuse ?? objCreator();
            }

            public byte[] GetFixedBuffer(object rec)
            {
                return ((SpecificFixed) rec).Value;
            }
        }

        private class SpecificArrayAccess : ArrayAccess
        {
            private ObjectCreator.CtorDelegate objCreator;

            public SpecificArrayAccess(ArraySchema readerSchema)
            {
                bool nEnum = false;
                string type = Avro.CodeGen.getType(readerSchema, false, ref nEnum);
                type = type.Remove(0, 6);              // remove IList<
                type = type.Remove(type.Length - 1);   // remove >
        
                objCreator = GetConstructor(type, Schema.Type.Array);
            }

            public object Create(object reuse)
            {
                IList array;

                if( reuse != null )
                {
                    array = reuse as IList;
                    if( array == null )
                        throw new AvroException( "array object does not implement non-generic IList" );
                    // retaining existing behavior where array contents aren't reused
                    // TODO: try to reuse contents?
                    array.Clear();
                }
                else
                    array = objCreator() as IList;
                return array;
            }

            public void EnsureSize(ref object array, int targetSize)
            {
                // no action needed
            }

            public void Resize(ref object array, int targetSize)
            {
                // no action needed
            }

            public void AddElements( object array, int elements, int index, ReadItem itemReader, Decoder decoder, bool reuse )
            {
                var list = (IList)array;
                for (int i=0; i < elements; i++)
                {
                    list.Add( itemReader( null, decoder ) );
                }
            }
        }

        private class SpecificMapAccess : MapAccess
        {
            private ObjectCreator.CtorDelegate objCreator;

            public SpecificMapAccess(MapSchema readerSchema)
            {
                bool nEnum = false;
                string type = Avro.CodeGen.getType(readerSchema, false, ref nEnum);
                type = type.Remove(0, 19);             // remove IDictionary<string,
                type = type.Remove(type.Length - 1);   // remove >
        
                objCreator = GetConstructor(type, Schema.Type.Map);
            }

            public object Create(object reuse)
            {
                IDictionary map;
                if (reuse != null)
                {
                    map = reuse as IDictionary;
                    if (map == null)
                        throw new AvroException("map object does not implement non-generic IList");

                    map.Clear();
                }
                else
                    map = objCreator() as System.Collections.IDictionary;
                return map;
            }

            public void AddElements(object mapObj, int elements, ReadItem itemReader, Decoder decoder, bool reuse)
            {
                var map = ((IDictionary)mapObj);
                for (int i = 0; i < elements; i++)
                {
                    var key = decoder.ReadString();
                    map[key] = itemReader(null, decoder);
                }
            }
        }
    }
}