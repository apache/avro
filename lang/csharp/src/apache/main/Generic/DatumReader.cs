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
using Avro.IO;

namespace Avro.Generic
{
    public interface DatumReader<T>
    {
        Schema ReaderSchema { get; }
        Schema WriterSchema { get; }

        /// <summary>
        /// Read a datum.  Traverse the schema, depth-first, reading all leaf values
        /// in the schema into a datum that is returned.  If the provided datum is
        /// non-null it may be reused and returned.
        /// </summary>        
        T Read(T reuse, Decoder decoder);
    }

    ///// <summary>
    ///// Deserialize Avro-encoded data into a .net data structure.
    ///// </summary>
    //public class DatumReader
    //{
    //    public Schema WriterSchema { get; private set; }
    //    public Schema ReaderSchema { get; private set; }

    //    /// <summary>
    //    /// As defined in the Avro specification, we call the schema encoded
    //    /// in the data the "writer's schema", and the schema expected by the
    //    /// reader the "reader's schema".
    //    /// </summary>
    //    /// <param name="writerSchema"></param>
    //    /// <param name="readerSchema"></param>
    //    public DatumReader(Schema writerSchema, Schema readerSchema)
    //    {
    //        if (null == writerSchema) throw new ArgumentNullException("writerSchema", "writerSchema cannot be null.");
    //        if (null == readerSchema) throw new ArgumentNullException("readerSchema", "readerSchema cannot be null.");

    //        this.WriterSchema = writerSchema;
    //        this.ReaderSchema = readerSchema;
    //    }


    //    static bool checkProps(Schema a, Schema b, params string[] props)
    //    {
    //        foreach (string prop in props)
    //        {
    //            if (!string.Equals(a[prop], b[prop]))
    //                return false;
    //        }

    //        return true;
    //    }
    //    //static bool CheckProps(Schema schema_one, Schema schema_two, IEnumerable<string> props)
    //    //{

    //    //}

    //    static bool matchSchemas(Schema writers_schema, Schema readers_schema)
    //    {
    //        string w_type = writers_schema.Type, r_type = readers_schema.Type;

    //        if (string.Equals(Schema.UNION, w_type) || string.Equals(Schema.UNION, r_type))
    //            return true;
    //        else if (PrimitiveSchema.PrimitiveKeyLookup.ContainsKey(w_type) && PrimitiveSchema.PrimitiveKeyLookup.ContainsKey(r_type) && w_type == r_type)
    //            return true;
    //        else if (w_type == Schema.RECORD && r_type == Schema.RECORD && DatumReader.checkProps(writers_schema, readers_schema, "fullname"))
    //            return true;
    //        else if (w_type == "error" && r_type == "error" && DatumReader.checkProps(writers_schema, readers_schema, "fullname"))
    //            return true;
    //        else if (w_type == "request" && r_type == "request")
    //            return true;
    //        else if (w_type == Schema.FIXED && r_type == Schema.FIXED && DatumReader.checkProps(writers_schema, readers_schema, "fullname", "size"))
    //            return true;
    //        else if (w_type == Schema.ENUM && r_type == Schema.ENUM && DatumReader.checkProps(writers_schema, readers_schema, "fullname"))
    //            return true;
    //        //else if (w_type == Schema.MAP && r_type == Schema.MAP && DatumReader.CheckProps(writers_schema.values, readers_schema.values, "type"))
    //        //    return true;
    //        //else if (w_type == Schema.ARRAY && r_type == Schema.ARRAY && DatumReader.check_props(writers_schema.items, readers_schema.items, "type"))
    //        //    return true;
    //        else if (w_type == Schema.INT && Util.checkIsValue(r_type, Schema.LONG, Schema.FLOAT, Schema.DOUBLE))
    //            return true;
    //        else if (w_type == Schema.LONG && Util.checkIsValue(r_type, Schema.FLOAT, Schema.DOUBLE))
    //            return true;
    //        else if (w_type == Schema.FLOAT && r_type == Schema.DOUBLE)
    //            return true;

    //        if (Util.checkIsValue(w_type, Schema.MAP, Schema.ARRAY))
    //            throw new NotImplementedException(w_type);

    //        return false;
    //    }

    //    public object Read(BinaryDecoder decoder)
    //    {
    //        if (null == this.ReaderSchema)
    //            this.ReaderSchema = this.WriterSchema;

    //        return ReadData(this.WriterSchema, this.ReaderSchema, decoder);

    //    }

    //    private object ReadData(Schema writers_schema, Schema readers_schema, BinaryDecoder decoder)
    //    {
    //        if (!matchSchemas(writers_schema, readers_schema))
    //            throw new SchemaResolutionException("Schemas do not match.", writers_schema, readers_schema);

    //        if (writers_schema.Type != Schema.UNION && readers_schema.Type == Schema.UNION)
    //        {
    //            foreach (Schema s in ((UnionSchema)readers_schema).Schemas)
    //            {
    //                if (DatumReader.matchSchemas(writers_schema, s))
    //                {
    //                    return ReadData(writers_schema, s, decoder);
    //                }
    //            }

    //            throw new SchemaResolutionException("Schemas do not match.", writers_schema, readers_schema);
    //        }

    //        if (writers_schema.Type == Schema.NULL)
    //            return decoder.ReadNull();
    //        else if (writers_schema.Type == Schema.BOOLEAN)
    //            return decoder.ReadBool();
    //        else if (writers_schema.Type == Schema.STRING)
    //            return decoder.ReadUTF8();
    //        else if (writers_schema.Type == Schema.INT)
    //            return decoder.ReadInt();
    //        else if (writers_schema.Type == Schema.LONG)
    //            return decoder.ReadLong();
    //        else if (writers_schema.Type == Schema.FLOAT)
    //            return decoder.ReadFloat();
    //        else if (writers_schema.Type == Schema.DOUBLE)
    //            return decoder.ReadDouble();
    //        else if (writers_schema.Type == Schema.BYTES)
    //            return decoder.ReadBytes();
    //        else if (writers_schema.Type == Schema.FIXED)
    //            return ReadFixed(writers_schema, readers_schema, decoder);
    //        else if (writers_schema.Type == Schema.ENUM)
    //            return ReadEnum(writers_schema, readers_schema, decoder);
    //        else if (writers_schema.Type == Schema.ARRAY)
    //            return ReadArray(writers_schema, readers_schema, decoder);
    //        else if (writers_schema.Type == Schema.MAP)
    //            return ReadMap(writers_schema, readers_schema, decoder);
    //        else if (writers_schema.Type == Schema.UNION)
    //            return ReadUnion(writers_schema, readers_schema, decoder);
    //        else if (Util.checkIsValue(writers_schema.Type, Schema.RECORD, "error", "request"))
    //            return ReadRecord(writers_schema, readers_schema, decoder);
    //        else
    //            throw new AvroException("Cannot Read unknown type type) " + writers_schema.Type);
    //    }

    //    public void SkipData(Schema writers_schema, BinaryDecoder decoder)
    //    {
    //        if (writers_schema.Type == Schema.NULL)
    //            decoder.SkipNull();
    //        else if (writers_schema.Type == Schema.BOOLEAN)
    //            decoder.SkipBoolean();
    //        else if (writers_schema.Type == Schema.STRING)
    //            decoder.SkipUTF8();
    //        else if (writers_schema.Type == Schema.INT)
    //            decoder.SkipInt();
    //        else if (writers_schema.Type == Schema.LONG)
    //            decoder.SkipLong();
    //        else if (writers_schema.Type == Schema.FLOAT)
    //            decoder.SkipFloat();
    //        else if (writers_schema.Type == Schema.DOUBLE)
    //            decoder.SkipDouble();
    //        else if (writers_schema.Type == Schema.BYTES)
    //            decoder.ReadBytes();
    //        else if (writers_schema.Type == Schema.FIXED)
    //            SkipFixed(writers_schema as FixedSchema, decoder);
    //        else if (writers_schema.Type == Schema.ENUM)
    //            SkipEnum(writers_schema, decoder);
    //        else if (writers_schema.Type == Schema.ARRAY)
    //            SkipArray(writers_schema as ArraySchema, decoder);
    //        else if (writers_schema.Type == Schema.MAP)
    //            SkipMap(writers_schema as MapSchema, decoder);
    //        else if (writers_schema.Type == Schema.UNION)
    //            SkipUnion(writers_schema as UnionSchema, decoder);
    //        else if (Util.checkIsValue(writers_schema.Type, Schema.RECORD, "error", "request"))
    //            SkipRecord(writers_schema as RecordSchema, decoder);
    //        else
    //            throw new AvroException("Unknown type type: %s" + writers_schema.Type);

    //    }

    //    private void SkipRecord(RecordSchema writers_schema, BinaryDecoder decoder)
    //    {
    //        foreach (Field field in writers_schema.Fields)
    //            SkipData(field.Schema, decoder);
    //    }

    //    private void SkipUnion(UnionSchema writers_schema, BinaryDecoder decoder)
    //    {
    //        int index_of_schema = (int)decoder.ReadLong();
    //        SkipData(writers_schema.Schemas[index_of_schema], decoder);
    //    }

    //    private void SkipMap(MapSchema writers_schema, BinaryDecoder decoder)
    //    {
    //        long block_count = decoder.ReadLong();
    //        while (block_count != 0)
    //        {
    //            if (block_count < 0)
    //            {
    //                long block_size = decoder.ReadLong();
    //                decoder.skip(block_size);
    //            }
    //            else
    //            {
    //                for (int i = 0; i < block_count; i++)
    //                {
    //                    decoder.SkipUTF8();
    //                    SkipData(writers_schema.Values, decoder);
    //                    block_count = decoder.ReadLong();
    //                }
    //            }
    //        }
    //    }

    //    private void SkipArray(ArraySchema writers_schema, BinaryDecoder decoder)
    //    {
    //        long block_count = decoder.ReadLong();
    //        while (block_count != 0)
    //        {
    //            if (block_count < 0)
    //            {
    //                long block_size = decoder.ReadLong();
    //                decoder.skip(block_size);
    //            }
    //            else
    //            {
    //                for (int i = 0; i < block_count; i++)
    //                {
    //                    decoder.SkipUTF8();
    //                    SkipData(writers_schema.Items, decoder);
    //                    block_count = decoder.ReadLong();
    //                }
    //            }
    //        }
    //    }

    //    private void SkipEnum(Schema writers_schema, BinaryDecoder decoder)
    //    {
    //        decoder.SkipInt();
    //    }

    //    private void SkipFixed(FixedSchema writers_schema, BinaryDecoder decoder)
    //    {
    //        decoder.skip(writers_schema.Size);
    //    }

    //    /// <summary>
    //    /// A record is encoded by encoding the values of its fields
    //    /// in the order that they are declared. In other words, a record
    //    /// is encoded as just the concatenation of the encodings of its fields.
    //    /// Field values are encoded per their schema.

    //    /// Schema Resolution:
    //    ///  * the ordering of fields may be different: fields are matched by name.
    //    ///  * schemas for fields with the same name in both records are resolved
    //    ///    recursively.
    //    ///  * if the writer's record contains a field with a name not present in the
    //    ///    reader's record, the writer's value for that field is ignored.
    //    ///  * if the reader's record schema has a field that contains a default value,
    //    ///    and writer's schema does not have a field with the same name, then the
    //    ///    reader should use the default value from its field.
    //    ///  * if the reader's record schema has a field with no default value, and 
    //    ///    writer's schema does not have a field with the same name, then the
    //    ///    field's value is unset.
    //    /// </summary>
    //    /// <param name="writers_schema"></param>
    //    /// <param name="readers_schema"></param>
    //    /// <param name="decoder"></param>
    //    /// <returns></returns>
    //    private object ReadRecord(Schema writers_schema, Schema readers_schema, BinaryDecoder decoder)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    private object ReadUnion(Schema writers_schema, Schema readers_schema, BinaryDecoder decoder)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    private object ReadMap(Schema writers_schema, Schema readers_schema, BinaryDecoder decoder)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    private object ReadArray(Schema writers_schema, Schema readers_schema, BinaryDecoder decoder)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    private object ReadEnum(Schema writers_schema, Schema readers_schema, BinaryDecoder decoder)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    private object ReadFixed(Schema writers_schema, Schema readers_schema, BinaryDecoder decoder)
    //    {
    //        throw new NotImplementedException();
    //    }
    //}
}
