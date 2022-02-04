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

using Avro.IO;

namespace Avro.Generic
{
    /// <summary>
    /// A function that can read the Avro type from the stream.
    /// </summary>
    /// <typeparam name="T">Type to read.</typeparam>
    /// <returns>The read object.</returns>
    public delegate T Reader<T>();

    /// <summary>
    /// A general purpose reader of data from avro streams. This can optionally resolve if the reader's and writer's
    /// schemas are different. This class is a wrapper around DefaultReader and offers a little more type safety. The default reader
    /// has the flexibility to return any type of object for each read call because the Read() method is generic. This
    /// class on the other hand can only return a single type because the type is a parameter to the class. Any
    /// user defined extension should, however, be done to DefaultReader. This class is sealed.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class GenericReader<T> : DatumReader<T>
    {
        private readonly DefaultReader reader;

        /// <summary>
        /// Constructs a generic reader for the given schemas using the DefaultReader. If the
        /// reader's and writer's schemas are different this class performs the resolution.
        /// </summary>
        /// <param name="writerSchema">The schema used while generating the data</param>
        /// <param name="readerSchema">The schema desired by the reader</param>
        public GenericReader(Schema writerSchema, Schema readerSchema)
            : this(new DefaultReader(writerSchema, readerSchema))
        {
        }

        /// <summary>
        /// Constructs a generic reader by directly using the given DefaultReader
        /// </summary>
        /// <param name="reader">The actual reader to use</param>
        public GenericReader(DefaultReader reader)
        {
            this.reader = reader;
        }

        /// <summary>
        /// Schema used to write the data.
        /// </summary>
        public Schema WriterSchema
        { get { return reader.WriterSchema; } }

        /// <summary>
        /// Schema used to read the data.
        /// </summary>
        public Schema ReaderSchema
        { get { return reader.ReaderSchema; } }

        /// <summary>
        /// Reads an object off the stream.
        /// </summary>
        /// <param name="reuse">
        /// If not null, the implementation will try to use to return the object
        /// </param>
        /// <param name="d">Decoder to read from.</param>
        /// <returns>Object we read from the decoder.</returns>
        public T Read(T reuse, Decoder d)
        {
            return reader.Read(reuse, d);
        }
    }
}
