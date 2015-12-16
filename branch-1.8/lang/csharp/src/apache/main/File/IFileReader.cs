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
using System.Linq;
using System.Text;

namespace Avro.File
{
    public interface IFileReader<T> : IDisposable
    {
        /// <summary>
        /// Return the header for the input 
        /// file / stream
        /// </summary>
        /// <returns></returns>
        Header GetHeader();

        /// <summary>
        /// Return the schema as read from 
        /// the input file / stream
        /// </summary>
        /// <returns></returns>
        Schema GetSchema();

        /// <summary>
        /// Return the list of keys in the metadata
        /// </summary>
        /// <returns></returns>
        ICollection<string> GetMetaKeys();

        /// <summary>
        /// Return an enumeration of the remaining entries in the file
        /// </summary>
        /// <returns></returns>
        IEnumerable<T> NextEntries { get; }

        /// <summary>
        /// Read the next datum from the file.
        /// </summary>
        T Next();

        /// <summary>
        /// True if more entries remain in this file.
        /// </summary>
        bool HasNext();

        /// <summary>
        /// Return the byte value of a metadata property
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        byte[] GetMeta(string key);

        /// <summary>
        /// Return the long value of a metadata property
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        long GetMetaLong(string key);

        /// <summary>
        /// Return the string value of a metadata property
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        string GetMetaString(string key);

        /// <summary>
        /// Return true if past the next synchronization
        /// point after a position
        /// </summary>
        /// <param name="position"></param>
        /// <returns></returns>
        bool PastSync(long position);

        /// <summary>
        /// Return the last synchronization point before
        /// our current position
        /// </summary>
        /// <returns></returns>
        long PreviousSync();

        /// <summary>
        /// Move to a specific, known synchronization point, 
        /// one returned from IFileWriter.Sync() while writing
        /// </summary>
        /// <param name="position"></param>
        void Seek(long position);

        /// <summary>
        /// Move to the next synchronization point
        /// after a position
        /// </summary>
        /// <param name="position"></param>
        void Sync(long position);

        /// <summary>
        /// Return the current position in the input
        /// </summary>
        /// <returns></returns>
        long Tell();
    }
}
