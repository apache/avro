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

namespace Avro.File
{
    public interface IFileWriter<T> : IDisposable
    {
        /// <summary>
        /// Append datum to a file / stream
        /// </summary>
        /// <param name="datum"></param>
        void Append(T datum);

        /// <summary>
        /// Closes the file / stream
        /// </summary>
        void Close();

        /// <summary>
        /// Flush out any buffered data
        /// </summary>
        void Flush();

        /// Returns true if parameter is a
        /// reserved Avro meta data value
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        bool IsReservedMeta(string key);

        /// <summary>
        /// Set meta data pair
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        void SetMeta(String key, byte[] value);

        /// <summary>
        /// Set meta data pair (long value)
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        void SetMeta(String key, long value);

        /// <summary>
        /// Set meta data pair (string value)
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        void SetMeta(String key, string value);

        /// <summary>
        /// Set the synchronization interval for this 
        /// file / stream, in bytes. Valid values range 
        /// from 32 to 2^30. Suggested values are 
        /// between 2K and 2M
        /// </summary>
        /// <param name="syncInterval"></param>
        /// <returns></returns>
        void SetSyncInterval(int syncInterval);

        /// <summary>
        /// Forces the end of the current block, 
        /// emitting a synchronization marker
        /// </summary>
        /// <returns></returns>
        long Sync();
    }
}
