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
    public class DataFileConstants
    {
        public const string MetaDataSync = "avro.sync";
        public const string MetaDataCodec = "avro.codec";
        public const string MetaDataSchema = "avro.schema";
        public const string NullCodec = "null";
        public const string DeflateCodec = "deflate";
        public const string MetaDataReserved = "avro";

        public const int Version = 1;
        public static byte[] Magic = { (byte)'O', 
                                       (byte)'b', 
                                       (byte)'j', 
                                       (byte)Version };

        public const int NullCodecHash = 2;
        public const int DeflateCodecHash = 0;

        public const int SyncSize = 16;
        public const int DefaultSyncInterval = 4000 * SyncSize;
    }
}
