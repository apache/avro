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
using Avro.POCO;

namespace Avro.Test
{
    [Avro(true)]
    public class LogMessage
    {
        private Dictionary<string, string> _tags = new Dictionary<string, string>();

        [AvroField(0)]
        public string IP { get; set; }

        [AvroField(1)]
        public string Message { get; set; }

        [AvroField(2, typeof(DateTimeOffsetToLongConverter))]
        public DateTimeOffset TimeStamp { get; set; }

        [AvroField(3)]
        public Dictionary<string, string> Tags { get => _tags; set => _tags = value; }

        [AvroField(4)]
        public MessageTypes Severity { get; set; }
    }
}
