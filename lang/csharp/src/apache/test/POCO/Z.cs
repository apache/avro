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

 using System.Collections.Generic;

namespace Avro.Test
{
    public class Z
    {
        public int? myUInt { get; set; }

        public long? myULong { get; set; }

        public bool? myUBool { get; set; }

        public double? myUDouble { get; set; }

        public float? myUFloat { get; set; }

        public byte[] myUBytes { get; set; }

        public string myUString { get; set; }

        public int myInt { get; set; }

        public long myLong { get; set; }

        public bool myBool { get; set; }

        public double myDouble { get; set; }

        public float myFloat { get; set; }

        public byte[] myBytes { get; set; }

        public string myString { get; set; }

        public object myNull { get; set; }

        public byte[] myFixed { get; set; }

        public A myA { get; set; }

        public MyEnum myE { get; set; }

        public List<byte[]> myArray { get; set; }

        public List<newRec> myArray2 { get; set; }

        public Dictionary<string, string> myMap { get; set; }

        public Dictionary<string, newRec> myMap2 { get; set; }

        public object myObject { get; set; }

        public List<List<object>> myArray3 { get; set; }
    }
}
