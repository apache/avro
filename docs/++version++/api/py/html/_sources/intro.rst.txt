.. Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at
   
       https://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

Welcome to Avro's Python documentation!
=======================================

Avro is a data serialization system. See `avro.apache.org <https://avro.apache.org/docs/current/>`_ for background information.

Avro Python is a Python library that implements parts of the `Avro Specification <https://avro.apache.org/docs/current/specification/>`_.

The library includes the following functionality:

* Assembling schemas programmatically.
* A schema parser, which can parse Avro schema (written in JSON) into a Schema object.
* Binary encoders and decoders to encode data into Avro format and decode it back using primitive functions.
* Streams for storing and reading data, which Encoders and Decoders use.
* Support for Avro DataFile.
