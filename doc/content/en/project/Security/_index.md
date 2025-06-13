---
title: "Security"
linkTitle: "Security"
weight: 10
manualLink: https://www.apache.org/security/
---

<!--

 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

-->

Security Policy
===============

Apache Avro project shares the same security policy as
the [Apache Software Foundation](https://www.apache.org/security/).


Security Model
==============

The Avro library implementations are designed to read and write any data conforming
to a schema. Transport is outside the scope of the Avro library: applications using
Avro should be surrounded by security measures that prevent attackers from writing
random data and otherwise interfering with the consumers of schemas.

Although the Avro library will not read or write data except as directed to by
invoking it, avoiding leaking data into a side channel like log files is a non-goal
security-wise for Avro. This means, for example, that you will need to catch and
handle exceptions instead of simply writing them to a log file.

In some cases, like schema parsing, type conversions and based on explicit schema
properties, Avro can execute code provided by the environment. Avro has opt-in
mechanisms for code that is eligible for execution. Applications using Avro should
have a secured supply chain, ensuring code registered to be executed is safe. This
supply chain also includes the schemas being used: if they are user provided,
additional validation is strongly advised.


Summary
-------

In short, using Avro is safe, provided applications:

* are surrounded by security measures that prevent attackers from writing random
  data and otherwise interfering with the consumers of schemas
* avoid leaking data by, for example, catching and handling exceptions
* have a secured supply chain, ensuring code registered to be executed is safe
