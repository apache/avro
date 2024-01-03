---
title: "Avro 1.11.3"
linkTitle: "Avro 1.11.3"
date: 2023-09-22
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

The Apache Avro community is pleased to announce the release of Avro 1.11.3!

All signed release artifacts, signatures and verification instructions can
be found <a href="{{< relref "/project/download" >}}">here</a>

This release [addresses 39 Jira issues](https://issues.apache.org/jira/issues/?jql=project%3DAVRO%20AND%20fixVersion%3D1.11.3).

## Highlights

Java
- [AVRO-3789](https://issues.apache.org/jira/browse/AVRO-3789): Comparing maps in GenericData is wrong for certain combinations and fails for empty maps
- [AVRO-3713](https://issues.apache.org/jira/browse/AVRO-3713): Thread scalability problem with the use of SynchronizedMap
- [AVRO-3486](https://issues.apache.org/jira/browse/AVRO-3486): Protocol namespace not parsed correctly if protocol is defined by full name
- [AVRO-2771](https://issues.apache.org/jira/browse/AVRO-2771): Allow having Error in a Record
- [AVRO-3819](https://issues.apache.org/jira/browse/AVRO-3819): Rationalize the system properties that limit allocation

Python
- [AVRO-3819](https://issues.apache.org/jira/browse/AVRO-3819): Rationalize the system properties that limit allocation
- [AVRO-312](https://issues.apache.org/jira/browse/AVRO-312): Generate documentation for Python with Sphinx

Rust
- [AVRO-3853](https://issues.apache.org/jira/browse/AVRO-3853): Support local-timestamp logical types for the Rust SDK
- [AVRO-3851](https://issues.apache.org/jira/browse/AVRO-3851): Validate default value for record fields and enums on parsing
- [AVRO-3847](https://issues.apache.org/jira/browse/AVRO-3847): Record field doesn't accept default value if field type is union and the type of default value is pre-defined name
- [AVRO-3846](https://issues.apache.org/jira/browse/AVRO-3846): Race condition can happen among serde tests
- [AVRO-3838](https://issues.apache.org/jira/browse/AVRO-3838): Replace regex crate with regex-lite
- [AVRO-3837](https://issues.apache.org/jira/browse/AVRO-3837): Disallow invalid namespaces for the Rust binding
- [AVRO-3835](https://issues.apache.org/jira/browse/AVRO-3835): Get rid of byteorder and zerocopy dependencies
- [AVRO-3830](https://issues.apache.org/jira/browse/AVRO-3830): Handle namespace properly if a name starts with dot
- [AVRO-3827](https://issues.apache.org/jira/browse/AVRO-3827): Disallow duplicate field names
- [AVRO-3787](https://issues.apache.org/jira/browse/AVRO-3787): Deserialization fails to use default if an enum in a record in a union is given an unknown symbol
- [AVRO-3786](https://issues.apache.org/jira/browse/AVRO-3786): Deserialization results in FindUnionVariant error if the writer and reader have the same symbol but at different positions
- 

In addition:
- Upgrade dependencies to latest versions, including CVE fixes.
- Testing and build improvements.
- Performance fixes, other bug fixes, better documentation and more.

Known issues: ∅

## Language SDK / Convenience artifacts

* C#: https://www.nuget.org/packages/Apache.Avro/1.11.3
* Java: https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.3/
* Javascript: https://www.npmjs.com/package/avro-js/v/1.11.3
* Perl: https://metacpan.org/release/Avro
* Python 3: https://pypi.org/project/avro/1.11.3
* Ruby: https://rubygems.org/gems/avro/versions/1.11.3
* Rust: https://crates.io/crates/apache-avro/0.16.0

Thanks to everyone for contributing!
