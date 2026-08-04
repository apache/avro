---
title: "Avro 1.11.2"
linkTitle: "Avro 1.11.2"
date: 2023-07-03
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

The Apache Avro community is pleased to announce the release of Avro 1.11.2!

All signed release artifacts, signatures and verification instructions can
be found <a href="{{< relref "/project/download" >}}">here</a>

This release addresses 89 [Avro JIRA](https://issues.apache.org/jira/issues/?jql=project%3DAVRO%20AND%20fixVersion%3D1.11.2).

## Highlights

C#
- [AVRO-3434](https://issues.apache.org/jira/browse/AVRO-3434): Support logical schemas in reflect reader and writer
- [AVRO-3670](https://issues.apache.org/jira/browse/AVRO-3670): Add NET 7.0 support
- [AVRO-3724](https://issues.apache.org/jira/browse/AVRO-3724): Fix C# JsonEncoder for nested array of records
- [AVRO-3756](https://issues.apache.org/jira/browse/AVRO-3756): Add a method to return types instead of writing them to disk

C++
- [AVRO-3601](https://issues.apache.org/jira/browse/AVRO-3601): C++ API header contains breaking include
- [AVRO-3705](https://issues.apache.org/jira/browse/AVRO-3705): C++17 support

Java
- [AVRO-2943](https://issues.apache.org/jira/browse/AVRO-2943): Add new GenericData String/Utf8 ARRAY comparison test
- [AVRO-2943](https://issues.apache.org/jira/browse/AVRO-2943): improve GenericRecord MAP type comparison
- [AVRO-3473](https://issues.apache.org/jira/browse/AVRO-3473): Use ServiceLoader to discover Conversion
- [AVRO-3536](https://issues.apache.org/jira/browse/AVRO-3536): Inherit conversions for Union type
- [AVRO-3597](https://issues.apache.org/jira/browse/AVRO-3597): Allow custom readers to override string creation
- [AVRO-3560](https://issues.apache.org/jira/browse/AVRO-3560): Throw SchemaParseException on dangling content beyond end of schema
- [AVRO-3602](https://issues.apache.org/jira/browse/AVRO-3602): Support Map(with non-String keys) and Set in ReflectDatumReader
- [AVRO-3676](https://issues.apache.org/jira/browse/AVRO-3676): Produce valid toString() for UUID JSON
- [AVRO-3698](https://issues.apache.org/jira/browse/AVRO-3698): SpecificData.getClassName must replace reserved words
- [AVRO-3700](https://issues.apache.org/jira/browse/AVRO-3700): Publish Java SBOM artifacts with CycloneDX
- [AVRO-3783](https://issues.apache.org/jira/browse/AVRO-3783): Read LONG length for bytes, only allow INT sizes
- [AVRO-3706](https://issues.apache.org/jira/browse/AVRO-3706): accept space in folder name

Python
- [AVRO-3761](https://issues.apache.org/jira/browse/AVRO-3761): Fix broken validation of nullable UUID field
- [AVRO-3229](https://issues.apache.org/jira/browse/AVRO-3229): Raise on invalid enum default only if validation enabled
- [AVRO-3622](https://issues.apache.org/jira/browse/AVRO-3622): Fix compatibility check for schemas having or missing namespace
- [AVRO-3669](https://issues.apache.org/jira/browse/AVRO-3669): Add py.typed marker file (PEP561 compliance)
- [AVRO-3672](https://issues.apache.org/jira/browse/AVRO-3672): Add CI testing for Python 3.11
- [AVRO-3680](https://issues.apache.org/jira/browse/AVRO-3680): allow to disable name validation

Ruby
- [AVRO-3775](https://issues.apache.org/jira/browse/AVRO-3775): Fix decoded default value of logical type
- [AVRO-3697](https://issues.apache.org/jira/browse/AVRO-3697): Test against Ruby 3.2
- [AVRO-3722](https://issues.apache.org/jira/browse/AVRO-3722): Eagerly initialize instance variables for better inline cache hits

Rust
- Many, many bug fixes and implementation progress in this experimental SDK.
- Rust CI builds and lints are passing, and has been released to crates.io as version 0.15.0

In addition:
- Upgrade dependencies to latest versions, including CVE fixes.
- Testing and build improvements.
- Performance fixes, other bug fixes, better documentation and more...


Known issues
- [AVRO-3789](https://issues.apache.org/jira/browse/AVRO-3789) Java: Problem when comparing empty MAP types.

## Language SDK / Convenience artifacts

* C#: https://www.nuget.org/packages/Apache.Avro/1.11.2
* Java: https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.2/
* Javascript: https://www.npmjs.com/package/avro-js/v/1.11.2
* Perl: https://metacpan.org/release/Avro
* Python 3: https://pypi.org/project/avro/1.11.2
* Ruby: https://rubygems.org/gems/avro/versions/1.11.2
* Rust: https://crates.io/crates/apache-avro/0.15.0

Thanks to everyone for contributing!

