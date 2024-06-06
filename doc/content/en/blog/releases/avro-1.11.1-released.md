---
title: "Avro 1.11.1"
linkTitle: "Avro 1.11.1"
date: 2022-07-31
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

The Apache Avro community is pleased to announce the release of Avro 1.11.1!

All signed release artifacts, signatures and verification instructions can
be found <a href="{{< relref "/project/download" >}}">here</a>

## Most interesting

This release includes 256 Jira issues, including some interesting features:

Avro specification
- [AVRO-3436](https://issues.apache.org/jira/browse/AVRO-3436) Clarify which names are allowed to be qualified with namespaces
- [AVRO-3370](https://issues.apache.org/jira/browse/AVRO-3370) Inconsistent behaviour on types as invalid names
- [AVRO-3275](https://issues.apache.org/jira/browse/AVRO-3275) Clarify how fullnames are created, with example
- [AVRO-3257](https://issues.apache.org/jira/browse/AVRO-3257) IDL: add syntax to create optional fields
- [AVRO-2019](https://issues.apache.org/jira/browse/AVRO-2019) Improve docs for logical type annotation

C++
- [AVRO-2722](https://issues.apache.org/jira/browse/AVRO-2722) Use of boost::mt19937 is not thread safe

C#
- [AVRO-3383](https://issues.apache.org/jira/browse/AVRO-3383) Many completed subtasks for modernizing C# coding style
- [AVRO-3481](https://issues.apache.org/jira/browse/AVRO-3481) Input and output variable type mismatch
- [AVRO-3475](https://issues.apache.org/jira/browse/AVRO-3475) Enforce time-millis and time-micros specification
- [AVRO-3469](https://issues.apache.org/jira/browse/AVRO-3469) Build and test using .NET SDK 7.0
- [AVRO-3468](https://issues.apache.org/jira/browse/AVRO-3468) Default values for logical types not supported
- [AVRO-3467](https://issues.apache.org/jira/browse/AVRO-3467) Use oracle-actions to test with Early Access JDKs
- [AVRO-3453](https://issues.apache.org/jira/browse/AVRO-3453) Avrogen Add Generated Code Attribute
- [AVRO-3432](https://issues.apache.org/jira/browse/AVRO-3432) Add command line option to skip creation of directories
- [AVRO-3411](https://issues.apache.org/jira/browse/AVRO-3411) Add Visual Studio Code Devcontainer support
- [AVRO-3388](https://issues.apache.org/jira/browse/AVRO-3388) Implement extra codecs for C# as seperate nuget packages
- [AVRO-3265](https://issues.apache.org/jira/browse/AVRO-3265) avrogen generates uncompilable code when namespace ends
with ".Avro"
- [AVRO-3219](https://issues.apache.org/jira/browse/AVRO-3219) Support nullable enum type fields

Java
- [AVRO-3531](https://issues.apache.org/jira/browse/AVRO-3531) GenericDatumReader in multithread lead to infinite loop
- [AVRO-3482](https://issues.apache.org/jira/browse/AVRO-3482) Reuse MAGIC in DataFileReader
- [AVRO-3586](https://issues.apache.org/jira/browse/AVRO-3586) Make Avro Build Reproducible
- [AVRO-3441](https://issues.apache.org/jira/browse/AVRO-3441) Automatically register LogicalTypeFactory classes
- [AVRO-3375](https://issues.apache.org/jira/browse/AVRO-3375) Add union branch, array index and map key "path"
information to serialization errors
- [AVRO-3374](https://issues.apache.org/jira/browse/AVRO-3374) Fully qualified type reference "ns.int" loses namespace
- [AVRO-3294](https://issues.apache.org/jira/browse/AVRO-3294) IDL parsing allows doc comments in strange places
- [AVRO-3273](https://issues.apache.org/jira/browse/AVRO-3273) avro-maven-plugin breaks on old versions of Maven
- [AVRO-3266](https://issues.apache.org/jira/browse/AVRO-3266) Output stream incompatible with MagicS3GuardCommitter
- [AVRO-3243](https://issues.apache.org/jira/browse/AVRO-3243) Lock conflicts when using computeIfAbsent
- [AVRO-3120](https://issues.apache.org/jira/browse/AVRO-3120) Support Next Java LTS (Java 17)
- [AVRO-2498](https://issues.apache.org/jira/browse/AVRO-2498) UUID generation is not working

Javascript
- [AVRO-3489](https://issues.apache.org/jira/browse/AVRO-3489) Replace istanbul with nyc for code coverage
- [AVRO-3322](https://issues.apache.org/jira/browse/AVRO-3322) Buffer is not defined in browser environment
- [AVRO-3084](https://issues.apache.org/jira/browse/AVRO-3084) Fix JavaScript interop test to work with other languages on CI

Perl
- [AVRO-3263](https://issues.apache.org/jira/browse/AVRO-3263) Schema validation warning on invalid schema with a long field

Python
- [AVRO-3542](https://issues.apache.org/jira/browse/AVRO-3542) Scale assignment optimization
- [AVRO-3521](https://issues.apache.org/jira/browse/AVRO-3521) "Scale" property from decimal object
- [AVRO-3380](https://issues.apache.org/jira/browse/AVRO-3380) Byte reading in avro.io does not assert bytes read
- [AVRO-3229](https://issues.apache.org/jira/browse/AVRO-3229) validate the default value of an enum field
- [AVRO-3218](https://issues.apache.org/jira/browse/AVRO-3218) Pass LogicalType to BytesDecimalSchema

Ruby
- [AVRO-3277](https://issues.apache.org/jira/browse/AVRO-3277) Test against Ruby 3.1

Rust
- [AVRO-3558](https://issues.apache.org/jira/browse/AVRO-3558) Add a demo crate that shows usage as WebAssembly
- [AVRO-3526](https://issues.apache.org/jira/browse/AVRO-3526) Improve resolving Bytes and Fixed from string
- [AVRO-3506](https://issues.apache.org/jira/browse/AVRO-3506) Implement Single Object Writer
- [AVRO-3507](https://issues.apache.org/jira/browse/AVRO-3507) Implement Single Object Reader
- [AVRO-3405](https://issues.apache.org/jira/browse/AVRO-3405) Add API for user-provided metadata to file
- [AVRO-3339](https://issues.apache.org/jira/browse/AVRO-3339) Rename crate from avro-rs to apache-avro
- [AVRO-3479](https://issues.apache.org/jira/browse/AVRO-3479) Derive Avro Schema macro

Website
- [AVRO-2175](https://issues.apache.org/jira/browse/AVRO-2175) Website refactor
- [AVRO-3450](https://issues.apache.org/jira/browse/AVRO-3450) Document IDL support in IDEs


## Rust

This is the first release that provides the `apache-avro` crate at [crates.io](https://crates.io/crates/apache-avro)!

## JIRA

A list of all JIRA tickets fixed in 1.11.1 could be found [here](https://issues.apache.org/jira/issues/?jql=project%3DAVRO%20AND%20fixVersion%3D1.11.1)

## Language repositories

In addition, language-specific release artifacts are available:

* C#: https://www.nuget.org/packages/Apache.Avro/1.11.1
* Java: https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.1/
* Javascript: https://www.npmjs.com/package/avro-js/v/1.11.1
* Perl: https://metacpan.org/release/Avro
* Python 3: https://pypi.org/project/avro/1.11.1
* Ruby: https://rubygems.org/gems/avro/versions/1.11.1
* Rust: https://crates.io/crates/apache-avro/0.14.0

Thanks to everyone for contributing!

