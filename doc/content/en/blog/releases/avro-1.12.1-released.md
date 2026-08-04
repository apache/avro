---
title: "Avro 1.12.1"
linkTitle: "Avro 1.12.1"
date: 2025-10-16
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

The Apache Avro community is pleased to announce the release of Avro 1.12.1!

All signed release artifacts, signatures and verification instructions can be found <a href="{{< relref "/project/download" >}}">here</a>

## Security Fixes

This release addresses 4 security fixes:
* Prevent class with empty Java package being trusted by SpecificDatumReader ([#3311](https://github.com/apache/avro/pull/3311))
* Remove the default serializable packages and deprecated the property to introduce org.apache.avro.SERIALIZABLE_CLASSES instead ([#3376](https://github.com/apache/avro/pull/3376))
* java-[key-]class allowed packages must be packages ([#3453](https://github.com/apache/avro/pull/3453))
* [AVRO-4053](https://issues.apache.org/jira/browse/AVRO-4053): doc consistency in velocity templates ([#3150](https://github.com/apache/avro/pull/3150))

These fixes apply only to the Java SDK.


## Highlights

### C++
* [AVRO-4038](https://issues.apache.org/jira/browse/AVRO-4038): Add support local-timestamp-nanos and timestamp-nanos
* [AVRO-4081](https://issues.apache.org/jira/browse/AVRO-4081): Add big decimal support
* [AVRO-4058](https://issues.apache.org/jira/browse/AVRO-4058): Allow custom attributes in arrays
* [AVRO-4120](https://issues.apache.org/jira/browse/AVRO-4120): Allow custom attributes for MAP and FIXED types
* [AVRO-4140](https://issues.apache.org/jira/browse/AVRO-4140): Support uuid to annotate fixed
* [AVRO-3984](https://issues.apache.org/jira/browse/AVRO-3984): Improved code generation for unions

### C#
* [AVRO-4075](https://issues.apache.org/jira/browse/AVRO-4075): Fix JsonDecoder string type failing to decode ISO string date
* [AVRO-2032](https://issues.apache.org/jira/browse/AVRO-2032): Add support for NaN, Infinity and -Infinity in JsonDecoder

### Java
* [AVRO-4062](https://issues.apache.org/jira/browse/AVRO-4062): Allow leading underscores for names in idl
* [AVRO-4119](https://issues.apache.org/jira/browse/AVRO-4119): Make Nullable and NotNull annotations configurable
* [AVRO-4039](https://issues.apache.org/jira/browse/AVRO-4039): fix GenericData.newArray to only return an appropriate array implementation
* [AVRO-3940](https://issues.apache.org/jira/browse/AVRO-3940): Allow schema redefinition when equal
* [AVRO-3230](https://issues.apache.org/jira/browse/AVRO-3230): Enable fastread by default
* [AVRO-4133](https://issues.apache.org/jira/browse/AVRO-4133): Support default enum value in Protobuf to Avro
* [AVRO-4165](https://issues.apache.org/jira/browse/AVRO-4165): ability to specify AvroEncode on a class

### PHP
* [AVRO-2843](https://issues.apache.org/jira/browse/AVRO-2843): PHP submit package on packagist.org
* [AVRO-4046](https://issues.apache.org/jira/browse/AVRO-4046): Handling of default values


## Other changes

These SDKs have upgraded dependencies and minor bugfixes:
* C++
* C#
* Javascript
* Java
* Python


## Language SDK / Convenience artifacts

* C#: https://www.nuget.org/packages/Apache.Avro/1.12.1
* Java: https://repo1.maven.org/maven2/org/apache/avro/avro/1.12.1/
* Javascript: https://www.npmjs.com/package/avro-js/v/1.12.1
* Perl: https://metacpan.org/release/Avro
* Python 3: https://pypi.org/project/avro/1.12.1
* Ruby: https://rubygems.org/gems/avro/versions/1.12.1

Thanks to everyone for contributing!
