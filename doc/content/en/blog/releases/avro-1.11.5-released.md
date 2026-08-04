---
title: "Avro 1.11.5"
linkTitle: "Avro 1.11.5"
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

The Apache Avro community is pleased to announce the release of Avro 1.11.5!

All signed release artifacts, signatures and verification instructions can
be found <a href="{{< relref "/project/download" >}}">here</a>

## Security Fixes

This release addresses 4 security fixes:
* Prevent class with empty Java package being trusted by SpecificDatumReader ([#3311](https://github.com/apache/avro/pull/3311))
* Remove the default serializable packages and deprecated the property to introduce org.apache.avro.SERIALIZABLE_CLASSES instead ([#3376](https://github.com/apache/avro/pull/3376))
* java-[key-]class allowed packages must be packages ([#3453](https://github.com/apache/avro/pull/3453))
* [AVRO-4053](https://issues.apache.org/jira/browse/AVRO-4053): doc consistency in velocity templates ([#3150](https://github.com/apache/avro/pull/3150))

These fixes apply only to the Java SDK. All other SDKs have no difference with their 1.11.4 release.


## Language SDK / Convenience artifacts

* Java: https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.5/

Thanks to everyone for contributing!
