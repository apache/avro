---
title: "Avro 1.11.4"
linkTitle: "Avro 1.11.4"
date: 2024-09-22
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

The Apache Avro community is pleased to announce the release of Avro 1.11.4!

All signed release artifacts, signatures and verification instructions can
be found <a href="{{< relref "/project/download" >}}">here</a>

This release [addresses 4 Jira issues](https://issues.apache.org/jira/issues/?jql=project%3DAVRO%20AND%20fixVersion%3D1.11.4) 
only in the Java SDK. All other SDKs have no difference to their 1.12.0 release, so please use 1.12.0 for them!

## Highlights

Java
- [AVRO-3985](https://issues.apache.org/jira/browse/AVRO-3985): Restrict trusted packages in ReflectData and SpecificData
- [AVRO-3989](https://issues.apache.org/jira/browse/AVRO-3989): Maven Plugin Always Recompiles IDL Files
- [AVRO-3880](https://issues.apache.org/jira/browse/AVRO-3880): Upgrade maven-antrun-plugin to 3.1.0
- [AVRO-3748](https://issues.apache.org/jira/browse/AVRO-3748): issue with DataFileSeekableInput.SeekableInputStream.skip


## Language SDK / Convenience artifacts

* Java: https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.4/

Thanks to everyone for contributing!
