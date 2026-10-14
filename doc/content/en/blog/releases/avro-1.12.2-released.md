---
title: "Avro 1.12.2"
linkTitle: "Avro 1.12.2"
date: 2026-08-12
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

The Apache Avro community is pleased to announce the release of Avro 1.12.2!

All signed release artifacts, signatures and verification instructions can be found <a href="{{< relref "/project/download" >}}">here</a>

## Security Fixes

This release includes a broad round of hardening against malformed and adversarial input across the Java and Python SDKs (bounding allocations and enforcing decompression limits before trusting size fields read from the input), plus a handful of other fixes with security impact in C#, C++ and JavaScript:

### C#
* [AVRO-4196](https://issues.apache.org/jira/browse/AVRO-4196): Package 'Microsoft.Build.Utilities.Core' 17.8.3 has a known high severity vulnerability
* [AVRO-4314](https://issues.apache.org/jira/browse/AVRO-4314): Validate names against the Avro name grammar at parse time

### C++
* [AVRO-4228](https://issues.apache.org/jira/browse/AVRO-4228): BinaryDecoder::arrayNext() does not handle negative block counts

### Java
* [AVRO-4241](https://issues.apache.org/jira/browse/AVRO-4241): BinaryDecoder should verify available bytes before reading
* [AVRO-4247](https://issues.apache.org/jira/browse/AVRO-4247): Avro compression codecs should verify decompression size
* [AVRO-4254](https://issues.apache.org/jira/browse/AVRO-4254): Avoid logging datum values in UnresolvedUnionException
* [AVRO-4300](https://issues.apache.org/jira/browse/AVRO-4300): Bound array/map allocation and skipping when decoding on both the classic and fast readers
* [AVRO-4313](https://issues.apache.org/jira/browse/AVRO-4313): javaAnnotation values can inject arbitrary Java code into generated sources
* [AVRO-4323](https://issues.apache.org/jira/browse/AVRO-4323): Bound DataFileStream block size against available input before allocating the block buffer
* [AVRO-4324](https://issues.apache.org/jira/browse/AVRO-4324): Align ReflectDatumReader.readArray with GenericDatumReader eager-allocation guards for malformed input
* [AVRO-4325](https://issues.apache.org/jira/browse/AVRO-4325): Validate column-file header counts and lengths before allocating in the Trevni readers

### JavaScript
* [AVRO-4252](https://issues.apache.org/jira/browse/AVRO-4252): Update JS dependencies with security issues

### Python
* [AVRO-4290](https://issues.apache.org/jira/browse/AVRO-4290): Enforce a maximum decompressed block size
* [AVRO-4296](https://issues.apache.org/jira/browse/AVRO-4296): Bound allocation when decoding length-prefixed values and collections


## Breaking Changes

### Java

The Avro 1.12.2 Java SDK now restricts arbitrary Java classes from being instantiated, either from the `SpecificDatumReader` or `java-class` attributes in a schema. 
If you are not setting the `org.apache.avro.SERIALIZABLE_CLASSES` or `org.apache.avro.SERIALIZABLE_PACKAGES` system properties, you may experience the following `java.lang.SecurityException`: 

```
java.lang.SecurityException: Forbidden com.example.MyCustomClass!
  This class is not trusted to be included in Avro schemas.
    at org.apache.avro.util.ClassSecurityValidator.validate(ClassSecurityValidator.java:60)
    at org.apache.avro.util.ClassUtils.forName(ClassUtils.java:99)
    ...
```

See [AVRO-4189](https://issues.apache.org/jira/browse/AVRO-4189) for more details.

The recommended action is to list the classes and packages that Avro is allowed to instantiate in the `org.apache.avro.SERIALIZABLE_CLASSES` or `org.apache.avro.SERIALIZABLE_PACKAGES` system properties.
If you are running Avro in an environment with trusted schemas and trusted data, you can restore the old behaviour by setting `org.apache.avro.SERIALIZABLE_PACKAGES` to `*`
(or calling `ClassSecurityValidator.setGlobal(...)` to trust your own classes).

## Highlights

### C
* [AVRO-4246](https://issues.apache.org/jira/browse/AVRO-4246): Memory leak in avroc on failed decoding
* [AVRO-4270](https://issues.apache.org/jira/browse/AVRO-4270): Fix wrong schema accessor in avro_generic_map_class

### C#
* [AVRO-2825](https://issues.apache.org/jira/browse/AVRO-2825): C# Logical Types throw exception on unknown logical type
* [AVRO-4162](https://issues.apache.org/jira/browse/AVRO-4162): C# AvroDecimal.CompareTo claims 1.55 > 2.5

### C++
* [AVRO-4206](https://issues.apache.org/jira/browse/AVRO-4206): Missing zstd in installed cmake
* [AVRO-4221](https://issues.apache.org/jira/browse/AVRO-4221): Allow using symbol visibility annotations on non-Windows platforms
* [AVRO-4248](https://issues.apache.org/jira/browse/AVRO-4248): ASAN misaligned address in BufferDetail.hh
* Reject lone low surrogate U+DFFF in the JSON decoder ([#3841](https://github.com/apache/avro/pull/3841))

### Java
* [AVRO-2032](https://issues.apache.org/jira/browse/AVRO-2032): Unable to decode JSON-encoded Double.NaN, Double.POSITIVE_INFINITY or Double.NEGATIVE_INFINITY
* [AVRO-4176](https://issues.apache.org/jira/browse/AVRO-4176): Java parser allows field type to be object with custom type
* [AVRO-4182](https://issues.apache.org/jira/browse/AVRO-4182): NoSuchElementException in IdlReader for duplicated Enum definition
* [AVRO-4183](https://issues.apache.org/jira/browse/AVRO-4183): 'result' variable in generated hashCode method shadows field 'result'
* [AVRO-4189](https://issues.apache.org/jira/browse/AVRO-4189): Simplify the setting of the serializable classes
* [AVRO-4197](https://issues.apache.org/jira/browse/AVRO-4197): Schema bytes defaults are broken
* [AVRO-4202](https://issues.apache.org/jira/browse/AVRO-4202): Avro tools hashCode method conflict with schema field result
* [AVRO-4209](https://issues.apache.org/jira/browse/AVRO-4209): ReflectData.getSchema fails for a POJO class that contains a field of the same type
* [AVRO-4210](https://issues.apache.org/jira/browse/AVRO-4210): BinaryData.compareBytes should treat bytes as unsigned
* [AVRO-4211](https://issues.apache.org/jira/browse/AVRO-4211): IDLUtils cannot generate schema for union fields with a default value that is not the first element
* [AVRO-4225](https://issues.apache.org/jira/browse/AVRO-4225): GenericDatumReader throws ClassCastException for schemas with "java-class" attribute on string fields
* [AVRO-4238](https://issues.apache.org/jira/browse/AVRO-4238): FastReader fails to unbox nested type when defaulting a union<array<>> field
* [AVRO-4242](https://issues.apache.org/jira/browse/AVRO-4242): Malformed Avro container without schema metadata should fail graciously
* [AVRO-4257](https://issues.apache.org/jira/browse/AVRO-4257): Use the new SchemaParser introduced with Avro 1.12
* [AVRO-4268](https://issues.apache.org/jira/browse/AVRO-4268): BytesWritableConverter serializes unused capacity bytes
* [AVRO-4269](https://issues.apache.org/jira/browse/AVRO-4269): TimestampNanosConversion.toLong(...) encodes pre-epoch instants with the wrong nanosecond offset
* [AVRO-4321](https://issues.apache.org/jira/browse/AVRO-4321): NPE on getConversionByClass
* [AVRO-4322](https://issues.apache.org/jira/browse/AVRO-4322): Fast and classic readers change GenericData behaviour

### Perl
* [AVRO-4239](https://issues.apache.org/jira/browse/AVRO-4239): Perl porting lacks a version number

### Python
* [AVRO-3760](https://issues.apache.org/jira/browse/AVRO-3760): Using enum with default symbol, cannot parse future value
* Fixed byte compare in ipc.py ([#3710](https://github.com/apache/avro/pull/3710))


## Other changes

These SDKs also picked up dependency and build-tooling updates with no other user-facing change: C#, C++, Java, JavaScript, Python.


## Language SDK / Convenience artifacts

* C#: https://www.nuget.org/packages/Apache.Avro/1.12.2
* Java: https://repo1.maven.org/maven2/org/apache/avro/avro/1.12.2/
* JavaScript: https://www.npmjs.com/package/avro-js/v/1.12.2
* Perl: https://metacpan.org/release/Avro
* Python 3: https://pypi.org/project/avro/1.12.2
* Ruby: https://rubygems.org/gems/avro/versions/1.12.2

Thanks to everyone for contributing!
