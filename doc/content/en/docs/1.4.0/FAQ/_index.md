---
title: "FAQ"
linkTitle: "FAQ"
weight: 210
date: 2021-10-25
aliases:
- spec.html
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
## What is Avro?
Avro is a data serialization system.

Avro provides:

* Rich data structures.
* A compact, fast, binary data format.
* A container file, to store persistent data.
* Remote procedure call (RPC).
* Simple integration with dynamic languages. Code generation is not required to read or write data files nor to use or implement RPC protocols. Code generation as an optional optimization, only worth implementing for statically typed languages.

## How can I get started quickly with Avro?
Check out the [Quick Start Guide](https://github.com/phunt/avro-rpc-quickstart).

## How do I statically compile a schema or protocol into generated code?
### In Java
* Add the avro jar, the jackson-mapper-asl.jar and jackson-core-asl.jar to your CLASSPATH.
* Run java org.apache.avro.specific.SpecificCompiler <json file>.

This appears to be out of date, the SpecificCompiler requires two arguments, presumably an input and and output file, but it isn't clear that this does.

Or use the Schema or Protocol Ant tasks. Avro's build.xml provides examples of how these are used.

Lastly, you can also use the "avro-tools" jar which ships with an Avro release. Just use the "compile (schema|protocol)" command.

## How are Strings represented in Java?
They use [org.apache.avro.util.Utf8](https://avro.apache.org/docs/current/api/java/org/apache/avro/util/Utf8.html), not java.lang.String.

## More generally, how do Avro types map to Java types?
The mappings are documented in the package javadoc for [generic](https://avro.apache.org/docs/1.7.6/gettingstartedjava.html#Defining+a+schema), [specific](https://avro.apache.org/docs/current/api/java/org/apache/avro/specific/package-summary.html#package_description) and [reflect](https://avro.apache.org/docs/current/api/java/org/apache/avro/reflect/package-summary.html#package_description) API.

## What is the purpose of the sync marker in the object file format?
From Doug Cutting:

HDFS splits files into blocks, and mapreduce runs a map task for each block. When the task starts, it needs to be able to seek into the file to the start of the block process through the block's end. If the file were, e.g., a gzip file, this would not be possible, since gzip files must be decompressed from the start. One cannot seek into the middle of a gzip file and start decompressing. So Hadoop's SequenceFile places a marker periodically (~64k) in the file at record and compression boundaries, where processing can be sensibly started. Then, when a map task starts processing an HDFS block, it finds the first marker after the block's start and continues through the first marker in the next block of the file. This requires a bit of non-local access (~0.1%). Avro's data file uses the same method as SequenceFile.

## Why isn't every value in Avro nullable?
When serialized, if any value may be null then it must be noted that it
is non-null, adding at least a bit to the size of every value stored and
corresponding computational costs to create this bit on write and
interpret it on read. These costs are wasted when values may not in
fact be null, as is the case in many datasets. In Avro such costs are
only paid when values may actually be null.

Also, allowing values to be null is a well-known source of errors. In
Avro, a value declared as non-null will always be non-null and programs
need not test for null values when processing it nor will they ever fail
for lack of such tests.

Tony Hoare calls his invention of null references his "Billion Dollar
Mistake".

http://qconlondon.com/london-2009/presentation/Null+References:+The+Billion+Dollar+Mistake

Also note that in some programming languages not all values are permitted to be null. For example, in Java, values of type boolean, byte, short, char, int, float, long, and double may not be null.

## How can I serialize directly to/from a byte array?
As pointed out in the specification, Avro data should always be stored with its [schema](https://avro.apache.org/docs/1.7.6/spec.html#Data+Serialization). The Avro provided classes [DataFileWriter](https://avro.apache.org/docs/1.7.6/api/java/org/apache/avro/file/DataFileWriter.html), [DataFileReader](https://avro.apache.org/docs/1.7.6/api/java/org/apache/avro/file/DataFileReader.html), and [DataFileStream](https://avro.apache.org/docs/1.7.6/api/java/org/apache/avro/file/DataFileStream.html) all ensure this by serializing the Schema in a container header. In some special cases, such as when implementing a new storage system or writing unit tests, you may need to write and read directly with the bare Avro serialized values. The following examples use [code generated for Java](https://cwiki.apache.org/confluence/display/AVRO/FAQ#FAQ-InJava) from an Avro Schema, specifically the [User example](https://avro.apache.org/docs/1.7.6/gettingstartedjava.html#Defining+a+schema) from the Quickstart guide.

### Serializing to a byte array
This example takes a User object and returns a newly allocated byte array with the Avro serialization of that user.
```json
ByteArrayOutputStream out = new ByteArrayOutputStream();
BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
DatumWriter<User> writer = new SpecificDatumWriter<User>(User.getClassSchema());

writer.write(user, encoder);
encoder.flush();
out.close();
byte[] serializedBytes = out.toByteArray();
```
### Deserializing from a byte array

This example takes a byte array containing the Avro serialization of a user and returns a User object.SpecificDatumReader<User> reader = new SpecificDatumReader<User>(User.getClassSchema());

```json
Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
User user = reader.read(null, decoder);
```
**Note**: If you are serializing or deserializing in a loop or as a method, you should be reusing objects, readers and/or writers. Check the JavaDoc to see how to reuse the objects.