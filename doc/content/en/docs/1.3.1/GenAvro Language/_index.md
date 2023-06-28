---
title: "GenAvro Language"
linkTitle: "GenAvro Language"
weight: 207
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

## Introduction
This document defines GenAvro, an experimental higher-level language for authoring Avro schemata. Before reading this document, you should have familiarity with the concepts of schemata and protocols, as well as the various primitive and complex types available in Avro.

N.B. This feature is considered experimental in the current version of Avro and the language has not been finalized. Although major changes are unlikely, some syntax may change in future versions of Avro.

## Overview

### Purpose
The aim of the GenAvro language is to enable developers to author schemata in a way that feels more similar to common programming languages like Java, C++, or Python. Additionally, the GenAvro language may feel more familiar for those users who have previously used the interface description languages (IDLs) in other frameworks like Thrift, Protocol Buffers, or CORBA.

### Usage
Each GenAvro file defines a single Avro Protocol, and thus generates as its output a JSON-format Avro Protocol file with extension .avpr.

To convert a _.genavro_ file into a _.avpr_ file, it must be processed by the `avroj genavro` tool. For example:
```shell
$ java -jar avroj-1.3.0.jar genavro src/test/genavro/input/namespaces.genavro /tmp/namespaces.avpr
$ head /tmp/namespaces.avpr
{
  "protocol" : "TestNamespace",
  "namespace" : "avro.test.protocol",
```

The avroj genavro tool can also process input to and from stdin and stdout. See avroj genavro --help for full usage information.

## Defining a Protocol in GenAvro
A GenAvro file consists of exactly one protocol definition. The minimal protocol is defined by the following code:

```java
protocol MyProtocol {
}
```
This is equivalent to (and generates) the following JSON protocol definition:
```json
{
"protocol" : "MyProtocol",
  "types" : [ ],
  "messages" : {
  }
}
```
The namespace of the protocol may be changed using the @namespace annotation:
```java
@namespace("mynamespace")
protocol MyProtocol {
}
```
3 This notation is used throughout GenAvro as a way of specifying properties for the annotated element, as will be described later in this document.

Protocols in GenAvro can contain the following items:

* Definitions of named schemata, including records, errors, enums, and fixeds.
* Definitions of RPC messages

## Defining an Enumeration
Enums are defined in GenAvro using a syntax similar to C or Java: 

```java
enum Suit {
  SPADES, DIAMONDS, CLUBS, HEARTS
}
```
Note that, unlike the JSON format, anonymous enums cannot be defined.

## Defining a Fixed Length Field
Fixed fields are defined using the following syntax:
```
fixed MD5(16);
```
This example defines a fixed-length type called MD5 which contains 16 bytes.

## Defining Records and Errors
Records are defined in GenAvro using a syntax similar to a struct definition in C:
```java
record Employee {
  string name;
  boolean active;
  long salary;
}
```
The above example defines a record with the name “Employee” with three fields.

To define an error, simply use the keyword _error_ instead of _record_. For example:
```java
error Kaboom {
  string explanation;
  int result_code;
}
```
Each field in a record or error consists of a type and a name, along with optional property annotations.

A type reference in GenAvro must be one of:

* A primitive type
* A named schema defined prior to this usage in the same Protocol
* A complex type (array, map, or union)

### Primitive Types
The primitive types supported by GenAvro are the same as those supported by Avro's JSON format. This list includes _int_, _long_, _string_, _boolean_, _float_, _double_, _null_, and _bytes_.




### References to Named Schemata
If a named schema has already been defined in the same GenAvro file, it may be referenced by name as if it were a primitive type:
```java
record Card {
  Suit suit; // refers to the enum Card defined above
  int number;
}
```

### Complex Types

#### Arrays
Array types are written in a manner that will seem familiar to C++ or Java programmers. An array of any type t is denoted `array<t>`. For example, an array of strings is denoted `array<string>`, and a multidimensional array of Foo records would be `array<array<Foo>>`.

#### Maps
Map types are written similarly to array types. An array that contains values of type t is written `map<t>`. As in the JSON schema format, all maps contain `string`-type keys.

#### Unions
Union types are denoted as union { typeA, typeB, typeC, ... }. For example, this record contains a string field that is optional (unioned with null):

```java
record RecordWithUnion {
  union { null, string } optionalString;

}
```

Note that the same restrictions apply to GenAvro unions as apply to unions defined in the JSON format; namely, a record may not contain multiple elements of the same type.

## Defining RPC Messages
The syntax to define an RPC message within a GenAvro protocol is similar to the syntax for a method declaration within a C header file or a Java interface. To define an RPC message add which takes two arguments named _foo_ and _bar_, returning an _int_, simply include the following definition within the protocol:
```java
int add(int foo, int bar);	
```

To define a message with no response, you may use the alias _void_, equivalent to the Avro _null_ type:
```java
void logMessage(string message);
```
If you have previously defined an error type within the same protocol, you may declare that a message can throw this error using the syntax:
```java
void goKaboom() throws Kaboom;
```


## Other Language Features

### Comments
All Java-style comments are supported within a GenAvro file. Any text following _//_ on a line is ignored, as is any text between _/*_ and _*/_, possibly spanning multiple lines.

### Escaping Identifiers
Occasionally, one will need to use a reserved language keyword as an identifier. In order to do so, backticks (`) may be used to escape the identifier. For example, to define a message with the literal name error, you may write:
```java
void `error`();
```
This syntax is allowed anywhere an identifier is expected.

### Annotations for Ordering and Namespaces
Java-style annotations may be used to add additional properties to types throughout GenAvro. For example, to specify the sort order of a field within a record, one may use the `@order` annotation as follows:

```java
record MyRecord {
  @order("ascending")
  string myAscendingSortField;
  @order("descending")
  string myDescendingField;
  @order("ignore")
  string myIgnoredField;
}
```

Similarly, a `@namespace` annotation may be used to modify the namespace when defining a named schema. For example:
```java
@namespace("org.apache.avro.firstNamespace")
protocol MyProto {
  @namespace("org.apache.avro.someOtherNamespace")
  record Foo {}

  record Bar {}
}
```
will define a protocol in the _firstNamespace_ namespace. The record _Foo_ will be defined in _someOtherNamespace_ and _Bar_ will be defined in _firstNamespace_ as it inherits its default from its container.



## Complete Example
The following is a complete example of a GenAvro file that shows most of the above features:
```java
/**

 * An example protocol in GenAvro

 */
@namespace("org.apache.avro.test")

protocol Simple {

  @aliases(["org.foo.KindOf"])

  enum Kind {

    FOO,

    BAR, // the bar enum value

    BAZ

  } 
  
  fixed MD5(16);

  record TestRecord {

     @order("ignore")

    string name;

   @order("descending")

    Kind kind;

    MD5 hash;

    union { MD5, null} nullableHash;

    array<long> arrayOfLongs;
  }

  error TestError {

    string message;

  }

  string hello(string greeting);

  TestRecord echo(TestRecord `record`);

  int add(int arg1, int arg2);

  bytes echoBytes(bytes data);

  void `error`() throws TestError;

  void ping() oneway;
}
```
Additional examples may be found in the Avro source tree under the src/test/genavro/input directory.