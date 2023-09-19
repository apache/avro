---
title: "IDL Language"
linkTitle: "IDL Language"
weight: 201
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
This document defines Avro IDL, a higher-level language for authoring Avro schemata. Before reading this document, you should have familiarity with the concepts of schemata and protocols, as well as the various primitive and complex types available in Avro.

## Overview

### Purpose
The aim of the Avro IDL language is to enable developers to author schemata in a way that feels more similar to common programming languages like Java, C++, or Python. Additionally, the Avro IDL language may feel more familiar for those users who have previously used the interface description languages (IDLs) in other frameworks like Thrift, Protocol Buffers, or CORBA.

### Usage
Each Avro IDL file defines either a single Avro Protocol, or an Avro Schema with supporting named schemata in a namespace. When parsed, it thus yields either a Protocol or a Schema. These can be respectively written to JSON-format Avro Protocol files with extension .avpr or JSON-format Avro Schema files with extension .avsc.

To convert a _.avdl_ file into a _.avpr_ file, it may be processed by the `idl` tool. For example:
```shell
$ java -jar avro-tools.jar idl src/test/idl/input/namespaces.avdl /tmp/namespaces.avpr
$ head /tmp/namespaces.avpr
{
  "protocol" : "TestNamespace",
  "namespace" : "avro.test.protocol",
```
To convert a _.avdl_ file into a _.avsc_ file, it may be processed by the `idl` tool too. For example:
```shell
$ java -jar avro-tools.jar idl src/test/idl/input/schema_syntax_schema.avdl /tmp/schema_syntax.avsc
$ head /tmp/schema_syntax.avsc
{
  "type": "array",
  "items": {
    "type": "record",
    "name": "StatusUpdate",
```
The `idl` tool can also process input to and from _stdin_ and _stdout_. See `idl --help` for full usage information.

A Maven plugin is also provided to compile .avdl files. To use it, add something like the following to your pom.xml:
```xml
<build>
  <plugins>
    <plugin>
      <groupId>org.apache.avro</groupId>
      <artifactId>avro-maven-plugin</artifactId>
      <executions>
        <execution>
          <goals>
            <goal>idl</goal>
          </goals>
        </execution>
      </executions>
    </plugin>
  </plugins>
</build>
```

## Defining a Schema in Avro IDL
An Avro IDL file consists of exactly one (main) schema definition. The minimal schema is defined by the following code:
```java
schema int;
```
This is equivalent to (and generates) the following JSON schema definition:
```json
{
  "type": "int"
}
```
More complex schemata can also be defined, for example by adding named schemata like this:
```java
namespace default.namespace.for.named.schemata;
schema Message;

record Message {
    string? title = null;
    string message;
}
```
This is equivalent to (and generates) the following JSON schema definition:
```json
{
  "type" : "record",
  "name" : "Message",
  "namespace" : "default.namespace.for.named.schemata",
  "fields" : [ {
    "name" : "title",
    "type" : [ "null", "string" ],
    "default": null
  }, {
    "name" : "message",
    "type" : "string"
  } ]
}
```
Schemata in Avro IDL can contain the following items:

* Imports of external protocol and schema files (only named schemata are imported).
* Definitions of named schemata, including records, errors, enums, and fixeds.

## Defining a Protocol in Avro IDL
An Avro IDL file consists of exactly one protocol definition. The minimal protocol is defined by the following code:
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
This notation is used throughout Avro IDL as a way of specifying properties for the annotated element, as will be described later in this document.

Protocols in Avro IDL can contain the following items:

* Imports of external protocol and schema files.
* Definitions of named schemata, including records, errors, enums, and fixeds.
* Definitions of RPC messages

## Imports
Files may be imported in one of three formats:

* An IDL file may be imported with a statement like:

  `import idl "foo.avdl";`

* A JSON protocol file may be imported with a statement like:

  `import protocol "foo.avpr";`

* A JSON schema file may be imported with a statement like:

  `import schema "foo.avsc";`

When importing into an IDL schema file, only (named) types are imported into this file. When importing into an IDL protocol, messages are imported into the protocol as well.

Imported file names are resolved relative to the current IDL file.

## Defining an Enumeration
Enums are defined in Avro IDL using a syntax similar to C or Java. An Avro Enum supports optional default values. In the case that a reader schema is unable to recognize a symbol written by the writer, the reader will fall back to using the defined default value. This default is only used when an incompatible symbol is read. It is not used if the enum field is missing.

Example Writer Enum Definition
```java
enum Shapes {
  SQUARE, TRIANGLE, CIRCLE, OVAL
}
```
Example Reader Enum Definition
```java
enum Shapes {
  SQUARE, TRIANGLE, CIRCLE
} = CIRCLE;
```
In the above example, the reader will use the default value of `CIRCLE` whenever reading data written with the `OVAL` symbol of the writer. Also note that, unlike the JSON format, anonymous enums cannot be defined.

## Defining a Fixed Length Field
Fixed fields are defined using the following syntax:
```
fixed MD5(16);
```
This example defines a fixed-length type called MD5, which contains 16 bytes.

## Defining Records and Errors
Records are defined in Avro IDL using a syntax similar to a struct definition in C:
```java
record Employee {
  string name;
  boolean active = true;
  long salary;
}
```
The above example defines a record with the name “Employee” with three fields.

To define an error, simply use the keyword _error_ instead of _record_. For example:
```java
error Kaboom {
  string explanation;
  int result_code = -1;
}
```
Each field in a record or error consists of a type and a name, optional property annotations and an optional default value.

A type reference in Avro IDL must be one of:

* A primitive type
* A logical type
* A named schema (either defined or imported)
* A complex type (array, map, or union)

### Primitive Types
The primitive types supported by Avro IDL are the same as those supported by Avro's JSON format. This list includes _int_, _long_, _string_, _boolean_, _float_, _double_, _null_, and _bytes_.

### Logical Types
Some of the logical types supported by Avro's JSON format are directly supported by Avro IDL. The currently supported types are:

* _decimal_ (logical type [decimal]({{< relref "specification#decimal" >}}))
* _date_ (logical type [date]({{< relref "specification#date" >}}))
* _time_ms_ (logical type [time-millis]({{< relref "specification#time-millisecond-precision" >}}))
* _timestamp_ms_ (logical type [timestamp-millis]({{< relref "specification#timestamp-millisecond-precision" >}}))
* _local_timestamp_ms_ (logical type [local-timestamp-millis]({{< relref "specification#local_timestamp_ms" >}}))
* _uuid_ (logical type [uuid]({{< relref "specification#uuid" >}}))

For example:
```java
record Job {
  string jobid;
  date submitDate;
  time_ms submitTime;
  timestamp_ms finishTime;
  decimal(9,2) finishRatio;
  uuid pk = "a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8";
}
```

Logical types can also be specified via an annotation, which is useful for logical types for which a keyword does not exist:

```java
record Job {
  string jobid;
  @logicalType("timestamp-micros")
  long finishTime;
}
```

### References to Named Schemata
If a named schema has already been defined in the same Avro IDL file, it may be referenced by name as if it were a primitive type:
```java
record Card {
  Suit suit; // refers to the enum Card defined above
  int number;
}
```

### Default Values
Default values for fields may be optionally specified by using an equals sign after the field name followed by a JSON expression indicating the default value. This JSON is interpreted as described in the [spec]({{< relref "specification#schema-record" >}}).

### Complex Types

#### Arrays
Array types are written in a manner that will seem familiar to C++ or Java programmers. An array of any type t is denoted `array<t>`. For example, an array of strings is denoted `array<string>`, and a multidimensional array of Foo records would be `array<array<Foo>>`.

#### Maps
Map types are written similarly to array types. An array that contains values of type t is written `map<t>`. As in the JSON schema format, all maps contain `string`-type keys.

#### Unions
Union types are denoted as `union { typeA, typeB, typeC, ... }`. For example, this record contains a string field that is optional (unioned with null), and a field containing either a precise or a imprecise number:
```java
record RecordWithUnion {
  union { null, string } optionalString;
  union { decimal(12, 6), float } number;
}
```
Note that the same restrictions apply to Avro IDL unions as apply to unions defined in the JSON format; namely, a union may not contain multiple elements of the same type. Also, fields/parameters that use the union type and have a default parameter must specify a default value of the same type as the **first** union type.

Because it occurs so often, there is a special shorthand to denote a union of `null` with one other schema. The first three fields in the following snippet have identical schemata, as do the last two fields:

```java
record RecordWithUnion {
  union { null, string } optionalString1 = null;
  string? optionalString2 = null;
  string? optionalString3; // No default value

  union { string, null } optionalString4 = "something";
  string? optionalString5 = "something else";
}
```

Note that unlike explicit unions, the position of the `null` type is fluid; it will be the first or last type depending on the default value (if any). So all fields are valid in the example above.

## Defining RPC Messages
The syntax to define an RPC message within a Avro IDL protocol is similar to the syntax for a method declaration within a C header file or a Java interface. To define an RPC message _add_ which takes two arguments named _foo_ and _bar_, returning an _int_, simply include the following definition within the protocol:
```java
int add(int foo, int bar = 0);
```
Message arguments, like record fields, may specify default values.

To define a message with no response, you may use the alias _void_, equivalent to the Avro _null_ type:
```java
void logMessage(string message);
```
If you have defined or imported an error type within the same protocol, you may declare that a message can throw this error using the syntax:
```java
void goKaboom() throws Kaboom;
```
To define a one-way message, use the keyword `oneway` after the parameter list, for example:
```java
void fireAndForget(string message) oneway;
```

## Other Language Features

### Comments and documentation
All Java-style comments are supported within a Avro IDL file. Any text following _//_ on a line is ignored, as is any text between _/*_ and _*/_, possibly spanning multiple lines.

Comments that begin with _/**_ are used as the documentation string for the type or field definition that follows the comment.

### Escaping Identifiers
Occasionally, one may want to distinguish between identifiers and languages keywords. In order to do so, backticks (`) may be used to escape
the identifier. For example, to define a message with the literal name error, you may write:
```java
void `error`();
```
This syntax is allowed anywhere an identifier is expected.

### Annotations for Ordering and Namespaces
Java-style annotations may be used to add additional properties to types and fields throughout Avro IDL. These can be custom properties, or
special properties as used in the JSON-format Avro Schema and Protocol files.

For example, to specify the sort order of a field within a record, one may use the `@order` annotation before the field name as follows:
```java
record MyRecord {
  string @order("ascending") myAscendingSortField;
  string @order("descending")  myDescendingField;
  string @order("ignore") myIgnoredField;
}
```
A field's type (with the exception of type references) may also be preceded by annotations, e.g.:
```java
record MyRecord {
  @java-class("java.util.ArrayList") array<string> myStrings;
}
```
This can be used to support java classes that can be serialized/deserialized via their `toString`/`String constructor`, e.g.:
```java
record MyRecord {
  @java-class("java.math.BigDecimal") string value;
  @java-key-class("java.io.File") map<string> fileStates;
  array<@java-class("java.math.BigDecimal") string> weights;
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

Type and field aliases are specified with the `@aliases` annotation as follows:
```java
@aliases(["org.old.OldRecord", "org.ancient.AncientRecord"])
record MyRecord {
  string @aliases(["oldField", "ancientField"]) myNewField;
}
```
Some annotations like those listed above are handled specially. All other annotations are added as properties to the protocol, message, schema or field. You can use any identifier or series of identifiers separated by dots and/or dashes as property name.

## Complete Example
The following is an example of two Avro IDL files that together show most of the above features:

### schema.avdl
```java
/*
 * Header with license information.
 */
// Optional default namespace (if absent, the default namespace is the null namespace).
namespace org.apache.avro.test;
// Optional main schema definition; if used, the IDL file is equivalent to a .avsc file.
schema TestRecord;

/** Documentation for the enum type Kind */
@aliases(["org.foo.KindOf"])
enum Kind {
  FOO,
  BAR, // the bar enum value
  BAZ
} = FOO; // For schema evolution purposes, unmatched values do not throw an error, but are resolved to FOO.

/** MD5 hash; good enough to avoid most collisions, and smaller than (for example) SHA256. */
fixed MD5(16);

record TestRecord {
  /** Record name; has no intrinsic order */
  string @order("ignore") name;

  Kind @order("descending") kind;

  MD5 hash;

  /*
  Note that 'null' is the first union type. Just like .avsc / .avpr files, the default value must be of the first union type.
  */
  union { null, MD5 } /** Optional field */ @aliases(["hash"]) nullableHash = null;
  // Shorthand syntax; the null in this union is placed based on the default value (or first is there's no default).
  MD5? anotherNullableHash = null;

  array<long> arrayOfLongs;
}
```

### protocol.avdl
```java
/*
 * Header with license information.
 */

/**
 * An example protocol in Avro IDL
 */
@namespace("org.apache.avro.test")
protocol Simple {
  // Import the example file above
  import idl "schema.avdl";

  /** Errors are records that can be thrown from a method */
  error TestError {
    string message;
  }

  string hello(string greeting);
  /** Return what was given. Demonstrates the use of backticks to name types/fields/messages/parameters after keywords */
  TestRecord echo(TestRecord `record`);
  int add(int arg1, int arg2);
  bytes echoBytes(bytes data);
  void `error`() throws TestError;
  // The oneway keyword forces the method to return null.
  void ping() oneway;
}
```

Additional examples may be found in the Avro source tree under the `src/test/idl/input` directory.

## IDE support

There are several editors and IDEs that support Avro IDL files, usually via plugins.

### JetBrains

Apache Avro IDL Schema Support 203.1.2 was released in 9 December 2021.

Features:
* Syntax Highlighting
* Code Completion
* Code Formatting
* Error Highlighting
* Inspections & quick fixes
* JSON schemas for .avpr and .avsc files

It's available via the [JetBrains Marketplace](https://plugins.jetbrains.com/plugin/15728-apache-avro-idl-schema-support)
and on [GitHub](https://github.com/opwvhk/avro-schema-support).

The plugin supports almost the all JetBrains products: IntelliJ IDEA, PyCharm, WebStorm, Android Studio, AppCode, GoLand, Rider, CLion, RubyMine, PhpStorm, DataGrip, DataSpell, MPS, Code With Me Guest and JetBrains Client.

Only JetBrains Gateway does not support this plugin directly. But the backend (JetBrains) IDE that it connects to does.

### Eclipse

Avroclipse 0.0.11 was released on 4 December 2019.

Features:
* Syntax Highlighting
* Error Highlighting
* Code Completion

It is available on the [Eclipse Marketplace](https://marketplace.eclipse.org/content/avroclipse)
and [GitHub](https://github.com/dvdkruk/avroclipse).

### Visual Studio Code

avro-idl 0.5.0 was released on 16 June 2021. It provides syntax highlighting.

It is available on the [VisualStudio Marketplace](https://marketplace.visualstudio.com/items?itemName=streetsidesoftware.avro)
and [GitHub](https://github.com/Jason3S/vscode-avro-ext)

### Atom.io

atom-language-avro 0.0.13 was released on 14 August 2015. It provides syntax highlighting.

It is available as [Atom.io package](https://atom.io/packages/atom-language-avro)
and [GitHub](https://github.com/jonesetc/atom-language-avro)

### Vim

A `.avdl` detecting plugin by Gurpreet Atwal on [GitHub](https://github.com/gurpreetatwal/vim-avro) (Last change in December 2016)

[avro-idl.vim](https://github.com/apache/avro/blob/master/share/editors/avro-idl.vim) in the Avro repository `share/editors` directory (last change in September 2010)

Both provide syntax highlighting.
