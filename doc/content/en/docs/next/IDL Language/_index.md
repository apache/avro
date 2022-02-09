---
title: "IDL Language"
linkTitle: "IDL Language"
weight: 8
---

## Introduction
This document defines Avro IDL, a higher-level language for authoring Avro schemata. Before reading this document, you should have familiarity with the concepts of schemata and protocols, as well as the various primitive and complex types available in Avro.

## Overview

### Purpose
The aim of the Avro IDL language is to enable developers to author schemata in a way that feels more similar to common programming languages like Java, C++, or Python. Additionally, the Avro IDL language may feel more familiar for those users who have previously used the interface description languages (IDLs) in other frameworks like Thrift, Protocol Buffers, or CORBA.

### Usage
Each Avro IDL file defines a single Avro Protocol, and thus generates as its output a JSON-format Avro Protocol file with extension .avpr.

To convert a _.avdl_ file into a _.avpr_ file, it may be processed by the `idl` tool. For example:
```shell
$ java -jar avro-tools.jar idl src/test/idl/input/namespaces.avdl /tmp/namespaces.avpr
$ head /tmp/namespaces.avpr
{
  "protocol" : "TestNamespace",
  "namespace" : "avro.test.protocol",
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
            <goal>idl-protocol</goal>
          </goals>
        </execution>
      </executions>
    </plugin>
  </plugins>
</build>
```

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

Messages and types in the imported file are added to this file's protocol.

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
This example defines a fixed-length type called MD5 which contains 16 bytes.

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
* A named schema defined prior to this usage in the same Protocol
* A complex type (array, map, or union)

### Primitive Types
The primitive types supported by Avro IDL are the same as those supported by Avro's JSON format. This list includes _int_, _long_, _string_, _boolean_, _float_, _double_, _null_, and _bytes_.

### Logical Types
Some of the logical types supported by Avro's JSON format are also supported by Avro IDL. The currently supported types are:

* _decimal_ (logical type decimal)
* _date_ (logical type date)
* _time_ms_ (logical type time-millis)
* _timestamp_ms_ (logical type timestamp-millis)

TODO FIX LINKS ABOVE

For example:
```java
record Job {
  string jobid;
  date submitDate;
  time_ms submitTime;
  timestamp_ms finishTime;
  decimal(9,2) finishRatio;
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
Default values for fields may be optionally specified by using an equals sign after the field name followed by a JSON expression indicating the default value. This JSON is interpreted as described in the spec. TODO fix link!

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
Note that the same restrictions apply to Avro IDL unions as apply to unions defined in the JSON format; namely, a record may not contain multiple elements of the same type. Also, fields/parameters that use the union type and have a default parameter must specify a default value of the same type as the **first** union type.

Because it occurs so often, there is a special shorthand to denote a union of `null` with another type. In the following snippet, the first three fields have identical types:

```java
record RecordWithUnion {
  union { null, string } optionalString1 = null;
  string? optionalString2 = null;
  string? optionalString3; // No default value
  string? optionalString4 = "something";
}
```

Note that unlike explicit unions, the position of the `null` type is fluid; it will be the first or last type depending on the default value (if any). So in the example above, all fields are valid.

## Defining RPC Messages
The syntax to define an RPC message within a Avro IDL protocol is similar to the syntax for a method declaration within a C header file or a Java interface. To define an RPC message add which takes two arguments named _foo_ and _bar_, returning an _int_, simply include the following definition within the protocol:
```java
int add(int foo, int bar = 0);
```
Message arguments, like record fields, may specify default values.

To define a message with no response, you may use the alias _void_, equivalent to the Avro _null_ type:
```java
void logMessage(string message);
```
If you have previously defined an error type within the same protocol, you may declare that a message can throw this error using the syntax:
```java
void goKaboom() throws Kaboom;
```
To define a one-way message, use the keyword `oneway` after the parameter list, for example:
```java
void fireAndForget(string message) oneway;
```

## Other Language Features

### Comments
All Java-style comments are supported within a Avro IDL file. Any text following _//_ on a line is ignored, as is any text between _/*_ and _*/_, possibly spanning multiple lines.

Comments that begin with _/**_ are used as the documentation string for the type or field definition that follows the comment.

### Escaping Identifiers
Occasionally, one will need to use a reserved language keyword as an identifier. In order to do so, backticks (`) may be used to escape the identifier. For example, to define a message with the literal name error, you may write:
```java
void `error`();
``` 
This syntax is allowed anywhere an identifier is expected.

### Annotations for Ordering and Namespaces
Java-style annotations may be used to add additional properties to types and fields throughout Avro IDL.

For example, to specify the sort order of a field within a record, one may use the `@order` annotation before the field name as follows:
```java
record MyRecord {
  string @order("ascending") myAscendingSortField;
  string @order("descending")  myDescendingField;
  string @order("ignore") myIgnoredField;
}
``` 
A field's type may also be preceded by annotations, e.g.:
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
Some annotations like those listed above are handled specially. All other annotations are added as properties to the protocol, message, schema or field.

## Complete Example
The following is a complete example of a Avro IDL file that shows most of the above features:
```java
/**
 * An example protocol in Avro IDL
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

    union { MD5, null} @aliases(["hash"]) nullableHash;

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
Additional examples may be found in the Avro source tree under the `src/test/idl/input` directory.