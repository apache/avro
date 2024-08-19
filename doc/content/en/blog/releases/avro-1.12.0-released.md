---
title: "Avro 1.12.0"
linkTitle: "Avro 1.12.0"
date: 2024-08-05
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

The Apache Avro community is pleased to announce the release of Avro 1.12.0!

All signed release artifacts, signatures and verification instructions can be found <a href="{{< relref "/project/download" >}}">here</a>

## Changes

### Sub-task

- [AVRO-3122]: TestAvroKeyOutputFormat and other avro-mapred tests fail with Java 17
- [AVRO-3308]: Include a curated list of resources
- [AVRO-3384]: Define C# Coding Style Guidelines
- [AVRO-3449]: Add an onboarding guide for contributors, committers and PMC
- [AVRO-3458]: Add test coverage for GenericRecord
- [AVRO-3488]: Fix Spelling Mistakes
- [AVRO-3490]: Fix IDE0016 Use throw expression
- [AVRO-3491]: Fix IDE0020 Use pattern matching to avoid 'is' check followed by a cast
- [AVRO-3497]: Fix IDE0075 Simplify conditional expression
- [AVRO-3499]: Fix IDE0079 Remove unnecessary suppression
- [AVRO-3538]: Improve the contributions page
- [AVRO-3700]: Publish Java SBOM artifacts with CycloneDX
- [AVRO-3813]: Use list of primitiv
- [AVRO-3826]: Commons test for C++ module
- [AVRO-3916]: Add nanos support for the Rust SDK
- [AVRO-3926]: [Rust] Allow UUID to serialize to Fixed[16]

### Bug fixes

- [AVRO-265]: Protocol namespace always written out in toJson
- [AVRO-1318]: Python schema should store fingerprints
- [AVRO-1463]: Undefined values cause warnings when unions with null serialized
- [AVRO-1517]: Unicode strings are accepted as bytes and fixed type by perl API
- [AVRO-1521]: Inconsistent behavior of Perl API with 'boolean' type
- [AVRO-1523]: Perl API: int/long type minimum value checks are off by one
- [AVRO-1737]: Unhashable type: 'RecordSchema'
- [AVRO-1830]: Avro-Perl DataFileReader chokes when avro.codec is absent
- [AVRO-2254]: Unions with 2 records declared downward fail
- [AVRO-2284]: Incorrect EnumSymbol initialization in TestReadingWritingDataInEvolvedSchemas.java
- [AVRO-2498]: UUID generation is not working avro 1.9 version
- [AVRO-2598]: C++ standard of library implies C++ standard of projects using Avro
- [AVRO-2722]: impl/DataFile.cc use of boost::mt19937 for DataFileWriteBase::makeSync is not thread safe
- [AVRO-2771]: Java 1.9.X doesn't allow having Error in a Record
- [AVRO-2862]: C# Primitive Schema losing metadata
- [AVRO-2883]: Avrogen (csharp) namespace mapping missing for references
- [AVRO-2885]: Providing a decimal number in an int field doesn't return an error
- [AVRO-2943]: Map comparison between Utf8 and String keys fails
- [AVRO-2987]: pkg-config has a broken `Requires:` section
- [AVRO-3003]: c# apache avro codegen - default value for enum types are not setting up properly
- [AVRO-3133]: EnumAdjust.resolve should compare unqualified name rather than full name
- [AVRO-3216]: Rust: failure reading multiple use of named schemas in file
- [AVRO-3232]: Rust deserializer: add missing matches to deserialize_any union and string/map
- [AVRO-3234]: Rust: Add new codec: zstandard
- [AVRO-3240]: Schema deserialization is not backwards compatible
- [AVRO-3259]: When opening an avro file which is encoded with anything besides none and deflate, it defaults to none and then returns garbage.
- [AVRO-3273]: [Java] avro-maven-plugin breaks on old versions of Maven
- [AVRO-3316]: [Rust] build breaks in docker build
- [AVRO-3322]: JavaScript: Buffer is not defined in browser environment
- [AVRO-3331]: Rust: Cannot extract Decimal value
- [AVRO-3350]: Validate that Default value is found in Enum
- [AVRO-3386]: [PHP] Build failing on github and travis
- [AVRO-3410]: [Rust] lint failure
- [AVRO-3433]: Rust: The canonical form should preserve schema references
- [AVRO-3448]: Rust: Encoding Panic with valid schema and input
- [AVRO-3452]: [rust] Derive Deserialize produces invalid Name struct
- [AVRO-3460]: [rust] Value::validate does not validate against Schema Refs
- [AVRO-3461]: [rust] Resolution Flow does not handle schema Refs
- [AVRO-3466]: Rust: serialize Schema to JSON loses inner namespace names
- [AVRO-3468]: Default values for logical types not supported
- [AVRO-3471]: Microseconds logical types are rounded to milliseconds
- [AVRO-3481]: Input and output variable type mismatch
- [AVRO-3482]: DataFileReader should reuse MAGIC data read from inputstream
- [AVRO-3486]: Protocol namespace not parsed correctly if protocol is defined by full name
- [AVRO-3495]: Rust: Record serialization is sensitive to order of fields in struct
- [AVRO-3511]: Rust: Fix the parsing of decimal logical type
- [AVRO-3516]: [rust] Avro Derive not working outside of repo context
- [AVRO-3529]: [Rust][branch-1.11] Cargo.toml is a virtual manifest, requires actual package
- [AVRO-3534]: Rust: Use dependency-review-action only for pull_request events
- [AVRO-3536]: Union type not inheriting type conversions
- [AVRO-3549]: [rust] Avro reader fails if it tries to read data compressed with codec that is not enabled in features
- [AVRO-3560]: avro ignores input after end of avsc json
- [AVRO-3568]: C# ToParsingForm normalizes logical type to "logical" rather than base type
- [AVRO-3581]: Usage of deprecated configuration properties in Velocity
- [AVRO-3585]: Unable to encode Value::String as Schema::UUID
- [AVRO-3587]: C: Fix possible heap-buffer-overflow in avro::DataFileReaderBase::readDataBlock()
- [AVRO-3595]: Release Notes missing for 1.11.1
- [AVRO-3597]: Recent changes in GenericDatumReader.java break compatibility
- [AVRO-3601]: C++ API header contains breaking include
- [AVRO-3612]: Report specific location of incompatibility in record schema
- [AVRO-3613]: Unions cannot have more than one logical type in C#
- [AVRO-3617]: [C++] Integer overflow risks with Validator::count_ and Validator::counters_
- [AVRO-3618]: [Java] TestBinaryDecoder should check consistency with directBinaryDecoder
- [AVRO-3619]: [Java] TestBinaryDecoder should check consistency with directBinaryDecoder
- [AVRO-3622]: Python compatibility check fails if record with and without namespace are compared
- [AVRO-3625]: [Rust] UnionSchema.is_nullable() should return true if any of the variants is Schema::Null
- [AVRO-3631]: Fix serialization of structs containing Fixed fields
- [AVRO-3632]: Union defaults are not handled as per the specification
- [AVRO-3642]: GenericSingleObjectReader::read_value fails on non-exhaustive read
- [AVRO-3645]: Fix deserialization of enum with unit () type
- [AVRO-3650]: Fix C++ Build on Manjaro
- [AVRO-3656]: Vulnerabilities from dependencies - jackson-databind & commons-text
- [AVRO-3657]: Computation of initial buffer size in OutputBuffer makes no sense
- [AVRO-3659]: Typo in python example
- [AVRO-3662]: [Ruby] Ruby 2.6 CI workflow fails since a while
- [AVRO-3663]: rust crate apache_avro_derive creates invalid schemas for raw identifiers
- [AVRO-3667]: [Python] Python 3.10 CI test fails since a while
- [AVRO-3669]: Missing py.typed file
- [AVRO-3674]: Value::Record containing enums fail to validate when using namespaces in Schema
- [AVRO-3683]: Rust  Writer, Reader can't use Schemas with dependencies in other Schemas.   i.e. The output of Schema::parse_list
- [AVRO-3687]: Rust enum missing default
- [AVRO-3688]: Schema resolution panics when a custom record field is included multiple times
- [AVRO-3698]: [Java] SpecificData.getClassName must replace reserved words
- [AVRO-3706]: AVDL nested imports cannot be resolved if path contains spaces
- [AVRO-3712]: C++  Build Failure on Manjaro
- [AVRO-3724]: C# JsonEncoder can't handle nested array of records
- [AVRO-3737]: [C] memcheck_test_avro_commons_schema is failing
- [AVRO-3738]: [Build][C#] The release build fails with .NET 7.0 target
- [AVRO-3747]: Make serde `is_human_readable` configurable
- [AVRO-3748]: issue with DataFileSeekableInput.SeekableInputStream.skip
- [AVRO-3749]: incorrect conflicting field when field name starts with symbols
- [AVRO-3751]: FastReaderBuilder in multithread lead to infinite loop also blocking other threads
- [AVRO-3755]: [Rust] Deserialization fails for reader schema with namespace
- [AVRO-3756]: Support writing types back to the user in memory without writing files to disk
- [AVRO-3767]: [Rust] Fix ref resolving in Union
- [AVRO-3772]: [Rust] Deserialize Errors for an Unknown Enum Symbol instead of Returning Default
- [AVRO-3773]: [Ruby] Decimal logical type fail to validate default
- [AVRO-3775]: [Ruby] decimal default is not converted to BigDecimal
- [AVRO-3780]: [Rust] Bug: decimal logical type usage through Fixed schema
- [AVRO-3782]: [Rust] Incorrect decimal resolving
- [AVRO-3785]: [Rust] Deserialization if reader schema has a namespace and a union with null and a record containing a reference type
- [AVRO-3786]: [Rust] Deserialization results in FindUnionVariant error if the writer and reader have the same symbol but at different positions
- [AVRO-3787]: [Rust] Deserialization fails to use default if an enum in a record in a union is given an unknown symbol
- [AVRO-3800]: profile section should be declared in the root package.
- [AVRO-3809]: Faulty validation of a type reference with implicit nested namespace
- [AVRO-3814]: [Rust] Schema resolution fails when extending a nested record with a union type
- [AVRO-3818]: Enclosing namespace should be inherited to the inner named types if they have no their own namespaces
- [AVRO-3820]: Don't allow invalid field names
- [AVRO-3821]: Rust: Record (de?)serialization is sensitive to order of fields in struct
- [AVRO-3823]: Show helpful error messages
- [AVRO-3824]: The instruction for building the website should be more precise
- [AVRO-3827]: Disallow duplicate field names
- [AVRO-3830]: Handle namespace properly if a name starts with dot
- [AVRO-3837]: Disallow invalid namespaces for the Rust binding
- [AVRO-3846]: Race condition can happen among serde tests
- [AVRO-3847]: Record field doesn't accept default value if field type is union and the type of default value is pre-defined name
- [AVRO-3849]: [Rust] "make readme" doesn't work
- [AVRO-3855]: [rust] lint/clippy fails in ubertool
- [AVRO-3858]: [Build] Add some config to ./build.sh sign
- [AVRO-3859]: [Build][C#] build.sh clean fails to remove some C# files
- [AVRO-3861]: [Build] Add RAT exclusions for python docs
- [AVRO-3865]: [Build][perl] Files are leftover after a build
- [AVRO-3866]: [Build][Python] Files are leftover after a build
- [AVRO-3876]: JacksonUtils is not symmetric
- [AVRO-3881]: Writer ignores user metadata when the body is empty
- [AVRO-3888]: CVE with common compress
- [AVRO-3889]: Maven Plugin Always Recompiles IDL Files
- [AVRO-3894]: [Rust] Record field aliases are not taken into account when serializing
- [AVRO-3897]: Disallow invalid namespace in fully qualified name for Rust SDK
- [AVRO-3898]: [rust] compatibility fails with different namespaces
- [AVRO-3899]: [Rust] Invalid logical types should be ignored and treated as the underlying type
- [AVRO-3912]: Issue with deserialization for BigDecimal in rust
- [AVRO-3925]: [Rust]Decimal type serialization/deserialization is incorrect.
- [AVRO-3928]: Avro Rust cannot parse default int logical-type date in a valid schema
- [AVRO-3932]: [C]: fix variable reference in CMakeLists.txt
- [AVRO-3940]: Failed to generate Java classes from multiple .avsc files containing same type
- [AVRO-3953]: C# CodeGen.cs:503 incorrectly throws for "reserved keywords"
- [AVRO-3955]: [Rust] unable to decode string enum from avro encoded data
- [AVRO-3956]: NPE when calling Protocol#equals or hashCode
- [AVRO-3957]: Fix typos in docs and examples
- [AVRO-3964]: [Rust] Out-of-bounds panic
- [AVRO-3970]: [Rust] incorrect compatibility checks with logicalType uuid
- [AVRO-3974]: [Rust] incorrect compatibility checks with ref fields
- [AVRO-3990]: [C++] avrogencpp generates invalid code for union with a reserved word
- [AVRO-4004]: [Rust] Canonical form transformation does not strip the logicalType
- [AVRO-4006]: [Java] DataFileReader does not correctly identify last sync marker when reading/skipping blocks
- [AVRO-4011]: Schema generated via AvroSchema is not compatible with itself
- [AVRO-4014]: [Rust] Sporadic value-schema mismatch with fixed struct

### New Features

- [AVRO-3223]: Support optional codecs in C# library
- [AVRO-3358]: Update documentation in DataFileReader
- [AVRO-3388]: Implement extra codecs for C# as seperate nuget packages
- [AVRO-3506]: [rust] Implement Single Object Writer
- [AVRO-3507]: [rust] Implement Single Object Reader
- [AVRO-3591]: Improve interoperability tests with a common test suite
- [AVRO-3592]: [C#] New packages are not included in the build distribution
- [AVRO-3666]: New schema parser for all supported schema formats
- [AVRO-3677]: Introduce Named Schema Formatters
- [AVRO-3678]: [Rust] Support write float value to field defined as double
- [AVRO-3725]: fix documentation of functions and variables
- [AVRO-3764]: [Rust] Add schemata-based resolve method
- [AVRO-3872]: [Build][C#] Warning on nuget upload about README
- [AVRO-3922]: Add timestamp-nanos support to Ruby

### Improvements

- [AVRO-312]: Generate documentation for Python with Sphinx
- [AVRO-530]: allow for mutual recursion in type definitions
- [AVRO-1496]: Avro aliases support for C++
- [AVRO-1514]: Clean up perl API dependencies
- [AVRO-1938]: Python support for generating canonical forms of schema
- [AVRO-2307]: Opt-in setting to improve GC behavior during deserialization?
- [AVRO-2397]: Implement Alias Support for C++
- [AVRO-2717]: Fix undefined behaviour in ZigZag encoding if Avro was compiled with the C++ standard less than C++20.
- [AVRO-3001]: JsonEncode Decode support for C#
- [AVRO-3043]: Remove redundant generic casts
- [AVRO-3078]: C#: Logical type 'local-timestamp-millis'
- [AVRO-3084]: Fix JavaScript interop test to read files generated by other languages on CI
- [AVRO-3120]: Support Next Java LTS (Java 17)
- [AVRO-3214]: Rust: Support "doc" for FixedSchema
- [AVRO-3245]: Rust: Replace crc crate with crc32fast
- [AVRO-3246]: Rust: Add new codec: bzip2
- [AVRO-3248]: Rust: Support named types in UnionSchema
- [AVRO-3255]: [Ruby] specify rubygems_mfa_required in gemspec metadata
- [AVRO-3264]: Improve the Avro landing page
- [AVRO-3274]: Request for C# API to implement a JSON Encoder
- [AVRO-3284]: Rust: Upgrade to digest 0.10
- [AVRO-3285]: Upgrade JavaCC and plugin
- [AVRO-3292]: Bump Microsoft.NET.Test.Sdk from 16.11.0 to 17.0.0 in /lang/csharp
- [AVRO-3302]: Rust: Implement interop tests for the Rust module
- [AVRO-3303]: Rust: Add support for Xz codec
- [AVRO-3306]: Java: Build failure with JDK 18+
- [AVRO-3312]: Rust: Use u32 instead of i32 for the Enum/Union's index field
- [AVRO-3314]: ArgumentOutOfRangeException thrown in AvroDecimal IConvertable.ToType
- [AVRO-3315]: Rust: Add support to back/cycle reference an alias
- [AVRO-3317]: JavaScript: Update dependencies
- [AVRO-3318]: Java: Bump slf4j.version from 1.7.32 to 1.7.33 in /lang/java
- [AVRO-3319]: Rust: Update zstd requirement from 0.9.0+zstd.1.5.0 to 0.10.0+zstd.1.5.0 in /lang/rust
- [AVRO-3320]: C#: Bump NUnit3TestAdapter from 4.2.0 to 4.2.1 in /lang/csharp
- [AVRO-3321]: Java: Bump commons-cli from 1.4 to 1.5.0 in /lang/java
- [AVRO-3323]: Remove suppression of CS1591 from AvroDecimal
- [AVRO-3324]: Add omitted braces in AvroDecimal
- [AVRO-3325]: Remove suppression of CA2225 in AvroDecimal
- [AVRO-3326]: Styling - Elements should not be on a single line in AvroDecimal
- [AVRO-3327]: Use Pattern Matching to avoid is check followed by cast
- [AVRO-3328]: Documentation update for CodeGen class
- [AVRO-3329]: Add omitted braces in CodeGen class
- [AVRO-3330]: Avrogen avsc compiler should return 0 exit code if help requested
- [AVRO-3333]: Spacing styling issues in CodeGen class
- [AVRO-3334]: Simplify getNullableType in CodeGen
- [AVRO-3335]: Throw exception for null parameter in GenerateNames
- [AVRO-3336]: Deprecate obsolete namespace lookup in CodeGen
- [AVRO-3337]: C#: Bump Log4net to a newer version
- [AVRO-3340]: Enable standard code analysis and Intellisense
- [AVRO-3341]: Update documentation of CodeGenException
- [AVRO-3342]: Update documentation in CodeGenUtil
- [AVRO-3343]: Update codec to styling standards
- [AVRO-3344]: C#: Remove DataBlock class
- [AVRO-3345]: Resolve unnecessary suppression of CA1052 in DataFileConstants
- [AVRO-3346]: Update documentation to meet standards in DataFileReader
- [AVRO-3347]: Update AddNamespace in CodeGen to meet styling guidelines
- [AVRO-3348]: Update ProcessSchemas to meet styling guidelines
- [AVRO-3349]: Update ProcessProtocols to meet styling guidelines
- [AVRO-3352]: Use required minimum package version fo Newtonsoft only
- [AVRO-3353]: Simplify naming in CodeGen
- [AVRO-3354]: Simplify If statements in CodeGen
- [AVRO-3355]: Fix order of Access Modifier in Codec
- [AVRO-3356]: Simplify naming in DataFileReader
- [AVRO-3357]: Properties only assigned in constructors should be marked readonly
- [AVRO-3359]: Updated formatting in DeflateCodec
- [AVRO-3360]: Update Header XML Documentation
- [AVRO-3361]: Simplify if statement in NullCodec
- [AVRO-3366]: Fix naming in GenericEnum
- [AVRO-3367]: Remove unnecessary suppression of CA1307 from GenericEnum
- [AVRO-3377]: Deserialization of record of mangled Java class throws ClassCastException
- [AVRO-3404]: Extend the IDL syntax to serve as a .avsc equivalent as well
- [AVRO-3405]: add API for user-provided metadata when writing to Object Container File
- [AVRO-3407]: Test for user metadata in the interop tests
- [AVRO-3415]: Add C# code coverage support
- [AVRO-3416]: Benchmarking project for C#
- [AVRO-3418]: [Rust] Fix clippy errors for Rust 1.59.0
- [AVRO-3421]: Add tests for ArraySchema
- [AVRO-3424]: C# Add support to parse string into Schema.Type
- [AVRO-3427]: Add command line option to skip creation of directories based on namespace path
- [AVRO-3434]: .NET/#C: Support LogicalSchema for ReflectReader/Writer
- [AVRO-3435]: Add --version to avrogen
- [AVRO-3450]: Document IDL support in IDEs
- [AVRO-3451]: fix poor Avro write performance
- [AVRO-3453]: C# Avrogen Add Generated Code Attribute
- [AVRO-3464]: Rust: Print user frientlier output for the 'benchmark' example
- [AVRO-3465]: Add avrogen protocol tests
- [AVRO-3467]: Use oracle-actions to test with  Early Access JDKs
- [AVRO-3469]: Build and test using .NET SDK 7.0 in guthub action
- [AVRO-3474]: Increase read performance by moving CanRead to constructor
- [AVRO-3475]: Enforce time-millis and time-micros specification
- [AVRO-3477]: Add unit tests for logical types with fixed base type
- [AVRO-3479]: [rust] Derive Avro Schema macro
- [AVRO-3483]: [Rust] Log error messages with a reason when the validation fails
- [AVRO-3484]: Rust: Implement derive default via annotation
- [AVRO-3485]: Rust: Implement derive doc via annotation
- [AVRO-3487]: Java: Bump Jackson to 2.12.6.1
- [AVRO-3489]: JavaScript: Replace istanbul with nyc for code coverage
- [AVRO-3492]: Rust: Implement derive aliases via annotation
- [AVRO-3496]: Rust: Use visitor.visit_borrowed_str() when possible
- [AVRO-3498]: Deprecate NameCtorKey
- [AVRO-3500]: Rust: Use property based testing for avro_derive IT tests
- [AVRO-3501]: Rust: Enable Github Actions caching for the Rust CI
- [AVRO-3502]: Rust: Wrong [ORDER] for Parsing Canonical Form
- [AVRO-3510]: PHP build fails on Travis
- [AVRO-3517]: Rust: Optimize crates' size by disabling default features of the dependencies
- [AVRO-3518]: Rust: Represent aliases as Name instead of String
- [AVRO-3522]: Rust: Setup better logging and colored stacktraces for the tests
- [AVRO-3526]: Rust: Improve resolving Bytes and Fixed from string
- [AVRO-3527]: Generated equals() and hashCode() for SpecificRecords
- [AVRO-3530]: Rust: Use dependency-review-action for Rust
- [AVRO-3533]: Rust: Update dependencies
- [AVRO-3542]: Scale assignment optimization
- [AVRO-3543]: Support wasm32 compilation target for Rust library
- [AVRO-3547]: support custom attribute at field level
- [AVRO-3554]: Create original art for the Avro logo
- [AVRO-3579]: Java Test : From Junit4 to JUnit5
- [AVRO-3586]: Make Avro Build Reproducible
- [AVRO-3599]: Rust: Make apache-avro-test-helper releasable
- [AVRO-3600]: [Rust] UnionSchema::new method should be public
- [AVRO-3602]: Support Map(with non-String keys) and Set in ReflectDatumReader
- [AVRO-3608]: Rust: Fix clippy errors in Rust 1.63.0
- [AVRO-3609]: support custom attributes
- [AVRO-3610]: [C++] Upgrade from C++ 11 to C++ 17
- [AVRO-3611]: org.apache.avro.util.RandomData generates invalid test data
- [AVRO-3616]: [C++]: Fix compilation warnings
- [AVRO-3621]: [Rust] Improved resolution of nullable record fields
- [AVRO-3623]: Improve the PULL_REQUEST_TEMPLATE
- [AVRO-3624]: Fix Avro website checks on whimsy
- [AVRO-3630]: [Rust] Make it possible to extend pre-existing Avro bytes
- [AVRO-3633]: Additional attributes for 'avro_derive' crate
- [AVRO-3634]: Implement AvroSchemaComponent for bool
- [AVRO-3639]: [Rust] Derive implementation for Eq where possible
- [AVRO-3644]: [JAVA] Support java.util.Optional in reflect package
- [AVRO-3649]: [JAVA] reorder union types to match default value
- [AVRO-3658]: Bump jackson to address CVE-2020-36518
- [AVRO-3660]: SpecificRecord java data generator helper method - should I contribute?
- [AVRO-3679]: [Rust] Enable 'perf' feature of regex dependency
- [AVRO-3692]: Serde flatten is not supported when deserializing
- [AVRO-3693]: avrogencpp Invalid type for union exception does not identify which union
- [AVRO-3704]: Naming rules : multiple choice
- [AVRO-3705]: avrogencpp needs an option to generate code using std instead of boost
- [AVRO-3708]: [Rust] Fix clippy warnings introduced with Rust 1.67.0
- [AVRO-3709]: [Rust] Add aliases to RecordField
- [AVRO-3711]: Add documentation about uuid in IDL
- [AVRO-3721]: [Java] Add cache to org.apache.avro.JsonProperties.getObjectProps
- [AVRO-3722]: Eagerly Initialize Instance Variables in Ruby Implementation
- [AVRO-3723]: [Rust] Make schema::ResolvedSchema and schema::Names public
- [AVRO-3727]: Add RollForward to C# avrogen tool
- [AVRO-3741]: Note about the version requirement of Rust in BUILD.md
- [AVRO-3742]: Bump maven-plugin-plugin from 3.8.1 to 3.8.2
- [AVRO-3743]: Bump cyclonedx-maven-plugin from 2.7.6 to 2.7.7
- [AVRO-3744]: Bump maven-checkstyle-plugin from 3.2.1 to 3.2.2
- [AVRO-3745]: Bump zstd-jni from 1.5.4-2 to 1.5.5-2
- [AVRO-3746]: Bump grpc.version from 1.54.0 to 1.54.1
- [AVRO-3757]: [rust] Update syn to 2.x
- [AVRO-3758]: [Rust] Use AtomicXyz types instead of static mutable ones
- [AVRO-3759]: [Rust] Schema types inconsistency
- [AVRO-3766]: [Rust] Print friendlier errors when test cases fail
- [AVRO-3771]: [Rust] Logging flood during validate method
- [AVRO-3779]: Any big decimal conversion
- [AVRO-3784]: [Rust] Make Decimal more usable until its rewritten
- [AVRO-3790]: [RUBY] Missing default namespace information in SchemaParseError
- [AVRO-3794]: [Rust] Do not fail the shared tests when the shared folder is not available
- [AVRO-3799]: Enable the schema parser to read and parse  from input streams for Rust binding
- [AVRO-3812]: Handle null namespace properly for canonicalized schema representation
- [AVRO-3815]: Broken indentation in the specification doc
- [AVRO-3828]: [Rust] Use newer Github actions for setting up Rust
- [AVRO-3829]: JUnit4 to JUnit5 : continue
- [AVRO-3833]: Spec: clarify usage names and aliases
- [AVRO-3835]: [Rust] Get rid of byteorder and zerocopy dependencies
- [AVRO-3836]: [Rust] Fix the build with Rust 1.65.0
- [AVRO-3838]: [Rust] Replace regex crate with regex-lite
- [AVRO-3839]: [Rust] Replace lazy_static crate with std::sync::OnceLock
- [AVRO-3844]: [Rust] Fix clippy errors with Rust 1.72.0
- [AVRO-3851]: Validate default value for record fields and enums on parsing
- [AVRO-3852]: Support Java 21
- [AVRO-3853]: Support local-timestamp logical types for the Rust SDK
- [AVRO-3862]: Add aliases and doc methods to Schema in Rust SDK
- [AVRO-3863]: Delete temporary test data after tests finish
- [AVRO-3868]: Check consistency between the doc comment in lib.rs and README.md
- [AVRO-3870]: Speed up CI for Rust
- [AVRO-3871]: Add BlockingDirectBinaryEncoder
- [AVRO-3877]: [doc] fix wrong configuration for avro-maven-plugin in java example
- [AVRO-3878]: Rename default git branch to be 'main'
- [AVRO-3879]: [Build][Python] Fix `./build.sh clean` to remove the generated Python documents
- [AVRO-3880]: Upgrade maven-antrun-plugin to 3.1.0
- [AVRO-3884]: Add local-timestamp-nanos and timestamp-nanos
- [AVRO-3885]: Update the maillist link
- [AVRO-3886]: [Rust] Serialize attribute in schema to support custom logical type
- [AVRO-3887]: Remove redundant casts
- [AVRO-3891]: Remove redundant cast from DirectBinaryDecoder
- [AVRO-3892]: [Rust] support to convert bytes to fixed in resolve_fixed
- [AVRO-3896]: [Rust] support read schema with custom logical type
- [AVRO-3900]: Permissiveness in schema namespaces for rust SDK?
- [AVRO-3901]: [Rust] Better serde union support
- [AVRO-3904]: [rust] Sometimes when calculating schema compatibility the code panics but maybe it should not
- [AVRO-3905]: [Rust] Fix clippy error with Rust 1.74.0
- [AVRO-3910]: [Rust] Replace `color-backtrace` with `better-panic` for the tests
- [AVRO-3914]: Add nanos support for the Java SDK
- [AVRO-3917]: [Rust] Field aliases are not taken into account when calculating schema compatibility
- [AVRO-3918]: Allow UUID to serialize to Fixed[16]
- [AVRO-3919]: Add UUID type example
- [AVRO-3920]: [Rust] Serialize custom attribute in RecordField
- [AVRO-3923]: Add Avro 1.11.3 release blog
- [AVRO-3927]: [Rust] support custom attributes in list and map
- [AVRO-3935]: Support logical types in Rust Schema Compatibility checks
- [AVRO-3936]: Clean up NOTICE file
- [AVRO-3938]: Schema.Parser.validate should not be null
- [AVRO-3939]: [Rust] Make it possible to use custom schema comparators
- [AVRO-3942]: MemoryOutputStream yields a compiler warning
- [AVRO-3943]: Unused folders
- [AVRO-3948]: [Rust] Re-export bigdecimal::BigDecimal as apache_avro::BigDecimal
- [AVRO-3949]: [Rust]: Add support for serde to apache_avro::Decimal
- [AVRO-3950]: [rust] Some code when checking schema compatibility is never reached
- [AVRO-3958]: Update min CMake version to 3.5
- [AVRO-3959]: Avoid deprecated OSX atomic ops
- [AVRO-3960]: Fix st ANYARGS warnings
- [AVRO-3961]: Add AVRO_INVALID to avro_type_t
- [AVRO-3962]: [Rust] avro-derive supports extract docs from field comments
- [AVRO-3977]: Fix failing typecheck in Python 3.12
- [AVRO-3981]: Close SyncableFileOutputStream
- [AVRO-3982]: Use String.isEmpty() instead
- [AVRO-3983]: Allow setting a custom encoder in DataFileWriter
- [AVRO-3985]: Restrict trusted packages in ReflectData and SpecificData
- [AVRO-3992]: [C++] Encoding a record with 0 fields in a vector throws
- [AVRO-3994]: [C++] Solidus (/) should not be escaped in JSON output
- [AVRO-3995]: [C++] Update build system to disallow compiling with unsupported language versions
- [AVRO-3998]: Switch Perl library from JSON::XS to JSON::MaybeXS
- [AVRO-3999]: Avoid warnings in Perl test suite
- [AVRO-4007]: [Rust] Faster is_nullable for UnionSchema
- [AVRO-4010]: Avoid resolving schema on every call to read()
- [AVRO-4013]: PHP 8 Deprecations
- [AVRO-4015]: avro-cpp does not work with CMake's FetchContent
- [AVRO-4016]: Remove the use of MD5 in org.apache.avro.file.DataFileWriter#generateSync
- [AVRO-4019]: [C++] Correct signedness of validator methods
- [AVRO-4022]: Revive docker image

### Testing

- [AVRO-3277]: Test against Ruby 3.1
- [AVRO-3278]: Drop support for Ruby 2.6
- [AVRO-3558]: Rust: Add a demo crate that shows usage as WebAssembly
- [AVRO-3696]: [Python] Replace tox-wheel with upstream tox 4
- [AVRO-3697]: Test against Ruby 3.2
- [AVRO-3701]: Add github action to validate maven 4 build compatibility
- [AVRO-3921]: Test against Ruby 3.3

### Wishes

- [AVRO-1757]: Serialize Avro schema objects to avdl file (IDL format)
- [AVRO-2211]: SchemaBuilder equivalent or other means of schema creation
- [AVRO-3197]: Rust: Disable logical type on failure

### Tasks

- [AVRO-3205]: Rust: Update Cargo.toml [package] information
- [AVRO-3241]: [Java] Publish SNAPSHOT artifacts
- [AVRO-3242]: Use TravisCI for testing Apache Avro on Linux ARM64
- [AVRO-3247]: Rust: Run MIRI checks
- [AVRO-3281]: Bump zstd-jni from 1.5.0-4 to 1.5.1-1 in /lang/java
- [AVRO-3282]: Bump grpc.version from 1.42.1 to 1.43.1 in /lang/java
- [AVRO-3283]: Update zerocopy requirement from 0.3.0 to 0.6.1 in /lang/rust
- [AVRO-3304]: avro-tools Update log4j dependency for critical vulnerability
- [AVRO-3309]: Bump NUnit.ConsoleRunner from 3.13.2 to 3.14.0 in /lang/csharp
- [AVRO-3310]: Bump build-helper-maven-plugin from 3.2.0 to 3.3.0 in /lang/java
- [AVRO-3311]: Bump grpc.version from 1.43.1 to 1.43.2 in /lang/java
- [AVRO-3332]: Java: Bump grpc.version from 1.43.2 to 1.44.0 in /lang/java
- [AVRO-3339]: Rust: Rename crate from avro-rs to apache-avro
- [AVRO-3351]: C#: Bump System.Reflection.Emit.Lightweight from 4.3.0 to 4.7.0 in /lang/csharp
- [AVRO-3372]: Java: Bump archetype-plugin.version from 3.2.0 to 3.2.1 in /lang/java
- [AVRO-3373]: Java: Bump protobuf-java from 3.19.1 to 3.19.4 in /lang/java
- [AVRO-3391]: Update typed-builder requirement from 0.9.1 to 0.10.0 in /lang/rust
- [AVRO-3409]: [Java] Bump Reload4j to 1.2.19
- [AVRO-3419]: [Rust] Update strum 0.23.1 and strum_macros to 0.24.0
- [AVRO-3422]: Bump jetty.version from 9.4.44.v20210927 to 9.4.45.v20220203 in /lang/java
- [AVRO-3428]: Rust: Restructure the RUST SDK to a Rust workspace
- [AVRO-3431]: CI: Cancel in-progress workflows if there are new commits in PR
- [AVRO-3432]: Java: Bump grpc.version from 1.44.0 to 1.44.1 in /lang/java
- [AVRO-3437]: Rust: Update dependencies
- [AVRO-3439]: Java: Bump netty-bom from 4.1.72.Final to 4.1.74.Final in /lang/java
- [AVRO-3455]: Java: Bump netty-bom from 4.1.74.Final to 4.1.75.Final
- [AVRO-3456]: Rust: Update zstd requirement from 0.10.0+zstd.1.5.2 to 0.11.0+zstd.1.5.2
- [AVRO-3457]: JS: Bump mocha from 9.2.1 to 9.2.2
- [AVRO-3462]: Java: Bump hadoop-client from 3.3.1 to 3.3.2
- [AVRO-3463]: Java: Bump grpc.version from 1.44.1 to 1.45.0
- [AVRO-3494]: Rust: uncomment some tests which pass
- [AVRO-3519]: Rust: Remove MIRI Github Actions check
- [AVRO-3552]: Rust: sort the contents in Cargo.toml files with cargo-tomlfmt
- [AVRO-3574]: Rust: Add Cargo.lock to Git
- [AVRO-3575]: Rust: Add a module for fuzzy testing
- [AVRO-3653]: [build] Move off Travis CI
- [AVRO-3661]: [Rust] Fix new clippy errors introduced with Rust 1.65
- [AVRO-3672]: Add CI testing for Python 3.11
- [AVRO-3681]: [Python] GitHub actions failing with python 3.6
- [AVRO-3682]: [Build] Remove forrest from Avro build
- [AVRO-3754]: upgrade to jackson 2.15.0
- [AVRO-3793]: [Rust] Bump minimum supported version of Rust to 1.65.0
- [AVRO-3808]: Drop support for Python 3.6, add Pypy 3.8-3.10
- [AVRO-3875]: [Rust]: Set "readme" metadata for each package separately
- [AVRO-3915]: [Rust] Extract dependencies used by more than one member crates into the workspace
- [AVRO-3937]: [Rust]: Use cargo-deny to check the dependencies' licenses
- [AVRO-3944]: Fix CMake warning
- [AVRO-3945]: Fix issues reported by cppcheck
- [AVRO-3967]: Replace boost::format with fmt
- [AVRO-3978]: Build with Java 11 minimum

## Language SDK / Convenience artifacts

* C#: https://www.nuget.org/packages/Apache.Avro/1.12.0
* Java: https://repo1.maven.org/maven2/org/apache/avro/avro/1.12.0/
* Javascript: https://www.npmjs.com/package/avro-js/v/1.12.0
* Perl: https://metacpan.org/release/Avro
* Python 3: https://pypi.org/project/avro/1.12.0
* Ruby: https://rubygems.org/gems/avro/versions/1.12.0
* Rust: https://crates.io/crates/apache-avro/0.17.0

Thanks to everyone for contributing!
