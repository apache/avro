# Change Log

## Unreleased
* Apply @Classpath annotation to classpath on `GenerateAvroProtocolTask`

## 0.20.0
* Built using Gradle 6.5
* Updated compatibility testing to include Java 14
* Updated compatibility testing through Gradle 6.5
* Add `ResolveAvroDependenciesTask` (#115)

## 0.19.1
* Fix schema dependency resolution when types are referenced with a `{ "type": NAME }` block rather than just `NAME` (#107)
* Eliminate `NullPointerException` handling in schema dependency resolution, as it no longer appears to be needed.

## 0.19.0
* Add support for Gradle 6.0-6.2.2 (#101)
* Drop support for Gradle versions prior to 5.1
* Update version of kotlin plugin in tests/example
* Built using Avro 1.9.2 (#104)
* Add support for Java 13
* Add support for testing multiple Kotlin versions
* Update plugin's own build to address some deprecation warnings of APIs being removed in Gradle 7
* Add tests for Kotlin DSL usage (#61)
* Support [Task Configuration Avoidance](https://docs.gradle.org/current/userguide/task_configuration_avoidance.html) (#97); thanks to [dcabasson](https://github.com/dcabasson) for the collaboration
* Upgrade Codenarc from 1.4 to 1.5
* Preliminary Java 14 support

## 0.18.0
* Use reproducible file order for plugin archives
* Eliminate usage of internal conventions API, using new Lazy Configuration approach instead; requires Gradle 4.4+
  * Technically, the APIs needed are available in Gradle 4.3, but there is a bug related to un-set `Property` instances in 4.3 and 4.3.1; see https://github.com/gradle/gradle/issues/3879
* Cleaned up compatibility code for older versions of Gradle
* Built using Gradle 5.6.2
* Upgrade Spock from 1.2 to 1.3
* Upgrade Checkstyle from 6.1.1 to 8.23 and adjust rules used
* Upgrade Codenarc from 1.0 to 1.4 and adjust rules used
* Change source compatibility to 8
* Modernized for Java 8
* Built using Avro 1.9.1
* GenerateAvroProtocolTask now has a `classpath` property; defaults to the runtime configuration when the Avro plugin is applied
* GenerateAvroProtocolTask now properly declares the `classpath` as an input; fixes #86; thanks to [RichSteele](https://github.com/RichSteele) for the bug report
* Fix handling of default `outputCharacterEncoding` (use of system default character set to match Java compiler)
* Add support for generating getters that return Optional (#90); contribution from [bspeakmon](https://github.com/bspeakmon)
* Add support for `logicalTypeFactories` and `customConversions`; fixes #92

## 0.17.0
* Built using Avro 1.9.0
* Removed configuration setting `validateDefaults`; defaults are now always validated due to an upstream change
* Java 7 is no longer supported, as Avro 1.9.0 is now Java 8+
* Began testing using Java 12

## 0.16.0
* Built using Gradle 4.10.2
* Updated compatibility testing through Gradle 4.10.2
* Added support for the Gradle [Build Cache](https://docs.gradle.org/current/userguide/build_cache.html) (#48); contribution from [dcabasson](https://github.com/dcabasson)
* Upgrade Spock from 1.0 to 1.2
* Update plugin publishing mode to address Gradle 5.0 deprecation warning

## 0.15.1
* Fix "Boolean configuration cannot be set with boolean values from Kotlin DSL" (#60)

## 0.15.0
* Built using Gradle 4.9
* Updated compatibility testing through Gradle 4.9
* Began testing using Java 11
* Add support for generating schema files (#56)
* Fix bug where `GenerateAvroProtocolTask` can't be used without a runtime configuration

## 0.14.2
* Stop creating default generated output directories when `outputDir` is customized and IntelliJ integration is used (#52)

## 0.14.1
* Built using Gradle 4.6
* Updated compatibility testing through Gradle 4.6
* Began testing using Java 10
* Began testing using Kotlin 1.2.31
* Fixed infinite loop when a schema file contains multiple definitions of the same type (#47)

## 0.14.0
* Built using Gradle 4.5
* Updated compatibility testing through Gradle 4.5
* Support for validation of default values in schema (#42)

## 0.13.0
* Remove pre-cleaning behavior from `GenerateAvroJavaTask` (#41)

## 0.12.0
* Improve support for Kotlin (#36)

## 0.11.0
* Built using Gradle 4.2.1
* Began testing using Java 9
* Built using Avro 1.8.2
* Breaking backward compatibility with Avro versions older than 1.8.2
* Add new configuration option "enableDecimalLogicalType" to generate `BigDecimal` for fields annotated with `logicalType` equals to `decimal`
* Breaking backward compatibility caused by "enableDecimalLogicalType" default value set `true`. `BigDecimal` will be used instead of old usage of `ByteBuffer`

## 0.10.0
* Drop support for Gradle 2.x
* As Gradle 3.0+ has a minimum Java version requiremenet of Java 7, drop support for Java 6
* Update source compatibility to Java 7
* Reduce access to utility methods not intended for re-use
* Stopped publishing to [Gradle plugin portal](https://plugins.gradle.org)
* Published to [Bintray](https://bintray.com/commercehub-oss/main/gradle-avro-plugin)
* MapUtils class is no longer public

## 0.9.1
* Built using Gradle 4.1
* Updated versions for cross-compatibility testing

## 0.9.0
* Built using Avro 1.8.1 (#23)
* Built using Gradle 2.13
* Added version cross-compatibility testing

## 0.8.1
* Compatible at runtime with Gradle 5; no functional changes.  Compiled with Gradle 5.6.

## 0.8.0
* Add support for Java 6 (#21)

## 0.7.0
* Remove usage of Apache Commons IO (#19)
* Add ability to retry processing of duplicate type definitions (#13)
* Renamed "encoding" option to "outputCharacterEncoding" to match Avro compiler
* Allowed setting "outputCharacterEncoding" to a `java.nio.charset.Charset` (in addition to a `String` charset name)
* Allowed setting "stringType" to a `org.apache.avro.generic.GenericData.StringType` (in addition to a String)
* Allowed setting "fieldVisibility" to a `org.apache.avro.compiler.specific.SpecificCompiler.FieldVisibility` (in addition to a String)
* Fixed handling of non-"true" String settings for "createSetters" option
* Automatically use encoding from `JavaCompile` task as "outputCharacterEncoding", if set
* Change default "outputCharacterEncoding" to system default to match `JavaCompile` task behavior (#20)

## 0.6.1
* Add Checkstyle ImportControl to prevent accidentally adding dependencies on libraries that Gradle makes available for build but not runtime.
* Remove usage of Guava (#18)

## 0.6.0
* Add new configuration option "templateDirectory" to set source directory for the Avro compiler's Velocity templates.
* Add new configuration option "createSetters" to allow suppressing the Avro compiler's creation of setters in created domain objects.
* Matching of fieldVisibility settings is now case-insensitive.
* Removed some excessive debug logging
* Built against Gradle 2.7
* Added Checkstyle and Codenarc to build
* Known Bug: doesn't work properly unless you manually add a dependency on guava; please upgrade to 0.6.1

## 0.5.0
* Add support for schemas/protocols/IDL in subdirectories of `src/main/avro`, etc. (#11)
* Expose original error messages from `avro-compiler` when compilation fails

## 0.4.0
* Add ability to specify fieldVisibility for generated Java source; contribution from [wooder79](https://github.com/wooder79)
* Removed support for unqualified plugin ID (just "avro")
* Published via new mechanism to [Gradle plugin portal](https://plugins.gradle.org)
* Stopped publishing to previous location on Bintray
* Built against Gradle 2.6; uses [test kit](https://docs.gradle.org/current/userguide/test_kit.html) for functional testing

## 0.3.4
* Fix registration of generated sources for compilation (#8)
* Change classloader handling to better support import of external dependencies (#9)

## 0.3.3
* Fix generation of Java files from .avdl files; contribution from [viacoban](https://github.com/viacoban)

## 0.3.2
* Improve handling when custom buildDir is used

## 0.3.1
* Fix extension support for configuring encoding
* Make default encoding UTF-8

## 0.3.0
* IntelliJ: register generated source directories even if they don't already exist.
* Add avro-base plugin, which exposes tasks and the extension without creating tasks, defaults, etc.
* Add support for configuring encoding

## 0.2.0
* Build against Gradle 1.12
* Compile using Avro 1.7.6
* Support for qualified plugin ID
* Deprecate unqualified plugin ID

## 0.1.3
* Always regenerate all Java classes when any schema file changes to avoid some classes having outdated schema information.

## 0.1.2
* Eliminate dependency on guava, make dependency on commons-io explicit

## 0.1.1
* Fixed NullPointerException when performing clean builds

## 0.1.0
* Add support for converting IDL files to JSON protocol declaration files
* Add support for generating Java classes from JSON protocol declaration files
* Add support for generating Java classes from JSON schema declaration files
* Add support for inter-dependent JSON schema declaration files
* Add support for tweaking source/exclude directories in IntelliJ
* Add support for specifying the string type to use in generated classes
