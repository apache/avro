# Change Log

* Unreleased
    * Add ability to set source directory for the Avro compiler's Velocity templates.
    * Matching of fieldVisibility settings is now case-insensitive.
    * Removed some excessive debug logging
    * Built against Gradle 2.7
    * Added Checkstyle and Codenarc to build

* 0.5.0
    * Add support for schemas/protocols/IDL in subdirectories of `src/main/avro`, etc. (#11)
    * Expose original error messages from `avro-compiler` when compilation fails

* 0.4.0
    * Add ability to specify fieldVisibility for generated Java source; contribution from [wooder79](https://github.com/wooder79)
    * Removed support for unqualified plugin ID (just "avro")
    * Published via new mechanism to [Gradle plugin portal](https://plugins.gradle.org)
    * Stopped publishing to previous location on Bintray
    * Built against Gradle 2.6; uses [test kit](https://docs.gradle.org/current/userguide/test_kit.html) for functional testing

* 0.3.4
    * Fix registration of generated sources for compilation (#8)
    * Change classloader handling to better support import of external dependencies (#9)

* 0.3.3
    * Fix generation of Java files from .avdl files; contribution from [viacoban](https://github.com/viacoban)

* 0.3.2
    * Improve handling when custom buildDir is used

* 0.3.1
    * Fix extension support for configuring encoding
    * Make default encoding UTF-8

* 0.3.0
    * IntelliJ: register generated source directories even if they don't already exist.
    * Add avro-base plugin, which exposes tasks and the extension without creating tasks, defaults, etc.

* 0.2.0
    * Build against Gradle 1.12
    * Compile using Avro 1.7.6
    * Support for qualified plugin ID
    * Deprecate unqualified plugin ID

* 0.1.3
    * Always regenerate all Java classes when any schema file changes to avoid some classes having outdated schema information.

* 0.1.2
    * Eliminate dependency on guava, make dependency on commons-io explicit

* 0.1.1
    * Fixed NullPointerException when performing clean builds

* 0.1.0
    * Add support for converting IDL files to JSON protocol declaration files
    * Add support for generating Java classes from JSON protocol declaration files
    * Add support for generating Java classes from JSON schema declaration files
    * Add support for inter-dependent JSON schema declaration files
    * Add support for tweaking source/exclude directories in IntelliJ
    * Add support for specifying the string type to use in generated classes
