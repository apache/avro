Originally reported as [#27](https://github.com/davidmc24/gradle-avro-plugin/issues/27).
Currently, Avro generation takes by running the avro-compiler library as part of the Gradle plugin process.
This is simple and works, but has a few drawbacks:

* Custom templates need to be available on the classpath for the plugin, which isn't compatible with the new style of Gradle plugin declarations.
* The Gradle plugin may use a different version of Avro for generation than you're using on the compile classpath for compilation.

Instead, here is an alternative view of how it could work.

* There is an enhanced-avro-compiler library that externalizes most of the logic currently present in GenerateAvroJavaTask/GenerateAvroProtocolTask, and makes those calls accessible as JVM entry points (via `main` methods).
    * This library would be published on Maven Central, and potentially have multiple versions as needed for compatibility with multiple versions of Avro
    * It's possible we might be able to get this logic pushed upstream into avro-compiler, in which case the need for this library would be eliminated.
* For a source-set, the plugin would take a single declaration of the desired Avro version, which is then used for both generation and compilation
* The plugin would use a configuration per source-set to resolve the appropriate version of enhanced-avro-compiler
    * The build script could add additional dependencies to this configuration in order to pull in custom templates
* When doing generation, the plugin would use [JavaExec](https://docs.gradle.org/current/javadoc/org/gradle/api/tasks/JavaExec.html) to spawn a child JVM and execute the appropriate logic in enhanced-avro-compiler.
