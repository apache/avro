# Overview

This is a [Gradle](http://www.gradle.org/) plugin to allow easily performing Java code generation for [Apache Avro](http://avro.apache.org/).  It supports JSON schema declaration files, JSON protocol declaration files, and Avro IDL files.

# Usage

Add the following to your `build.gradle` file.  Substitute the desired version based on [CHANGES.md](https://github.com/commercehub-oss/gradle-avro-plugin/blob/master/CHANGES.md).

```groovy
buildscript {
    repositories {
        jcenter()
    }
    dependencies {
        classpath "com.commercehub.gradle.plugin:gradle-avro-plugin:VERSION"
    }
}

apply plugin: "com.commercehub.gradle.plugin.avro"
```

Optionally, configure the string type to `charSequence`, `string` (the default), or `utf8`.

```groovy
avro {
    stringType = "string"
}
```

Additionally, ensure that you have a compile dependency on avro, such as:

```groovy
repositories {
    jcenter()
}
dependencies {
    compile "org.apache.avro:avro:1.7.6"
}
```

If you now run `gradle build`, Java classes will be compiled from Avro files in `src/main/avro`.  Actually, it will attempt to process an "avro" directory in every `SourceSet` (main, test, etc.)
