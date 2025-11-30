# Avro Gradle plugin

This plugin generated Java code from Avro schemas

### Installation instructions

* Checkout https://github.com/frevib/avro/tree/feat/gradle-plugin
* install to local Maven repo with `./gradlew publishToMavenLocal` from inside `avro/lang/java/gradle-plugin`

Gradle plugin is now available under:

```kotlin
plugins {
    id("org.apache.avro.avro-gradle-plugin") version "1.13.0-SNAPSHOT"
}
```

## Usage:

### Add avro extension in `build.gradle.kts`
```kotlin
avro {
    sourceDirectory = "src/main/avro"
    outputDirectory = "generated-sources/avro"
} 
```

All properties are available in `GradlePluginExtension.kt`


### Generate Java classes

`./gradlew avroGenerateJavaClasses`


### Example project that uses gradle plugin
https://codeberg.org/frevib/use-gradle-plugin-test


