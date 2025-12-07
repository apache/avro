# Avro Gradle plugin (in development)

This plugin generated Java code from Avro schemas

## Installation
### Checkout

`git clone https://github.com/frevib/avro/tree/feat/gradle-plugin`

### Install to local Maven repo

* inside `avro/lang/java/gradle-plugin`

* run `./gradlew publishToMavenLocal` 

Gradle plugin is now available under:

`org.apache.avro.avro-gradle-plugin:1.13.0-SNAPSHOT`

## Usage

### Add avro extension
In `build.gradle.kts`:

```kotlin
plugins {
    id("org.apache.avro.avro-gradle-plugin") version "1.13.0-SNAPSHOT"
}
```

and

```kotlin
avro {
    sourceDirectory = "src/main/avro"
} 
```

All properties are available in `GradlePluginExtension.kt`

### Add a task hook
For Intellij to recognize the newly generated Java files add this to `build.gradle.kts`:

```kotlin
tasks.named("compileKotlin") { dependsOn(tasks.named("avroGenerateJavaClasses")) }
```

### Generate Java classes

`./gradlew avroGenerateJavaClasses`


## Example project that uses avro-gradle-plugin
https://codeberg.org/frevib/use-gradle-plugin-test


