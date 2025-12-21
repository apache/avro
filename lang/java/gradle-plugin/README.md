# Avro Gradle plugin (in development)

Gradle plugin that generates Java code from Avro schemas

## Usage

### Add avro extension
In `build.gradle.kts`:

Add plugin

```kotlin
plugins {
    id("eu.eventloopsoftware.avro-gradle-plugin") version "0.0.2"
}
```
Add Avro dependency

```kotlin
implementation("org.apache.avro:avro:1.12.1")
```
Configure Avro Gradle plugin
```kotlin
avro {
    sourceDirectory = "src/main/avro"
    // All properties are available in `GradlePluginExtension.kt`
} 
```

In `settings.gradle.kts`:

Plugin is published on Maven Central:
```kotlin
pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}
```



### Add a task hook
For Intellij to recognize the newly generated Java files add this to `build.gradle.kts`:

```kotlin
tasks.named("compileKotlin") { dependsOn(tasks.named("avroGenerateJavaClasses")) }
```

### Generate Java classes

`./gradlew avroGenerateJavaClasses`


## Example project that uses avro-gradle-plugin
https://codeberg.org/frevib/use-gradle-plugin-test


