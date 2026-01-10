# Avro Gradle plugin (in development)

Gradle plugin that generates Java code from Avro schemas

## Version
`0.0.2`

first beta

`0.0.5`

Possible breaking change: rename `CompileSchemaTask` to `CompileAvroSchemaTask`

Add logical type factories

Now released on Gradle plugin portal: https://plugins.gradle.org/plugin/eu.eventloopsoftware.avro-gradle-plugin

`0.0.7`

It is not needed to add `tasks.named("compileKotlin") { dependsOn(tasks.named("avroGenerateJavaClasses")) }` any more

`0.0.8`

Add `sourceZipFiles` property to add zip files with schemas in them
pu

## Usage

### Add avro extension
In `build.gradle.kts`:

### Add plugin

```kotlin
plugins {
    id("eu.eventloopsoftware.avro-gradle-plugin") version "0.0.2"
}
```
### Add Avro dependency

```kotlin
implementation("org.apache.avro:avro:1.12.1")
```

### Configure Avro Gradle plugin

```kotlin
avro {
    sourceDirectory = "src/main/avro"
    // All properties are available in `GradlePluginExtension.kt`
} 
```

### Generate Java classes

`./gradlew avroGenerateJavaClasses`


## Example project that uses the Apache Avro gradle-plugin
https://codeberg.org/frevib/use-gradle-plugin-test

## FAQ

#### How can I benefit from Kotlin's null safety?
Use `createNullSafeAnnotations = true` and Java getters will be annotated with 
`@org.jetbrains.annotations.NotNull`/ `@org.jetbrains.annotations.Nullable`. This way
Kotlin will recognize which value is nullable.

#### I get my Avro schemas from a Maven dependency, how can I add JAR files that contain schemas?
Use `sourceZipFiles = listOf("file_path")`, e.g.

```kotlin
avro {
    sourceZipFiles = listOf("/home/user/.gradle/caches/modules-2/files-2.1/eu.eventloopsoftware.group-id/artifact-id/1.0.0/92ac3d0533de9dd79ac35373c892ebaa01763d4d/jar_with_schemas-1.0.0.jar")
} 
```

