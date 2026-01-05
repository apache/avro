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

## FAQ

#### How can I benefit from Kotlin's null safety?
Use `createNullSafeAnnotations = true` and Java getters will be annotated with 
`@org.jetbrains.annotations.NotNull`/ `@org.jetbrains.annotations.Nullable`. This way
Kotlin will recognize which value is nullable.

#### When I update my schemas and generate code, the Java classes stay the same
Yes, for now you have to `./gradlew clean` every time you update your Avro schemas.




