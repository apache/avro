# Purpose

An example project for having dependencies on .avsc schema files loaded from an JAR file
produced by a subproject of the current multi-project Gradle build.

# Variants
            
## schema project JAR doesn't contain classes

If you'd rather have the `schema` project **not** generate Java classes, you can rename `src/main/avro` to `src/main/resources`.
In that case, you can also replace the Avro plugin with the `java` plugin.
