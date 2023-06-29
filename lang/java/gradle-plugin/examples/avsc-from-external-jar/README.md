# Purpose

An example project for having dependencies on .avsc schema files loaded from an external JAR file
(not produced by the current Gradle project).

# Maintainer Notes

* Command to create JAR: `(cd external-files && jar --create --no-manifest --file ../external-libs/schema.jar Breed.avsc)`
