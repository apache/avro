Originally requested as [#4](https://github.com/davidmc24/gradle-avro-plugin/issues/4).
Some users would like the ability to have JAR files that contain Avro schema/protocol files, and have a way to declare a dependency on these, such that the plugin's generation capability can use them without needing to manual extract the archives.

Intended approach:

* The plugin defines a new `<sourceSetName>Avro` configuration for every source-set that it is working with.
* As with any configuration, the build script can define dependencies in the configuration, and the resolution mechanism will resolve the configuration against the configured repositories when it is resolved.
* A task would be added to extract all such archives to a `<buildDir>/unpacked-<sourceSetName>-avro` directory, per source-set
* The plugin's generation tasks would be configured to use the relevant unpacked directories as additional source directories.
