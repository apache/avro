This example demonstrates a custom plugin that registers a logical type factory and custom conversion.

To simplify the example, the custom conversion/logicalTypeFactory classes are duplicated in both buildSrc and the project.
In the real world, you would likely put them in a separate project, publish them as a JAR, and depend on them in both places.
