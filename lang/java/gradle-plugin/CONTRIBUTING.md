# Contributing

> Before contributing, please read our [code of conduct](https://github.com/davidmc24/gradle-avro-plugin/blob/master/CODE_OF_CONDUCT.md).

Before starting work on an enhancement, it's highly recommended to open an [issue](https://github.com/davidmc24/gradle-avro-plugin/issues) to describe the intended change.
This allows for the project maintainers to provide feedback before you've done work that may not fit the project's vision.

Note that this plugin is primarily focussed on exposing functionality from the [Apache Avro Java API](https://avro.apache.org/docs/current/api/java/index.html) in the ways most commonly used in Gradle builds.
If the capability that you are looking for doesn't currently exist in said upstream API, you're likely better off requesting the feature from the [Apache Avro project](https://avro.apache.org/) than requesting it here.

Some possible enhancements may have already been considered and documented.  Check the design-docs folder for the design specification for such features.

To run the project's build, run:

* (Mac/Linux): `./gradlew build`
* (Windows): `gradlew.bat build`

This will run static analysis against the project, run the project's tests, and build the project.
If any failures are detected, please correct them prior to submitting your pull request.

All enhancements should be accompanied by test coverage.
Our tests are based on [Spock](https://github.com/spockframework/spock).
Generally, it's best to extend our `FunctionalSpec` class, which provides useful functions for running the plugin within Gradle.

Note that the "build" task only tests the plugin against a single version of Gradle/Avro.
If you want to test compatibility with a larger range, consider using the `testRecentVersionCompatibility` task or `testVersionCompatibility` task.

For information on how to use GitHub to submit a pull request, see [Collaborating on projects using issues and pull requests](https://help.github.com/categories/collaborating-on-projects-using-issues-and-pull-requests/).
