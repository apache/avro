# Contributing

> Before contributing, please read our [code of conduct](https://github.com/commercehub-oss/gradle-avro-plugin/blob/master/CODE_OF_CONDUCT.md).

> See [our security policy](SECURITY.md) for handling of security-related matters.

Before starting work on an enhancement, it's highly recommended to open an [issue](https://github.com/commercehub-oss/gradle-avro-plugin/issues) to describe the intended change.
This allows for the project maintainers to provide feedback before you've done work that may not fit the project's vision.

Some possible enhancements may have already been considered and documented.  Check the design-docs folder for the design specification for such features.

To run the project's build, run:

* (Mac/Linux): `./gradlew build`
* (Windows): `gradlew.bat build`

This will run static analysis against the project, run the project's tests, and build the project.
If any failures are detected, please correct them prior to submitting your pull request.

All enhancements should be accompanied by test coverage.
Our tests are based on [Spock](https://github.com/spockframework/spock).
Generally, it's best to extend our `FunctionalSpec` class, which provides useful functions for running the plugin within Gradle.

Note that the "build" task only tests the plugin against the a single version of Gradle/Avro.
If you want to test compatibility with a larger range, consider using the `testRecentVersionCompatibility` task or `testVersionCompatibility` task.

For information on how to use GitHub to submit a pull request, see [Collaborating on projects using issues and pull requests](https://help.github.com/categories/collaborating-on-projects-using-issues-and-pull-requests/).
