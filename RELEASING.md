# Release Process

1. Check that you've followed the setup steps listed [here](https://plugins.gradle.org/docs/submit)
1. Update `CHANGES.md`
1. Ensure that there is a milestone for the version, and that appropriate issues are associated with the milestone.
1. Update the plugin version in `build.gradle` under "version"
1. Commit and tag with the version number (don't push yet)
1. Run `./gradlew clean bintrayUpload`
1. Go to the [Bintray page](https://bintray.com/commercehub-oss/main/gradle-avro-plugin), verify the files, and click "Publish".
1. Update the version the next SNAPSHOT and commit.
1. Push
1. If there was a issue requesting the release, close it.
1. Close the milestone.
1. Go to the [GitHub Releases page](https://github.com/commercehub-oss/gradle-avro-plugin/releases), click "Draft a new release", select the tag version, use the version number as the title, copy the relevant segment from `CHANGES.md` into the description, and click "Publish release".
