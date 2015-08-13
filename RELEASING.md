# Release Process

1. Check that you've followed the setup steps listed [here](https://plugins.gradle.org/docs/submit)
1. Update `CHANGES.md`
1. Update the plugin version in `build.gradle` under "pluginBundle/version"
1. Commit and tag with the version number
1. Run `./gradlew clean build publishPlugins`
1. Push
