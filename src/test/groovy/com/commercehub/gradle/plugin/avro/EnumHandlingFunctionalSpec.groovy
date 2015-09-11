package com.commercehub.gradle.plugin.avro

import org.gradle.testkit.runner.GradleRunner
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path

import static org.gradle.testkit.runner.TaskOutcome.SUCCESS

class EnumHandlingFunctionalSpec extends Specification {
    private static final String AVRO_VERSION = "1.7.7" // TODO: externalize

    @Rule
    TemporaryFolder testProjectDir

    File buildFile
    File avroDir
    File avroSubDir

    def setup() {
        buildFile = testProjectDir.newFile('build.gradle')
        avroDir = testProjectDir.newFolder("src", "main", "avro")
        avroSubDir = testProjectDir.newFolder("src", "main", "avro", "foo")

        def pluginClasspathResource = getClass().classLoader.findResource("plugin-classpath.txt")
        if (pluginClasspathResource == null) {
            throw new IllegalStateException("Did not find plugin classpath resource, run `testClasses` build task.")
        }

        def pluginClasspath = pluginClasspathResource.readLines()
            .collect { it.replace('\\', '\\\\') } // escape backslashes in Windows paths
            .collect { "'$it'" }
            .join(", ")

        // Add the logic under test to the test build
        buildFile << """
            buildscript {
                dependencies {
                    classpath files($pluginClasspath)
                }
            }
            apply plugin: "com.commercehub.gradle.plugin.avro"
            repositories { jcenter() }
            dependencies { compile "org.apache.avro:avro:${AVRO_VERSION}" }
        """
    }

    def "supports simple enums"() {
        given:
        copyResource("simpleEnum.avsc", avroDir)

        when:
        def result = GradleRunner.create().withProjectDir(testProjectDir.root).withArguments("build").build()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        Files.exists(projectPath("build/classes/main/example/avro/MyEnum.class"))
    }

    def "supports enums nested within a schema"() {
        given:
        copyResource("innerEnum.avsc", avroDir)

        when:
        def result = GradleRunner.create().withProjectDir(testProjectDir.root).withArguments("build").build()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        Files.exists(projectPath("build/classes/main/example/avro/Test.class"))
        Files.exists(projectPath("build/classes/main/example/avro/Kind.class"))
    }

    def "supports using enums defined in a separate schema file"() {
        given:
        copyResource("useEnum.avsc", avroDir)
        copyResource("simpleEnum.avsc", avroDir)

        when:
        def result = GradleRunner.create().withProjectDir(testProjectDir.root).withArguments("build").build()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        Files.exists(projectPath("build/classes/main/example/avro/User.class"))
        Files.exists(projectPath("build/classes/main/example/avro/MyEnum.class"))
    }

    private void copyResource(String name, File targetFolder) {
        def file = new File(targetFolder, name)
        file << getClass().getResourceAsStream(name)
    }

    private Path projectPath(String path) {
        return testProjectDir.root.toPath().resolve(path)
    }
}
