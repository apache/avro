package com.commercehub.gradle.plugin.avro

import org.gradle.testkit.runner.GradleRunner
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path

import static org.gradle.testkit.runner.TaskOutcome.SUCCESS

class AvroPluginFunctionalSpec extends Specification {
    private static final String AVRO_VERSION = "1.7.7" // TODO: externalize

    @Rule
    TemporaryFolder testProjectDir

    File buildFile
    File avroDir

    def setup() {
        buildFile = testProjectDir.newFile('build.gradle')
        avroDir = testProjectDir.newFolder("src", "main", "avro")

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

    def "can generate and compile java files from json schema"() {
        given:
        copyResource("user.avsc", avroDir)

        when:
        def result = GradleRunner.create().withProjectDir(testProjectDir.root).withArguments("build").build()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        Files.exists(projectPath("build/classes/main/example/avro/User.class"))
    }

    def "can generate and compile java files from json protocol"() {
        given:
        buildFile << """
            dependencies { compile "org.apache.avro:avro-ipc:${AVRO_VERSION}" }
        """
        copyResource("mail.avpr", avroDir)

        when:
        def result = GradleRunner.create().withProjectDir(testProjectDir.root).withArguments("build").build()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        Files.exists(projectPath("build/classes/main/org/apache/avro/test/Mail.class"))
        Files.exists(projectPath("build/classes/main/org/apache/avro/test/Message.class"))
    }

    def "can generate and compile java files from IDL"() {
        given:
        copyResource("interop.avdl", avroDir)

        when:
        def result = GradleRunner.create().withProjectDir(testProjectDir.root).withArguments("build").build()

        then:
        result.task(":generateAvroProtocol").outcome == SUCCESS
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        Files.exists(projectPath("build/classes/main/org/apache/avro/Foo.class"))
        Files.exists(projectPath("build/classes/main/org/apache/avro/Interop.class"))
        Files.exists(projectPath("build/classes/main/org/apache/avro/Kind.class"))
        Files.exists(projectPath("build/classes/main/org/apache/avro/MD5.class"))
        Files.exists(projectPath("build/classes/main/org/apache/avro/Node.class"))
    }

    private void copyResource(String name, File targetFolder) {
        def file = new File(targetFolder, name)
        file << getClass().getResourceAsStream(name)
    }

    private Path projectPath(String path) {
        return testProjectDir.root.toPath().resolve(path)
    }
}
