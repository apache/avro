package com.commercehub.gradle.plugin.avro

import java.nio.file.Files

import static org.gradle.testkit.runner.TaskOutcome.FAILED
import static org.gradle.testkit.runner.TaskOutcome.SUCCESS

class AvroPluginFunctionalSpec extends FunctionalSpec {
    def "can generate and compile java files from json schema"() {
        given:
        copyResource("user.avsc", avroDir)

        when:
        def result = run()

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
        def result = run()

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
        def result = run()

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

    def "supports json schema files in subdirectories"() {
        given:
        copyResource("user.avsc", avroSubDir)

        when:
        def result = run()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        Files.exists(projectPath("build/classes/main/example/avro/User.class"))
    }

    def "supports json protocol files in subdirectories"() {
        given:
        buildFile << """
            dependencies { compile "org.apache.avro:avro-ipc:${AVRO_VERSION}" }
        """
        copyResource("mail.avpr", avroSubDir)

        when:
        def result = run()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        Files.exists(projectPath("build/classes/main/org/apache/avro/test/Mail.class"))
        Files.exists(projectPath("build/classes/main/org/apache/avro/test/Message.class"))
    }

    def "supports IDL files in subdirectories"() {
        given:
        copyResource("interop.avdl", avroSubDir)

        when:
        def result = run()

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

    def "gives a meaningful error message when presented a malformed schema file"() {
        given:
        copyResource("enumMalformed.avsc", avroDir)

        when:
        def result = runAndFail()

        then:
        result.task(":generateAvroJava").outcome == FAILED
        result.standardError.contains("> Could not compile schema definition files:")
        result.standardError.contains("* src/main/avro/enumMalformed.avsc: \"enum\" is not a defined name. The type of the \"gender\" " +
                "field must be a defined name or a {\"type\": ...} expression.")
    }
}
