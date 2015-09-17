package com.commercehub.gradle.plugin.avro

import java.nio.file.Files

import static org.gradle.testkit.runner.TaskOutcome.SUCCESS

class EnumHandlingFunctionalSpec extends FunctionalSpec {
    def "supports simple enums"() {
        given:
        copyResource("enumSimple.avsc", avroDir)

        when:
        def result = run()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        Files.exists(projectPath("build/classes/main/example/avro/MyEnum.class"))
    }

    def "supports enums defined within a record field"() {
        given:
        copyResource("enumField.avsc", avroDir)

        when:
        def result = run()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        Files.exists(projectPath("build/classes/main/example/avro/Test.class"))
        Files.exists(projectPath("build/classes/main/example/avro/Gender.class"))
    }

    def "supports enums defined within a union"() {
        given:
        copyResource("enumUnion.avsc", avroDir)

        when:
        def result = run()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        Files.exists(projectPath("build/classes/main/example/avro/Test.class"))
        Files.exists(projectPath("build/classes/main/example/avro/Kind.class"))
    }

    def "supports using enums defined in a separate schema file"() {
        given:
        copyResource("enumSimple.avsc", avroDir)
        copyResource("enumUseSimple.avsc", avroDir)

        when:
        def result = run()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        Files.exists(projectPath("build/classes/main/example/avro/User.class"))
        Files.exists(projectPath("build/classes/main/example/avro/MyEnum.class"))
    }
}
