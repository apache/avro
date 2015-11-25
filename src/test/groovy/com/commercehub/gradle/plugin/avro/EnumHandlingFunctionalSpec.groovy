package com.commercehub.gradle.plugin.avro

import static org.gradle.testkit.runner.TaskOutcome.SUCCESS

/**
 * Functional tests relating to handling of enums.
 */
class EnumHandlingFunctionalSpec extends FunctionalSpec {
    def "supports simple enums"() {
        given:
        copyResource("enumSimple.avsc", avroDir)

        when:
        def result = run()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        projectFile("build/classes/main/example/avro/MyEnum.class").file
    }

    def "supports enums defined within a record field"() {
        given:
        copyResource("enumField.avsc", avroDir)

        when:
        def result = run()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        projectFile("build/classes/main/example/avro/Test.class").file
        projectFile("build/classes/main/example/avro/Gender.class").file
    }

    def "supports enums defined within a union"() {
        given:
        copyResource("enumUnion.avsc", avroDir)

        when:
        def result = run()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        projectFile("build/classes/main/example/avro/Test.class").file
        projectFile("build/classes/main/example/avro/Kind.class").file
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
        projectFile("build/classes/main/example/avro/User.class").file
        projectFile("build/classes/main/example/avro/MyEnum.class").file
    }
}
