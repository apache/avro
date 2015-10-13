package com.commercehub.gradle.plugin.avro

import java.nio.file.Files

import static org.gradle.testkit.runner.TaskOutcome.FAILED
import static org.gradle.testkit.runner.TaskOutcome.SUCCESS

/**
 * Functional tests related to handling of duplicate type definitions.
 *
 * <p>This situation is generally encountered when schema files define records with inline record/enum definitions, and those inline types
 * are used in more than one file.</p>
 */
class DuplicateHandlingFunctionalSpec extends FunctionalSpec {
    def "Duplicate enum definition succeeds if definition identical"() {
        given:
        copyIdenticalEnum()

        when:
        def result = run()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        Files.exists(projectPath("build/classes/main/example/Person.class"))
        Files.exists(projectPath("build/classes/main/example/Cat.class"))
        Files.exists(projectPath("build/classes/main/example/Gender.class"))
    }

    def "Duplicate record definition succeeds if definition identical"() {
        given:
        copyIdenticalRecord()

        when:
        def result = run()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        Files.exists(projectPath("build/classes/main/example/Person.class"))
        Files.exists(projectPath("build/classes/main/example/Fish.class"))
        Files.exists(projectPath("build/classes/main/example/Gender.class"))
    }

    def "Duplicate enum definition fails if definition differs"() {
        given:
        copyDifferentEnum()

        when:
        def result = runAndFail()

        then:
        result.task(":generateAvroJava").outcome == FAILED
        result.standardError.contains("Found conflicting definition of type example.Gender in "
            + "[src/main/avro/duplicate/Dog.avsc, src/main/avro/duplicate/Person.avsc]")
    }

    def "Duplicate record definition fails if definition differs"() {
        given:
        copyDifferentRecord()

        when:
        def result = runAndFail()

        then:
        result.task(":generateAvroJava").outcome == FAILED
        result.standardError.contains("Found conflicting definition of type example.Person in "
            + "[src/main/avro/duplicate/Person.avsc, src/main/avro/duplicate/Spider.avsc]")
    }

    private void copyIdenticalEnum() {
        copyResource("duplicate/Person.avsc", avroDir)
        copyResource("duplicate/Cat.avsc", avroDir)
    }

    private void copyDifferentEnum() {
        copyResource("duplicate/Person.avsc", avroDir)
        copyResource("duplicate/Dog.avsc", avroDir)
    }

    private void copyIdenticalRecord() {
        copyResource("duplicate/Person.avsc", avroDir)
        copyResource("duplicate/Fish.avsc", avroDir)
    }

    private void copyDifferentRecord() {
        copyResource("duplicate/Person.avsc", avroDir)
        copyResource("duplicate/Spider.avsc", avroDir)
    }
}
