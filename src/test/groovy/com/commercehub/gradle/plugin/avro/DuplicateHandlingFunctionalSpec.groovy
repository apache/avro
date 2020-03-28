/*
 * Copyright © 2015-2016 Commerce Technologies, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.commercehub.gradle.plugin.avro

import static org.gradle.testkit.runner.TaskOutcome.FAILED
import static org.gradle.testkit.runner.TaskOutcome.SUCCESS

/**
 * Functional tests related to handling of duplicate type definitions.
 *
 * <p>This situation is generally encountered when schema files define records with inline record/enum definitions, and those inline types
 * are used in more than one file.</p>
 */
class DuplicateHandlingFunctionalSpec extends FunctionalSpec {
    def "setup"() {
        applyAvroPlugin()
        addDefaultRepository()
        addAvroDependency()
    }

    def "Duplicate record definition succeeds if definition identical"() {
        given:
        copyIdenticalRecord()

        when:
        def result = run()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        projectFile(buildOutputClassPath("example/Person.class")).file
        projectFile(buildOutputClassPath("example/Fish.class")).file
        projectFile(buildOutputClassPath("example/Gender.class")).file
    }

    def "Duplicate enum definition succeeds if definition identical"() {
        given:
        copyIdenticalEnum()

        when:
        def result = run()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        projectFile(buildOutputClassPath("example/Person.class")).file
        projectFile(buildOutputClassPath("example/Cat.class")).file
        projectFile(buildOutputClassPath("example/Gender.class")).file
    }

    def "Duplicate fixed definition succeeds if definition identical"() {
        given:
        copyIdenticalFixed()

        when:
        def result = run()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        projectFile(buildOutputClassPath("example/ContainsFixed1.class")).file
        projectFile(buildOutputClassPath("example/ContainsFixed2.class")).file
        projectFile(buildOutputClassPath("example/Picture.class")).file
    }

    def "Duplicate record definition fails if definition differs"() {
        given:
        copyDifferentRecord()
        def errorFilePath1 = new File("src/main/avro/duplicate/Person.avsc").path
        def errorFilePath2 = new File("src/main/avro/duplicate/Spider.avsc").path
        when:
        def result = runAndFail()

        then:
        result.task(":generateAvroJava").outcome == FAILED
        result.output.contains("Found conflicting definition of type example.Person in "
            + "[$errorFilePath1, $errorFilePath2]")
    }

    def "Duplicate enum definition fails if definition differs"() {
        given:
        copyDifferentEnum()
        def errorFilePath1 = new File("src/main/avro/duplicate/Dog.avsc").path
        def errorFilePath2 = new File("src/main/avro/duplicate/Person.avsc").path

        when:
        def result = runAndFail()

        then:
        result.task(":generateAvroJava").outcome == FAILED
        result.output.contains("Found conflicting definition of type example.Gender in "
            + "[$errorFilePath1, $errorFilePath2]")
    }

    def "Duplicate fixed definition fails if definition differs"() {
        given:
        copyDifferentFixed()
        def errorFilePath1 = new File("src/main/avro/duplicate/ContainsFixed1.avsc").path
        def errorFilePath2 = new File("src/main/avro/duplicate/ContainsFixed3.avsc").path

        when:
        def result = runAndFail()

        then:
        result.task(":generateAvroJava").outcome == FAILED
        result.output.contains("Found conflicting definition of type example.Picture in "
            + "[$errorFilePath1, $errorFilePath2]")
    }

    def "Duplicate record definition in single file fails with clear error"() {
        given:
        copyResource("duplicate/duplicateInSingleFile.avsc", avroDir)
        def errorFilePath = new File("src/main/avro/duplicate/duplicateInSingleFile.avsc").path

        when:
        def result = runAndFail()

        then:
        result.task(":generateAvroJava").outcome == FAILED
        result.output.contains("Failed to compile schema definition file $errorFilePath; " +
            "contains duplicate type definition example.avro.date")
    }

    private void copyIdenticalRecord() {
        copyResource("duplicate/Person.avsc", avroDir)
        copyResource("duplicate/Fish.avsc", avroDir)
    }

    private void copyIdenticalEnum() {
        copyResource("duplicate/Person.avsc", avroDir)
        copyResource("duplicate/Cat.avsc", avroDir)
    }

    private void copyIdenticalFixed() {
        copyResource("duplicate/ContainsFixed1.avsc", avroDir)
        copyResource("duplicate/ContainsFixed2.avsc", avroDir)
    }

    private void copyDifferentRecord() {
        copyResource("duplicate/Person.avsc", avroDir)
        copyResource("duplicate/Spider.avsc", avroDir)
    }

    private void copyDifferentEnum() {
        copyResource("duplicate/Person.avsc", avroDir)
        copyResource("duplicate/Dog.avsc", avroDir)
    }

    private void copyDifferentFixed() {
        copyResource("duplicate/ContainsFixed1.avsc", avroDir)
        copyResource("duplicate/ContainsFixed3.avsc", avroDir)
    }
}
