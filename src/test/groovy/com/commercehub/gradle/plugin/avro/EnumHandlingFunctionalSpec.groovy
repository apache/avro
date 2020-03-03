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

import static org.gradle.testkit.runner.TaskOutcome.SUCCESS

/**
 * Functional tests relating to handling of enums.
 */
class EnumHandlingFunctionalSpec extends FunctionalSpec {
    def "setup"() {
        applyAvroPlugin()
        addDefaultRepository()
        addAvroDependency()
    }

    def "supports simple enums"() {
        given:
        copyResource("enumSimple.avsc", avroDir)

        when:
        def result = run()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        projectFile(buildOutputClassPath("example/avro/MyEnum.class")).file
    }

    def "supports enums defined within a record field"() {
        given:
        copyResource("enumField.avsc", avroDir)

        when:
        def result = run()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        projectFile(buildOutputClassPath("example/avro/Test.class")).file
        projectFile(buildOutputClassPath("example/avro/Gender.class")).file
    }

    def "supports enums defined within a union"() {
        given:
        copyResource("enumUnion.avsc", avroDir)

        when:
        def result = run()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        projectFile(buildOutputClassPath("example/avro/Test.class")).file
        projectFile(buildOutputClassPath("example/avro/Kind.class")).file
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
        projectFile(buildOutputClassPath("example/avro/User.class")).file
        projectFile(buildOutputClassPath("example/avro/MyEnum.class")).file
    }
}
