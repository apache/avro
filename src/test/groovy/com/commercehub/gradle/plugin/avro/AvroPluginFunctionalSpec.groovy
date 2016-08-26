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

class AvroPluginFunctionalSpec extends FunctionalSpec {
    def "can generate and compile java files from json schema"() {
        given:
        copyResource("user.avsc", avroDir)

        when:
        def result = run()

        then:
        taskInfoAbsent || result.task(":generateAvroJava").outcome == SUCCESS
        taskInfoAbsent || result.task(":compileJava").outcome == SUCCESS
        projectFile("build/classes/main/example/avro/User.class").file
    }

    def "can generate and compile java files from json protocol"() {
        given:
        buildFile << """
            dependencies { compile "org.apache.avro:avro-ipc:${avroVersion}" }
        """
        copyResource("mail.avpr", avroDir)

        when:
        def result = run()

        then:
        taskInfoAbsent || result.task(":generateAvroJava").outcome == SUCCESS
        taskInfoAbsent || result.task(":compileJava").outcome == SUCCESS
        projectFile("build/classes/main/org/apache/avro/test/Mail.class").file
        projectFile("build/classes/main/org/apache/avro/test/Message.class").file
    }

    def "can generate and compile java files from IDL"() {
        given:
        copyResource("interop.avdl", avroDir)

        when:
        def result = run()

        then:
        taskInfoAbsent || result.task(":generateAvroProtocol").outcome == SUCCESS
        taskInfoAbsent || result.task(":generateAvroJava").outcome == SUCCESS
        taskInfoAbsent || result.task(":compileJava").outcome == SUCCESS
        projectFile("build/classes/main/org/apache/avro/Foo.class").file
        projectFile("build/classes/main/org/apache/avro/Interop.class").file
        projectFile("build/classes/main/org/apache/avro/Kind.class").file
        projectFile("build/classes/main/org/apache/avro/MD5.class").file
        projectFile("build/classes/main/org/apache/avro/Node.class").file
    }

    def "supports json schema files in subdirectories"() {
        given:
        copyResource("user.avsc", avroSubDir)

        when:
        def result = run()

        then:
        taskInfoAbsent || result.task(":generateAvroJava").outcome == SUCCESS
        taskInfoAbsent || result.task(":compileJava").outcome == SUCCESS
        projectFile("build/classes/main/example/avro/User.class").file
    }

    def "supports json protocol files in subdirectories"() {
        given:
        buildFile << """
            dependencies { compile "org.apache.avro:avro-ipc:${avroVersion}" }
        """
        copyResource("mail.avpr", avroSubDir)

        when:
        def result = run()

        then:
        taskInfoAbsent || result.task(":generateAvroJava").outcome == SUCCESS
        taskInfoAbsent || result.task(":compileJava").outcome == SUCCESS
        projectFile("build/classes/main/org/apache/avro/test/Mail.class").file
        projectFile("build/classes/main/org/apache/avro/test/Message.class").file
    }

    def "supports IDL files in subdirectories"() {
        given:
        copyResource("interop.avdl", avroSubDir)

        when:
        def result = run()

        then:
        taskInfoAbsent || result.task(":generateAvroProtocol").outcome == SUCCESS
        taskInfoAbsent || result.task(":generateAvroJava").outcome == SUCCESS
        taskInfoAbsent || result.task(":compileJava").outcome == SUCCESS
        projectFile("build/classes/main/org/apache/avro/Foo.class").file
        projectFile("build/classes/main/org/apache/avro/Interop.class").file
        projectFile("build/classes/main/org/apache/avro/Kind.class").file
        projectFile("build/classes/main/org/apache/avro/MD5.class").file
        projectFile("build/classes/main/org/apache/avro/Node.class").file
    }

    def "gives a meaningful error message when presented a malformed schema file"() {
        given:
        copyResource("enumMalformed.avsc", avroDir)

        when:
        def result = runAndFail()

        then:
        taskInfoAbsent || result.task(":generateAvroJava").outcome == FAILED
        result.output.contains("> Could not compile schema definition files:")
        result.output.contains("* src/main/avro/enumMalformed.avsc: \"enum\" is not a defined name. The type of the \"gender\" " +
                "field must be a defined name or a {\"type\": ...} expression.")
    }
}
