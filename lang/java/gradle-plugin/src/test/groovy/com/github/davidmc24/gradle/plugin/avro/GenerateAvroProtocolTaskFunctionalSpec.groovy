/*
 * Copyright © 2019 David M. Carr
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
package com.github.davidmc24.gradle.plugin.avro

import spock.lang.Subject

import static org.gradle.testkit.runner.TaskOutcome.SUCCESS

@Subject(GenerateAvroProtocolTask)
class GenerateAvroProtocolTaskFunctionalSpec extends FunctionalSpec {
    def "With base plugin, declares input on classpath"() {
        given: "a build that declares another task's output in the classpath"
        applyAvroBasePlugin()
        applyPlugin("java") // Jar task appears to only work with the java plugin applied
        buildFile << """
        |configurations.create("shared")
        |tasks.register("sharedIdlJar", Jar) {
        |    from "src/shared"
        |}
        |dependencies {
        |    shared sharedIdlJar.outputs.files  
        |}
        |tasks.register("generateProtocol", com.github.davidmc24.gradle.plugin.avro.GenerateAvroProtocolTask) {
        |    classpath = configurations.shared
        |    source file("src/dependent")
        |    outputDir = file("build/protocol")
        |}
        |""".stripMargin()

        copyResource("shared.avdl", projectFolder("src/shared"))
        copyResource("dependent.avdl", projectFolder("src/dependent"))

        when: "running the task"
        def result = run("generateProtocol")

        then: "running the generate protocol task occurs after running the producing task"
        result.tasks*.path == [":sharedIdlJar", ":generateProtocol"]
        result.task(":generateProtocol").outcome == SUCCESS
        projectFile("build/protocol/com/example/dependent/DependentProtocol.avpr").file
    }

    def "With avro plugin, declares input on classpath (runtime configuration by default)"() {
        given: "a build that declares another task's output in the classpath"
        applyAvroPlugin()
        buildFile << """
        |tasks.register("sharedIdlJar", Jar) {
        |    from "src/shared"
        |}
        |dependencies {
        |    runtimeOnly sharedIdlJar.outputs.files  
        |}
        |""".stripMargin()

        copyResource("shared.avdl", projectFolder("src/shared"))
        copyResource("dependent.avdl", avroDir)

        when: "running the task"
        def result = run("generateAvroProtocol")

        then: "running the generate protocol task occurs after running the producing task"
        result.tasks*.path == [":sharedIdlJar", ":generateAvroProtocol"]
        result.task(":generateAvroProtocol").outcome == SUCCESS
        projectFile("build/generated-main-avro-avpr/com/example/dependent/DependentProtocol.avpr").file
    }

    def "supports files with the same name in different directories"() {
        given: "a project with two IDL files with the same name, but in different directories"
        applyAvroPlugin()

        copyResource("namespaced-idl/v1/test.avdl", projectFolder("src/main/avro/v1"))
        copyResource("namespaced-idl/v2/test.avdl", projectFolder("src/main/avro/v2"))

        when: "running the task"
        def result = run("generateAvroProtocol")

        then: "avpr files are generated for each IDL file"
        result.task(":generateAvroProtocol").outcome == SUCCESS
        projectFile("build/generated-main-avro-avpr/org/example/v1/TestProtocol.avpr").file
        projectFile("build/generated-main-avro-avpr/org/example/v2/TestProtocol.avpr").file
    }

    def "fails if avpr will be overwritten"() {
        given: "a project with two IDL files with the same protocol name and namespace"
        applyAvroPlugin()

        copyResource("namespaced-idl/v1/test.avdl", projectFolder("src/main/avro/v1"))
        copyResource("namespaced-idl/v1/test_same_protocol.avdl", projectFolder("src/main/avro/v1"))

        when: "running the task"
        run("generateAvroProtocol")

        then:
        def ex = thrown(Exception)
        ex.message.contains("Failed to compile IDL file")
        ex.message.contains("File already processed with same namespace and protocol name.")
    }
}
