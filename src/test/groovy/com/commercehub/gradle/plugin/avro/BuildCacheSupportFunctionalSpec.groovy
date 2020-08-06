/*
 * Copyright © 2018 Commerce Technologies, LLC.
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

import spock.lang.IgnoreIf
import spock.util.environment.OperatingSystem

import static org.gradle.testkit.runner.TaskOutcome.FROM_CACHE

/**
 * Testing for <a href="https://docs.gradle.org/current/userguide/build_cache.html">Build Cache</a> feature support.
 */
class BuildCacheSupportFunctionalSpec extends FunctionalSpec {
    def "setup"() {
        applyAvroPlugin()
        addDefaultRepository()
        addAvroDependency()
    }

    def "supports build cache for schema/protocol java source generation"() {
        given: "a project is built once with build cache enabled"
        copyResource("user.avsc", avroDir)
        copyResource("mail.avpr", avroDir)
        addAvroIpcDependency()
        run("build", "--build-cache")

        and: "the project is cleaned"
        run("clean")

        when: "the project is built again with build cache enabled"
        def result = run("build", "--build-cache")

        then: "the expected outputs were produced from the build cache"
        result.task(":generateAvroJava").outcome == FROM_CACHE
        result.task(":compileJava").outcome == FROM_CACHE
        projectFile("build/generated-main-avro-java/example/avro/User.java").file
        projectFile("build/generated-main-avro-java/org/apache/avro/test/Mail.java").file
        projectFile(buildOutputClassPath("example/avro/User.class")).file
        projectFile(buildOutputClassPath("org/apache/avro/test/Mail.class")).file
    }

    /**
     * This test appears to fail on Windows due to clean being unable to delete interop.avpr.
     */
    @IgnoreIf({ OperatingSystem.current.windows })
    def "supports build cache for IDL to protocol conversion"() {
        given: "a project is built once with build cache enabled"
        copyResource("interop.avdl", avroDir)
        run("build", "--build-cache")

        and: "the project is cleaned"
        run("clean")

        when: "the project is built again with build cache enabled"
        def result = run("build", "--build-cache")

        then: "the expected outputs were produced from the build cache"
        result.task(":generateAvroProtocol").outcome == FROM_CACHE
        result.task(":generateAvroJava").outcome == FROM_CACHE
        result.task(":compileJava").outcome == FROM_CACHE
        projectFile("build/generated-main-avro-avpr/org/apache/avro/InteropProtocol.avpr").file
        projectFile("build/generated-main-avro-java/org/apache/avro/Interop.java").file
        projectFile(buildOutputClassPath("org/apache/avro/Interop.class")).file
    }

    def "supports build cache for protocol to schema conversion"() {
        given: "a project is built once with build cache enabled"
        copyResource("mail.avpr", avroDir)
        buildFile << """
        |tasks.register("generateSchema", com.commercehub.gradle.plugin.avro.GenerateAvroSchemaTask) {
        |    source file("src/main/avro")
        |    include("**/*.avpr")
        |    outputDir = file("build/generated-main-avro-avsc")
        |}
        |""".stripMargin()
        run("generateSchema", "--build-cache")

        and: "the project is cleaned"
        run("clean")

        when: "the project is built again with build cache enabled"
        def result = run("generateSchema", "--build-cache")

        then: "the expected outputs were produced from the build cache"
        result.task(":generateSchema").outcome == FROM_CACHE
        projectFile("build/generated-main-avro-avsc/org/apache/avro/test/Message.avsc").file
    }
}
