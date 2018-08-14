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

import static org.gradle.testkit.runner.TaskOutcome.SUCCESS

class AvroBasePluginFunctionalSpec extends FunctionalSpec {
    def "setup"() {
        applyAvroBasePlugin()
    }

    def "can generate json schema files from json protocol"() {
        given:
        buildFile << """
            task("generateSchema", type: com.commercehub.gradle.plugin.avro.GenerateAvroSchemaTask) {
                source file("src/main/avro")
                include("**/*.avpr")
                outputDir = file("build/generated-main-avro-avsc")
            }
        """

        copyResource("mail.avpr", avroDir)

        when:
        def result = run("generateSchema")

        then:
        taskInfoAbsent || result.task(":generateSchema").outcome == SUCCESS
        def expectedFileContents = getClass().getResource("Message.avsc").text.trim()
        def generateFileContents = new File(testProjectDir.root,
            "build/generated-main-avro-avsc/org/apache/avro/test/Message.avsc").text.trim()
        expectedFileContents == generateFileContents
    }

    def "can generate json schema files from IDL"() {
        given:
        buildFile << """
            task("generateProtocol", type: com.commercehub.gradle.plugin.avro.GenerateAvroProtocolTask) {
                source file("src/main/avro")
                outputDir = file("build/generated-avro-main-avpr")
            }

            task("generateSchema", type: com.commercehub.gradle.plugin.avro.GenerateAvroSchemaTask) {
                dependsOn generateProtocol
                source file("build/generated-avro-main-avpr")
                include("**/*.avpr")
                outputDir = file("build/generated-main-avro-avsc")
            }
        """

        copyResource("interop.avdl", avroDir)

        when:
        def result = run("generateSchema")

        then:
        taskInfoAbsent || result.task(":generateSchema").outcome == SUCCESS
        projectFile("build/generated-main-avro-avsc/org/apache/avro/Foo.avsc").file
        projectFile("build/generated-main-avro-avsc/org/apache/avro/Kind.avsc").file
        projectFile("build/generated-main-avro-avsc/org/apache/avro/MD5.avsc").file
        projectFile("build/generated-main-avro-avsc/org/apache/avro/Node.avsc").file
        projectFile("build/generated-main-avro-avsc/org/apache/avro/Interop.avsc").file
    }
}
