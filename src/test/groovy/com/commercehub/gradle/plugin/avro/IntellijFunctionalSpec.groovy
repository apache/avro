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

import org.gradle.testkit.runner.BuildResult

import static org.gradle.testkit.runner.TaskOutcome.SUCCESS

class IntellijFunctionalSpec extends FunctionalSpec {
    def "setup"() {
        applyAvroPlugin()
        applyPlugin("idea")
    }

    def "generated intellij project files include source directories for generated source"() {
        given:
        copyResource("user.avsc", avroDir)
        testProjectDir.newFolder("src", "main", "java")
        testProjectDir.newFolder("src", "test", "java")
        testProjectDir.newFolder("src", "test", "avro")

        when:
        runIdea()

        then:
        def moduleFile = new File(testProjectDir.root, "${testProjectDir.root.name}.iml")
        def module = new XmlSlurper().parseText(moduleFile.text)
        module.component.content.sourceFolder.findAll { it.@isTestSource.text() == "false" }.@url*.text().sort() == [
            'file://$MODULE_DIR\$/build/generated-main-avro-java',
            'file://$MODULE_DIR\$/src/main/avro', 'file://$MODULE_DIR\$/src/main/java',
        ]
        module.component.content.sourceFolder.findAll { it.@isTestSource.text() == "true" }.@url*.text().sort() == [
            'file://$MODULE_DIR\$/build/generated-test-avro-java',
            'file://$MODULE_DIR\$/src/test/avro', 'file://$MODULE_DIR\$/src/test/java',
        ]
    }

    def "generated output directories are created by default"() {
        when:
        def result = runIdea()

        then:
        result.task(":idea").outcome == SUCCESS
        projectFile("build/generated-main-avro-java").directory
        projectFile("build/generated-test-avro-java").directory
    }

    def "overriding task's outputDir doesn't result in default directory still being created"() {
        given:
        buildFile << """
        |tasks.named("generateAvroJava").configure {
        |    outputDir = file("build/generatedMainAvro")
        |}
        |tasks.named("generateTestAvroJava").configure {
        |    outputDir = file("build/generatedTestAvro")
        |}
        |""".stripMargin()

        when:
        def result = runIdea()

        then:
        result.task(":idea").outcome == SUCCESS
        !projectFile("build/generated-main-avro-java").directory
        !projectFile("build/generated-test-avro-java").directory
        projectFile("build/generatedMainAvro").directory
        projectFile("build/generatedTestAvro").directory
    }

    private BuildResult runIdea() {
        def args = ["idea"]
        if (GradleFeatures.configCache.isSupportedBy(gradleVersion)) {
            // As of Gradle 6.7.1, idea plugin doesn't support configuration cache yet.
            // Thus, don't try to use it in this spec.
            args << "--no-configuration-cache"
        }
        return run(args as String[])
    }
}
