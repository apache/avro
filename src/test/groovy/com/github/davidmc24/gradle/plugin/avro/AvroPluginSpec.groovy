/*
 * Copyright © 2013-2019 Commerce Technologies, LLC.
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

import org.gradle.api.Project
import org.gradle.testfixtures.ProjectBuilder
import spock.lang.Specification

class AvroPluginSpec extends Specification {
    Project project = ProjectBuilder.builder().build()

    def "avro protocol generation tasks are registered"() {
        when: "the plugin is applied to a project"
        project.apply(plugin: AvroPlugin)

        then: "avro protocol generation tasks are registered with the project with appropriate configuration"
        project.tasks.withType(GenerateAvroProtocolTask)*.name.sort() == ["generateAvroProtocol", "generateTestAvroProtocol"]
        mainGenerateAvroProtoTask.description == "Generates main Avro protocol definition files from IDL files."
        testGenerateAvroProtoTask.description == "Generates test Avro protocol definition files from IDL files."
        mainGenerateAvroProtoTask.group == Constants.GROUP_SOURCE_GENERATION
        testGenerateAvroProtoTask.group == Constants.GROUP_SOURCE_GENERATION
        // Can't easily test the sources
        mainGenerateAvroProtoTask.outputDir.get().asFile == project.file("build/generated-main-avro-avpr")
        testGenerateAvroProtoTask.outputDir.get().asFile == project.file("build/generated-test-avro-avpr")
    }

    def "avro java generation tasks are registered"() {
        when: "the plugin is applied to a project"
        project.apply(plugin: AvroPlugin)

        then: "avro java generation tasks are registered with the project with appropriate configuration"
        project.tasks.withType(GenerateAvroJavaTask)*.name.sort() == ["generateAvroJava", "generateTestAvroJava"]
        mainGenerateAvroJavaTask.description == "Generates main Avro Java source files from schema/protocol definition files."
        testGenerateAvroJavaTask.description == "Generates test Avro Java source files from schema/protocol definition files."
        mainGenerateAvroJavaTask.group == Constants.GROUP_SOURCE_GENERATION
        testGenerateAvroJavaTask.group == Constants.GROUP_SOURCE_GENERATION
        // Can't easily test the sources
        mainGenerateAvroJavaTask.outputDir.get().asFile == project.file("build/generated-main-avro-java")
        testGenerateAvroJavaTask.outputDir.get().asFile == project.file("build/generated-test-avro-java")
    }

    OutputDirTask getTask(String name) {
        return project.tasks.getByName(name) as OutputDirTask
    }

    OutputDirTask getMainGenerateAvroProtoTask() {
        return getTask("generateAvroProtocol")
    }

    OutputDirTask getTestGenerateAvroProtoTask() {
        return getTask("generateTestAvroProtocol")
    }

    OutputDirTask getMainGenerateAvroJavaTask() {
        return getTask("generateAvroJava")
    }

    OutputDirTask getTestGenerateAvroJavaTask() {
        return getTask("generateTestAvroJava")
    }
}
