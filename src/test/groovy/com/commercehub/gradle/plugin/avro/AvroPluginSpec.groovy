package com.commercehub.gradle.plugin.avro

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
        mainGenerateAvroProtoTask.outputDir == project.file("build/generated-main-avro-avpr")
        testGenerateAvroProtoTask.outputDir == project.file("build/generated-test-avro-avpr")
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
        mainGenerateAvroJavaTask.outputDir == project.file("build/generated-main-avro-java")
        testGenerateAvroJavaTask.outputDir == project.file("build/generated-test-avro-java")
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
