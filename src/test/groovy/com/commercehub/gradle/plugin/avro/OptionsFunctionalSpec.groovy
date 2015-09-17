package com.commercehub.gradle.plugin.avro

import org.apache.avro.compiler.specific.SpecificCompiler.FieldVisibility
import org.apache.avro.generic.GenericData.StringType
import spock.lang.Unroll

import static com.commercehub.gradle.plugin.avro.Constants.DEFAULT_ENCODING
import static org.gradle.testkit.runner.TaskOutcome.SUCCESS

class OptionsFunctionalSpec extends FunctionalSpec {
    def "works with default options"() {
        given:
        copyResource("user.avsc", avroDir)

        when:
        def result = runBuild("generateAvroJava")

        then: "the task succeeds"
        result.task(":generateAvroJava").outcome == SUCCESS

        and: "the encoding is the default"
        def content = projectPath("build/generated-main-avro-java/example/avro/User.java").getText(DEFAULT_ENCODING)

        and: "the stringType is string"
        content.contains("public java.lang.String getName()")

        and: "the fieldVisibility is PUBLIC_DEPRECATED"
        content.contains("@Deprecated public java.lang.String name;")

        and: "the default template is used"
        !content.contains("Custom template")
    }

    def "supports configuring encoding"() {
        given:
        def encoding = "UTF-16"
        copyResource("user.avsc", avroDir)
        buildFile << """
        |avro {
        |    encoding = "${encoding}"
        |}
        |""".stripMargin()

        when:
        def result = runBuild("generateAvroJava")

        then: "the task succeeds"
        result.task(":generateAvroJava").outcome == SUCCESS

        and: "the specified encoding is used"
        projectPath("build/generated-main-avro-java/example/avro/User.java").getText(encoding)
    }

    @Unroll
    def "supports configuring stringType to #stringType"() {
        given:
        copyResource("user.avsc", avroDir)
        buildFile << """
        |avro {
        |    stringType = "${stringType.name()}"
        |}
        |""".stripMargin()

        when:
        def result = runBuild("generateAvroJava")

        then: "the task succeeds"
        result.task(":generateAvroJava").outcome == SUCCESS
        def content = projectPath("build/generated-main-avro-java/example/avro/User.java").getText(DEFAULT_ENCODING)

        and: "the specified stringType is used"
        content.contains(expectedContent)

        where:
        stringType              | expectedContent
        StringType.String       | "public java.lang.String getName()"
        StringType.CharSequence | "public java.lang.CharSequence getName()"
        StringType.Utf8         | "public org.apache.avro.util.Utf8 getName()"
    }

    @Unroll
    def "supports configuring fieldVisibility to #fieldVisibility"() {
        given:
        copyResource("user.avsc", avroDir)
        buildFile << """
        |avro {
        |    fieldVisibility = "${fieldVisibility.name()}"
        |}
        |""".stripMargin()

        when:
        def result = runBuild("generateAvroJava")

        then: "the task succeeds"
        result.task(":generateAvroJava").outcome == SUCCESS
        def content = projectPath("build/generated-main-avro-java/example/avro/User.java").getText(DEFAULT_ENCODING)

        and: "the specified fieldVisibility is used"
        content.contains(expectedContent)

        where:
        fieldVisibility                   | expectedContent
        FieldVisibility.PRIVATE           | "private java.lang.String name;"
        FieldVisibility.PUBLIC            | "public java.lang.String name;"
        FieldVisibility.PUBLIC_DEPRECATED | "@Deprecated public java.lang.String name;"
    }

    def "supports configuring templateDirectory"() {
        given:
        def templatesDir = testProjectDir.newFolder("templates", "alternateTemplates")
        copyResource("user.avsc", avroDir)
        copyResource("record.vm", templatesDir)
        buildFile << """
        |buildscript {
        |    dependencies {
        |        classpath files(["${templatesDir.parentFile.absolutePath}"])
        |    }
        |}
        |avro {
        |    templateDirectory = "/alternateTemplates/"
        |}
        |""".stripMargin()

        when:
        def result = runBuild("generateAvroJava")

        then: "the task succeeds"
        result.task(":generateAvroJava").outcome == SUCCESS
        def content = projectPath("build/generated-main-avro-java/example/avro/User.java").getText(DEFAULT_ENCODING)

        and: "the specified templates are used"
        content.contains("Custom template")
    }

    // TODO: invalid value handling
}
