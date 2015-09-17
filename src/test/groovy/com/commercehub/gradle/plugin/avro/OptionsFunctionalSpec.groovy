package com.commercehub.gradle.plugin.avro

import org.apache.avro.compiler.specific.SpecificCompiler.FieldVisibility
import org.apache.avro.generic.GenericData.StringType
import spock.lang.Unroll

import static com.commercehub.gradle.plugin.avro.Constants.DEFAULT_ENCODING
import static org.gradle.testkit.runner.TaskOutcome.FAILED
import static org.gradle.testkit.runner.TaskOutcome.SUCCESS

class OptionsFunctionalSpec extends FunctionalSpec {
    def "works with default options"() {
        given:
        copyResource("user.avsc", avroDir)

        when:
        def result = run("generateAvroJava")

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
        def result = run("generateAvroJava")

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
        |    stringType = "${stringType}"
        |}
        |""".stripMargin()

        when:
        def result = run("generateAvroJava")

        then: "the task succeeds"
        result.task(":generateAvroJava").outcome == SUCCESS
        def content = projectPath("build/generated-main-avro-java/example/avro/User.java").getText(DEFAULT_ENCODING)

        and: "the specified stringType is used"
        content.contains(expectedContent)

        where:
        stringType                           | expectedContent
        StringType.String.name()             | "public java.lang.String getName()"
        StringType.CharSequence.name()       | "public java.lang.CharSequence getName()"
        StringType.Utf8.name()               | "public org.apache.avro.util.Utf8 getName()"
        StringType.Utf8.name().toUpperCase() | "public org.apache.avro.util.Utf8 getName()"
        StringType.Utf8.name().toLowerCase() | "public org.apache.avro.util.Utf8 getName()"
    }

    @Unroll
    def "supports configuring fieldVisibility to #fieldVisibility"() {
        given:
        copyResource("user.avsc", avroDir)
        buildFile << """
        |avro {
        |    fieldVisibility = "${fieldVisibility}"
        |}
        |""".stripMargin()

        when:
        def result = run("generateAvroJava")

        then: "the task succeeds"
        result.task(":generateAvroJava").outcome == SUCCESS
        def content = projectPath("build/generated-main-avro-java/example/avro/User.java").getText(DEFAULT_ENCODING)

        and: "the specified fieldVisibility is used"
        content.contains(expectedContent)

        where:
        fieldVisibility                              | expectedContent
        FieldVisibility.PRIVATE.name().toLowerCase() | "private java.lang.String name;"
        FieldVisibility.PRIVATE.name()               | "private java.lang.String name;"
        FieldVisibility.PUBLIC.name()                | "public java.lang.String name;"
        FieldVisibility.PUBLIC_DEPRECATED.name()     | "@Deprecated public java.lang.String name;"
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
        def result = run("generateAvroJava")

        then: "the task succeeds"
        result.task(":generateAvroJava").outcome == SUCCESS
        def content = projectPath("build/generated-main-avro-java/example/avro/User.java").getText(DEFAULT_ENCODING)

        and: "the specified templates are used"
        content.contains("Custom template")
    }

    def "rejects unsupported stringType values"() {
        given:
        copyResource("user.avsc", avroDir)
        buildFile << """
        |avro {
        |    stringType = "badValue"
        |}
        |""".stripMargin()

        when:
        def result = runAndFail("generateAvroJava")

        then:
        result.task(":generateAvroJava").outcome == FAILED
        result.standardError.contains("Invalid stringType 'badValue'.  Value values are: [CharSequence, String, Utf8]")
    }

    def "rejects unsupported fieldVisibility values"() {
        given:
        copyResource("user.avsc", avroDir)
        buildFile << """
        |avro {
        |    fieldVisibility = "badValue"
        |}
        |""".stripMargin()

        when:
        def result = runAndFail("generateAvroJava")

        then:
        result.task(":generateAvroJava").outcome == FAILED
        result.standardError.contains("Invalid fieldVisibility 'badValue'.  Value values are: [PUBLIC, PUBLIC_DEPRECATED, PRIVATE]")
    }
}
