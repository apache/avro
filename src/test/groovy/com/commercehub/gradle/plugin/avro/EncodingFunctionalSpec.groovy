package com.commercehub.gradle.plugin.avro

import spock.lang.Unroll

import java.nio.charset.Charset

import static org.gradle.testkit.runner.TaskOutcome.SUCCESS

class EncodingFunctionalSpec extends FunctionalSpec {
    private static final List<String> LANGUAGES = ["alemán", "chino", "español", "francés", "inglés", "japonés"]
    /* Not all encodings have the characters needed for the test file, and not all encoding may be supported by any given JRE */
    private static final List<String> AVAILABLE_ENCODINGS =
        ["UTF-8", "UTF-16", "UTF-32", "windows-1252", "X-MacRoman"].findAll { Charset.isSupported(it) }

    def "default encoding matches default compilation behavior"() {
        given:
        copyResource("idioma.avsc", avroDir)

        when:
        def result = run()

        then: "compilation succeeds"
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS

        and: "the system default encoding is used"
        def content = projectFile("build/generated-main-avro-java/example/avro/Idioma.java").text
        LANGUAGES.collect { content.contains(it) }.every { it }
    }

    @Unroll
    def "supports configuring outputCharacterEncoding to #outputCharacterEncoding"() {
        given:
        copyResource("idioma.avsc", avroDir)
        buildFile << """
        |avro {
        |    outputCharacterEncoding = ${outputCharacterEncoding}
        |}
        |""".stripMargin()

        when:
        def result = run("generateAvroJava")

        then: "compilation succeeds"
        result.task(":generateAvroJava").outcome == SUCCESS

        and: "the specified encoding is used"
        def content = projectFile("build/generated-main-avro-java/example/avro/Idioma.java").getText(expectedEncoding)
        LANGUAGES.collect { content.contains(it) }.every { it }

        where:
        outputCharacterEncoding                      | expectedEncoding
        "'UTF-16'"                                   | "UTF-16"
        "'utf-8'"                                    | "UTF-8"
        "java.nio.charset.Charset.forName('UTF-16')" | "UTF-16"
    }

    @Unroll
    def "uses configured encoding #encoding from java compilation task"() {
        given:
        copyResource("idioma.avsc", avroDir)
        buildFile << """
        |compileJava {
        |    options.encoding = '${encoding}'
        |}
        |""".stripMargin()

        when:
        def result = run()

        then: "compilation succeeds"
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS

        and: "the specified encoding is used"
        def content = projectFile("build/generated-main-avro-java/example/avro/Idioma.java").getText(encoding)
        LANGUAGES.collect { content.contains(it) }.every { it }

        where:
        encoding << AVAILABLE_ENCODINGS
    }
}
