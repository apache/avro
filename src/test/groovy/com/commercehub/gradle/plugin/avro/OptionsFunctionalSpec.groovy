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

import org.apache.avro.compiler.specific.SpecificCompiler.FieldVisibility
import org.apache.avro.generic.GenericData.StringType
import spock.lang.Unroll

import java.nio.ByteBuffer

import static org.gradle.testkit.runner.TaskOutcome.FAILED
import static org.gradle.testkit.runner.TaskOutcome.SUCCESS

/**
 * Functional tests for most functions.  Encoding tests have been pulled out into {@link EncodingFunctionalSpec}
 */
class OptionsFunctionalSpec extends FunctionalSpec {

    def "works with default options"() {
        given:
        copyResource("user.avsc", avroDir)
        applyAvroPlugin()

        when:
        def result = run("generateAvroJava")

        then: "the task succeeds"
        result.task(":generateAvroJava").outcome == SUCCESS
        def content = projectFile("build/generated-main-avro-java/example/avro/User.java").text

        and: "the stringType is string"
        content.contains("public java.lang.String getName()")

        and: "the fieldVisibility is PUBLIC_DEPRECATED"
        content.contains("@Deprecated public java.lang.String name;")

        and: "the default template is used"
        !content.contains("Custom template")

        and: "createSetters is enabled"
        content.contains("public void setName(java.lang.String value)")

        and: "createOptionalGetters is disabled"
        !content.contains("Optional")

        and: "gettersReturnOptional is disabled"
        !content.contains("Optional")

        and: "enableDecimalLogicalType is enabled"
        content.contains("public void setSalary(${BigDecimal.name} value)")
    }

    @Unroll
    def "supports configuring stringType to #stringType"() {
        given:
        copyResource("user.avsc", avroDir)
        applyAvroPlugin()
        buildFile << """
        |avro {
        |    stringType = ${stringType}
        |}
        |""".stripMargin()

        when:
        def result = run("generateAvroJava")

        then: "the task succeeds"
        result.task(":generateAvroJava").outcome == SUCCESS
        def content = projectFile("build/generated-main-avro-java/example/avro/User.java").text
        def mainClassContent = getMainClassContent(content)

        and: "the specified stringType is used"
        mainClassContent.contains(expectedContent)

        where:
        stringType                                     | expectedContent
        "'${StringType.String.name()}'"                | "public java.lang.String getName()"
        "'${StringType.CharSequence.name()}'"          | "public java.lang.CharSequence getName()"
        "'${StringType.Utf8.name()}'"                  | "public org.apache.avro.util.Utf8 getName()"
        "'${StringType.Utf8.name().toUpperCase()}'"    | "public org.apache.avro.util.Utf8 getName()"
        "'${StringType.Utf8.name().toLowerCase()}'"    | "public org.apache.avro.util.Utf8 getName()"
        "${StringType.name}.${StringType.Utf8.name()}" | "public org.apache.avro.util.Utf8 getName()"
    }

    @Unroll
    def "supports configuring fieldVisibility to #fieldVisibility"() {
        given:
        copyResource("user.avsc", avroDir)
        applyAvroPlugin()
        buildFile << """
        |avro {
        |    fieldVisibility = "${fieldVisibility}"
        |}
        |""".stripMargin()

        when:
        def result = run("generateAvroJava")

        then: "the task succeeds"
        result.task(":generateAvroJava").outcome == SUCCESS
        def content = projectFile("build/generated-main-avro-java/example/avro/User.java").text
        def mainClassContent = getMainClassContent(content)

        and: "the specified fieldVisibility is used"
        mainClassContent.contains(expectedContent)

        where:
        fieldVisibility                              | expectedContent
        FieldVisibility.PRIVATE.name().toLowerCase() | "private java.lang.String name;"
        FieldVisibility.PRIVATE.name()               | "private java.lang.String name;"
        FieldVisibility.PUBLIC.name()                | "public java.lang.String name;"
        FieldVisibility.PUBLIC_DEPRECATED.name()     | "@Deprecated public java.lang.String name;"
    }

    @Unroll
    def "supports configuring createSetters to #createSetters"() {
        given:
        copyResource("user.avsc", avroDir)
        applyAvroPlugin()
        buildFile << """
        |avro {
        |    createSetters = ${createSetters}
        |}
        |""".stripMargin()

        when:
        def result = run("generateAvroJava")

        then: "the task succeeds"
        result.task(":generateAvroJava").outcome == SUCCESS
        def content = projectFile("build/generated-main-avro-java/example/avro/User.java").text
        def mainClassContent = getMainClassContent(content)

        and: "the specified createSetters is used"
        mainClassContent.contains("public void setName(java.lang.String value)") == expectedPresent

        where:
        createSetters   | expectedPresent
        "Boolean.TRUE"  | true
        "Boolean.FALSE" | false
        "true"          | true
        "false"         | false
        "'true'"        | true
        "'false'"       | false
    }

    @Unroll
    def "supports configuring createOptionalGetters to #createOptionalGetters"() {
        given:
        copyResource("user.avsc", avroDir)
        applyAvroPlugin()
        buildFile << """
        |avro {
        |    createOptionalGetters = ${createOptionalGetters}
        |}
        |""".stripMargin()

        when:
        def result = run("generateAvroJava")

        then: "the task succeeds"
        result.task(":generateAvroJava").outcome == SUCCESS
        def content = projectFile("build/generated-main-avro-java/example/avro/User.java").text
        def mainClassContent = getMainClassContent(content)

        and: "the nullable getter is generated"
        content.contains("public java.lang.String getFavoriteColor()")
        
        and: "the specified createOptionalGetters is used"
        mainClassContent.contains("public Optional<java.lang.String> getOptionalFavoriteColor()") == expectedPresent
        mainClassContent.contains("public java.lang.String getFavoriteColor()")

        where:
        createOptionalGetters | expectedPresent
        "Boolean.TRUE"        | true
        "Boolean.FALSE"       | false
        "true"                | true
        "false"               | false
        "'true'"              | true
        "'false'"             | false
    }

    @SuppressWarnings("LineLength")
    @Unroll
    def "supports configuring gettersReturnOptional/optionalGettersForNullableFieldsOnly to #gettersReturnOptional/#optionalGettersForNullableFieldsOnly"() {
        given:
        copyResource("user.avsc", avroDir)
        applyAvroPlugin()
        buildFile << """
        |avro {
        |    gettersReturnOptional = ${gettersReturnOptional}
        |    optionalGettersForNullableFieldsOnly = ${optionalGettersForNullableFieldsOnly}
        |}
        |""".stripMargin()

        when:
        def result = run("generateAvroJava")

        then: "the task succeeds"
        result.task(":generateAvroJava").outcome == SUCCESS
        def content = projectFile("build/generated-main-avro-java/example/avro/User.java").text
        def mainClassContent = getMainClassContent(content)

        and: "the specified optionalGettersForNullableFieldsOnly is used"
        mainClassContent.contains("public Optional<java.lang.String> getFavoriteColor()") == expectedNullableOptionalGetter
        mainClassContent.contains("public java.lang.String getFavoriteColor()") != expectedNullableOptionalGetter
        mainClassContent.contains("public Optional<java.lang.String> getName()") == expectedRequiredOptionalGetter
        mainClassContent.contains("public java.lang.String getName()") != expectedRequiredOptionalGetter


        where:
        gettersReturnOptional | optionalGettersForNullableFieldsOnly | expectedNullableOptionalGetter | expectedRequiredOptionalGetter
        "Boolean.TRUE"        | "Boolean.TRUE"                       | true                           | false
        "Boolean.TRUE"        | "Boolean.FALSE"                      | true                           | true
        "Boolean.FALSE"       | "Boolean.TRUE"                       | false                          | false
        "Boolean.FALSE"       | "Boolean.FALSE"                      | false                          | false
        "true"                | "true"                               | true                           | false
        "true"                | "false"                              | true                           | true
        "false"               | "true"                               | false                          | false
        "false"               | "false"                              | false                          | false
        "'true'"              | "'true'"                             | true                           | false
        "'true'"              | "'false'"                            | true                           | true
        "'false'"             | "'true'"                             | false                          | false
        "'false'"             | "'false'"                            | false                          | false
    }

    def "supports configuring templateDirectory"() {
        given:
        def templatesDir = testProjectDir.newFolder("templates", "alternateTemplates")
        copyResource("user.avsc", avroDir)
        copyResource("record.vm", templatesDir)
        // This functionality doesn't work with the plugins DSL syntax.
        // To load files from the buildscript classpath you need to load the plugin from it as well.
        buildFile << """
        |buildscript {
        |    dependencies {
        |        classpath files(${readPluginClasspath()})
        |        classpath files(["${templatesDir.parentFile.toURI()}"])
        |    }
        |}
        |apply plugin: "com.commercehub.gradle.plugin.avro"
        |avro {
        |    templateDirectory = "/alternateTemplates/"
        |}
        |""".stripMargin()

        when:
        def result = run("generateAvroJava")

        then: "the task succeeds"
        result.task(":generateAvroJava").outcome == SUCCESS
        def content = projectFile("build/generated-main-avro-java/example/avro/User.java").text

        and: "the specified templates are used"
        content.contains("Custom template")
    }

    def "rejects unsupported stringType values"() {
        given:
        copyResource("user.avsc", avroDir)
        applyAvroPlugin()
        buildFile << """
        |avro {
        |    stringType = "badValue"
        |}
        |""".stripMargin()

        when:
        def result = runAndFail("generateAvroJava")

        then:
        result.task(":generateAvroJava").outcome == FAILED
        result.output.contains("Invalid stringType 'badValue'.  Value values are: [CharSequence, String, Utf8]")
    }

    def "rejects unsupported fieldVisibility values"() {
        given:
        copyResource("user.avsc", avroDir)
        applyAvroPlugin()
        buildFile << """
        |avro {
        |    fieldVisibility = "badValue"
        |}
        |""".stripMargin()

        when:
        def result = runAndFail("generateAvroJava")

        then:
        result.task(":generateAvroJava").outcome == FAILED
        result.output.contains("Invalid fieldVisibility 'badValue'.  Value values are: [PUBLIC, PUBLIC_DEPRECATED, PRIVATE]")
    }

    @Unroll
    def "supports configuring enableDecimalLogicalType to #enableDecimalLogicalType"() {
        given:
        copyResource("user.avsc", avroDir)
        applyAvroPlugin()
        buildFile << """
        |avro {
        |    enableDecimalLogicalType = $enableDecimalLogicalType
        |}
        |""".stripMargin()

        when:
        def result = run("generateAvroJava")

        then: "the task succeeds"
        result.task(":generateAvroJava").outcome == SUCCESS
        def content = projectFile("build/generated-main-avro-java/example/avro/User.java").text
        def mainClassContent = getMainClassContent(content)

        and: "the specified enableDecimalLogicalType is used"
        mainClassContent.contains("public void setSalary(${fieldClz.name} value)")

        where:
        enableDecimalLogicalType | fieldClz
        "Boolean.TRUE"           | BigDecimal
        "Boolean.FALSE"          | ByteBuffer
        "true"                   | BigDecimal
        "false"                  | ByteBuffer
        "'true'"                 | BigDecimal
        "'false'"                | ByteBuffer
    }

    /**
     * Returns just the portion of a file that relates to the main class.
     * This is used in order to allow assertions on the getters/setters/fields of the generated class itself, as opposed to a Builder.
     *
     * @param content the file content for which to get the main content
     * @return the content of the class, from the start of the class body to the first inner class definition
     */
    @SuppressWarnings("LineLength")
    private static String getMainClassContent(String content) {
        def className = "User"
        def matcher = content =~ /(?s)public class ${className} extends org\.apache\.avro\.specific\.\SpecificRecordBase implements org\.apache\.avro\.specific\.SpecificRecord \{(?<mainClassContent>.*)public static class Builder/
        assert matcher.find()
        return matcher.group("mainClassContent")
    }
}
