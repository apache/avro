/*
 * Copyright © 2015-2017 Commerce Technologies, LLC.
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

import static org.gradle.testkit.runner.TaskOutcome.FAILED
import static org.gradle.testkit.runner.TaskOutcome.SUCCESS

class AvroPluginFunctionalSpec extends FunctionalSpec {
    def "setup"() {
        applyAvroPlugin()
        addDefaultRepository()
        addAvroDependency()
    }

    def "can generate and compile java files from json schema"() {
        given:
        copyResource("user.avsc", avroDir)

        when:
        def result = run()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        projectFile(buildOutputClassPath("example/avro/User.class")).file
    }

    def "can generate and compile java files from json protocol"() {
        given:
        addAvroIpcDependency()
        copyResource("mail.avpr", avroDir)

        when:
        def result = run()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        projectFile(buildOutputClassPath("org/apache/avro/test/Mail.class")).file
        projectFile(buildOutputClassPath("org/apache/avro/test/Message.class")).file
    }

    def "can generate and compile java files from IDL"() {
        given:
        copyResource("interop.avdl", avroDir)

        when:
        def result = run()
        def interopJavaContent = projectFile("build/generated-main-avro-java/org/apache/avro/Interop.java").text

        then:
        result.task(":generateAvroProtocol").outcome == SUCCESS
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        projectFile("build/generated-main-avro-java/org/apache/avro/Interop.java").file
        projectFile(buildOutputClassPath("org/apache/avro/Foo.class")).file
        projectFile(buildOutputClassPath("org/apache/avro/Interop.class")).file
        projectFile(buildOutputClassPath("org/apache/avro/Kind.class")).file
        projectFile(buildOutputClassPath("org/apache/avro/MD5.class")).file
        projectFile(buildOutputClassPath("org/apache/avro/Node.class")).file
        interopJavaContent
        interopJavaContent.contains("int intField")
        interopJavaContent.contains("long longField")
        interopJavaContent.contains("String stringField")
        interopJavaContent.contains("boolean boolField")
        interopJavaContent.contains("float floatField")
        interopJavaContent.contains("double doubleField")
        interopJavaContent.contains("java.lang.Void nullField")
        interopJavaContent.contains("java.util.List<java.lang.Double> arrayField")
        interopJavaContent =~ /Map<java.lang.String,.*Foo> mapField/
        interopJavaContent.contains("Object unionField")
        interopJavaContent.contains("Kind enumField")
        interopJavaContent.contains("MD5 fixedField")
        interopJavaContent.contains("Node recordField")
        interopJavaContent.contains("BigDecimal decimalField")
        interopJavaContent.contains("LocalDate dateField")
        interopJavaContent.contains("LocalTime timeField")
        interopJavaContent.contains("Instant timeStampField")
        interopJavaContent.contains("LocalDateTime localTimeStampField")
    }

    def "supports json schema files in subdirectories"() {
        given:
        copyResource("user.avsc", avroSubDir)

        when:
        def result = run()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        projectFile(buildOutputClassPath("example/avro/User.class")).file
    }

    def "supports json protocol files in subdirectories"() {
        given:
        addAvroIpcDependency()
        copyResource("mail.avpr", avroSubDir)

        when:
        def result = run()

        then:
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        projectFile(buildOutputClassPath("org/apache/avro/test/Mail.class")).file
        projectFile(buildOutputClassPath("org/apache/avro/test/Message.class")).file
    }

    def "supports IDL files in subdirectories"() {
        given:
        copyResource("interop.avdl", avroSubDir)

        when:
        def result = run()

        then:
        result.task(":generateAvroProtocol").outcome == SUCCESS
        result.task(":generateAvroJava").outcome == SUCCESS
        result.task(":compileJava").outcome == SUCCESS
        projectFile(buildOutputClassPath("org/apache/avro/Foo.class")).file
        projectFile(buildOutputClassPath("org/apache/avro/Interop.class")).file
        projectFile(buildOutputClassPath("org/apache/avro/Kind.class")).file
        projectFile(buildOutputClassPath("org/apache/avro/MD5.class")).file
        projectFile(buildOutputClassPath("org/apache/avro/Node.class")).file
    }

    def "gives a meaningful error message when presented a malformed schema file"() {
        given:
        copyResource("enumMalformed.avsc", avroDir)
        def errorFilePath = new File("src/main/avro/enumMalformed.avsc").path

        when:
        def result = runAndFail()

        then:
        result.task(":generateAvroJava").outcome == FAILED
        result.output.contains("> Could not resolve schema definition files:")
        result.output.contains("* $errorFilePath: \"enum\" is not a defined name. The type of the \"gender\" " +
                "field must be a defined name or a {\"type\": ...} expression.")
    }

    @SuppressWarnings(["GStringExpressionWithinString"])
    def "avro plugin correctly uses task configuration avoidance"() {
        given:
        buildFile << """
        |def configuredTasks = []
        |tasks.configureEach {
        |    println "Configured task: \${it.path}"
        |}
        |""".stripMargin()

        when:
        def result = run("help")

        then:
        def expectedConfiguredTasks = [":help"]
        if (GradleFeatures.configCache.isSupportedBy(gradleVersion)) {
            // Not sure why, but when configuration caching was introduced, the base plugin started configuring the
            // clean task even if it wasn't called.
            expectedConfiguredTasks << ":clean"
        }
        def actualConfiguredTasks = []
        result.output.findAll(/(?m)^Configured task: (.*)$/) { match, taskPath -> actualConfiguredTasks << taskPath }
        actualConfiguredTasks == expectedConfiguredTasks
    }
}
