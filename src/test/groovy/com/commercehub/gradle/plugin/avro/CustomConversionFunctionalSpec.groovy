/*
 * Copyright © 2019 David M. Carr
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

class CustomConversionFunctionalSpec extends FunctionalSpec {
    def "setup"() {
        applyAvroPlugin()
        addAvroDependency()
    }

    private void copyCustomConversion(String destDir) {
        copyFile("src/test/java", destDir,
            "com/commercehub/gradle/plugin/avro/test/custom/TimeZoneConversion.java")
        copyFile("src/test/java", destDir,
            "com/commercehub/gradle/plugin/avro/test/custom/TimeZoneLogicalType.java")
        copyFile("src/test/java", destDir,
            "com/commercehub/gradle/plugin/avro/test/custom/TimeZoneLogicalTypeFactory.java")
    }

    def "can use a custom conversion when generating java from a schema with stringType = \"String\""() {
        // since Avro 1.9.2 https://issues.apache.org/jira/browse/AVRO-2548 is fixed
        // This is a behavior of the buildscript version of avro rather than the compile-time one,
        // so our version compatibility tests won't cover the difference
        given:
        copyResource("customConversion.avsc", avroDir)
        buildFile << """
            import com.commercehub.gradle.plugin.avro.test.custom.*
            avro {
                stringType = "String"
                logicalTypeFactory("timezone", TimeZoneLogicalTypeFactory)
                customConversion(TimeZoneConversion)
            }
        """
        testProjectDir.newFolder("buildSrc")
        testProjectDir.newFile("buildSrc/build.gradle") << """
            repositories {
                jcenter()
            }
            dependencies {
                compile "org.apache.avro:avro:${avroVersion}"
            }
        """
        copyCustomConversion("buildSrc/src/main/java")
        copyCustomConversion("src/main/java")

        when:
        def result = run()

        then:
        taskInfoAbsent || result.task(":generateAvroJava").outcome == SUCCESS
        taskInfoAbsent || result.task(":compileJava").outcome == SUCCESS
        projectFile(buildOutputClassPath("test/Event.class")).file
        def javaSource = projectFile("build/generated-main-avro-java/test/Event.java").text
        javaSource.contains("java.time.Instant start;")
        javaSource.contains("java.util.TimeZone timezone;")
    }

    def "can use a custom conversion when generating java from a schema"() {
        // As of Avro 1.9.1, custom conversions have an undesirable interaction with stringType=String.
        // See https://issues.apache.org/jira/browse/AVRO-2548
        given:
        copyResource("customConversion.avsc", avroDir)
        buildFile << """
            import com.commercehub.gradle.plugin.avro.test.custom.*
            avro {
                stringType = "CharSequence"
                logicalTypeFactory("timezone", TimeZoneLogicalTypeFactory)
                customConversion(TimeZoneConversion)
            }
        """
        testProjectDir.newFolder("buildSrc")
        testProjectDir.newFile("buildSrc/build.gradle") << """
            repositories {
                jcenter()
            }
            dependencies {
                compile "org.apache.avro:avro:${avroVersion}"
            }
        """
        copyCustomConversion("buildSrc/src/main/java")
        copyCustomConversion("src/main/java")

        when:
        def result = run()

        then:
        taskInfoAbsent || result.task(":generateAvroJava").outcome == SUCCESS
        taskInfoAbsent || result.task(":compileJava").outcome == SUCCESS
        projectFile(buildOutputClassPath("test/Event.class")).file
        def javaSource = projectFile("build/generated-main-avro-java/test/Event.java").text
        javaSource.contains("java.time.Instant start;")
        javaSource.contains("java.util.TimeZone timezone;")
    }

    def "can use a custom conversion when generating java from a protocol"() {
        // As of Avro 1.9.1, custom conversions have an undesirable interaction with stringType=String.
        // See https://issues.apache.org/jira/browse/AVRO-2548
        given:
        copyResource("customConversion.avpr", avroDir)
        buildFile << """
            import com.commercehub.gradle.plugin.avro.test.custom.*
            avro {
                stringType = "CharSequence"
                logicalTypeFactory("timezone", TimeZoneLogicalTypeFactory)
                customConversion(TimeZoneConversion)
            }
        """
        testProjectDir.newFolder("buildSrc")
        testProjectDir.newFile("buildSrc/build.gradle") << """
            repositories {
                jcenter()
            }
            dependencies {
                compile "org.apache.avro:avro:${avroVersion}"
            }
        """
        copyCustomConversion("buildSrc/src/main/java")
        copyCustomConversion("src/main/java")

        when:
        def result = run()

        then:
        taskInfoAbsent || result.task(":generateAvroJava").outcome == SUCCESS
        taskInfoAbsent || result.task(":compileJava").outcome == SUCCESS
        projectFile(buildOutputClassPath("test/Event.class")).file
        def javaSource = projectFile("build/generated-main-avro-java/test/Event.java").text
        javaSource.contains("java.time.Instant start;")
        javaSource.contains("java.util.TimeZone timezone;")
    }

    def "can use a custom logical type while generating a schema from a protocol"() {
        given:
        copyResource("customConversion.avpr", avroDir)
        buildFile << """
            task("generateSchema", type: com.commercehub.gradle.plugin.avro.GenerateAvroSchemaTask) {
                source file("src/main/avro")
                include("**/*.avpr")
                outputDir = file("build/generated-main-avro-avsc")
            }
        """

        when:
        def result = run("generateSchema")

        then:
        taskInfoAbsent || result.task(":generateSchema").outcome == SUCCESS
        def schemaFile = projectFile("build/generated-main-avro-avsc/test/Event.avsc").text
        schemaFile.contains('"logicalType" : "timestamp-millis"')
        schemaFile.contains('"logicalType" : "timezone"')
    }
}
