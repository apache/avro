package com.commercehub.gradle.plugin.avro

import org.gradle.util.GradleVersion
import spock.lang.IgnoreIf

class KotlinCompatibilityFunctionalSpec extends FunctionalSpec {
    /**
     * Since Kotlin 1.1.2, the Kotlin compiler requires Java 8+
     * https://blog.jetbrains.com/kotlin/2017/04/kotlin-1-1-2-is-out/
     *
     * Kotlin support appears broken on Gradle 3.2-3.2.1
     * https://discuss.kotlinlang.org/t/1-1-50-js-compiler-requires-kotlin-reflect/4699
     */
    @IgnoreIf({ javaVersion < 1.8 ||
        (gradleVersion >= GradleVersion.version("3.2") && gradleVersion <= GradleVersion.version("3.2.1")) }
    )
    def "works with kotlin-gradle-plugin"() {
        given:
        File kotlinDir = testProjectDir.newFolder("src", "main", "kotlin")
        buildFile << """
            buildscript {
                repositories {
                    jcenter()
                }
                dependencies {
                    classpath "org.jetbrains.kotlin:kotlin-gradle-plugin:1.1.51"
                }
            }
            apply plugin: "kotlin"
            apply plugin: "application"
            repositories {
                jcenter()
            }
            dependencies {
                compile "org.jetbrains.kotlin:kotlin-stdlib"
                runtime "joda-time:joda-time:2.9.9"
            }
            mainClassName = "demo.HelloWorldKt"
        """
        copyResource("user.avsc", avroDir)
        copyResource("helloWorld.kt", kotlinDir)

        when:
        def result = run("run")

        then:
        result.output.contains("Hello, David")
    }
}
