/*
 * Copyright © 2017 Commerce Technologies, LLC.
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
    @IgnoreIf({
        javaVersion < 1.8 ||
            (gradleVersion >= GradleVersion.version("3.2") && gradleVersion <= GradleVersion.version("3.2.1"))
    })
    def "works with kotlin-gradle-plugin"() {
        given:
        File kotlinDir = testProjectDir.newFolder("src", "main", "kotlin")
        buildFile << """
            buildscript {
                repositories {
                    jcenter()
                }
                dependencies {
                    classpath "org.jetbrains.kotlin:kotlin-gradle-plugin:1.2.31"
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
