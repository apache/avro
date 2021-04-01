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
package com.github.davidmc24.gradle.plugin.avro

import org.gradle.testkit.runner.BuildResult
import org.gradle.util.GradleVersion

@SuppressWarnings(["Println"])
class KotlinPluginCompatibilityFunctionalSpec extends FunctionalSpec {
    @SuppressWarnings(["FieldName"])
    protected static final String kotlinPluginVersion = System.getProperty("kotlinPluginVersion", "undefined")

    def "setup"() {
        println "Testing using Kotlin plugin version ${kotlinPluginVersion}."
    }

    def "works with kotlin-gradle-plugin"() {
        given:
        File kotlinDir = testProjectDir.newFolder("src", "main", "kotlin")
        applyAvroPlugin()
        applyPlugin("org.jetbrains.kotlin.jvm", kotlinPluginVersion)
        applyPlugin("application")
        addDefaultRepository()
        addAvroDependency()
        addImplementationDependency("org.jetbrains.kotlin:kotlin-stdlib")
        addRuntimeDependency("joda-time:joda-time:2.9.9")
        buildFile << 'mainClassName = "demo.HelloWorldKt"'
        copyResource("user.avsc", avroDir)
        copyResource("helloWorld.kt", kotlinDir)

        when:
        def result = runBuild()

        then:
        result.output.contains("Hello, David")
    }

    private BuildResult runBuild() {
        def args = ["run"]
        if (GradleFeatures.configCache.isSupportedBy(gradleVersion)) {
            // The kotlin plugin prior to 1.4.20 doesn't support the configuration cache, so we need to disable it.
            // This is a bit of a mis-use of the GradleVersion class, but it's way easier than writing our own
            // version comparison logic.
            if (GradleVersion.version(kotlinPluginVersion) < GradleVersion.version("1.4.20")) {
                args << "--no-configuration-cache"
            }
        }
        return run(args as String[])
    }
}
