/*
 * Copyright © 2015-2018 Commerce Technologies, LLC.
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

import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.GradleRunner
import org.gradle.util.GradleVersion
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

@SuppressWarnings(["Println"])
abstract class FunctionalSpec extends Specification {
    @SuppressWarnings(["FieldName"])
    protected static final String avroVersion = System.getProperty("avroVersion", "undefined")
    @SuppressWarnings(["FieldName"])
    protected static final GradleVersion gradleVersion = GradleVersion.version(System.getProperty("gradleVersion", "undefined"))

    @Rule
    TemporaryFolder testProjectDir

    File buildFile
    File avroDir
    File avroSubDir

    def setup() {
        println "Testing using Avro version ${avroVersion}."
        println "Testing using Gradle version ${gradleVersion}."

        buildFile = testProjectDir.newFile("build.gradle")
        avroDir = testProjectDir.newFolder("src", "main", "avro")
        avroSubDir = testProjectDir.newFolder("src", "main", "avro", "foo")
    }

    protected String readPluginClasspath() {
        def pluginClasspathResource = getClass().classLoader.findResource("plugin-classpath.txt")
        if (pluginClasspathResource == null) {
            throw new IllegalStateException("Did not find plugin classpath resource, run `testClasses` build task.")
        }

        // escape backslashes in Windows paths and assemble
        return pluginClasspathResource.readLines()*.replace('\\', '\\\\').collect { "\"$it\"" }.join(", ")
    }

    protected void applyAvroPlugin() {
        applyPlugin("com.commercehub.gradle.plugin.avro")
    }

    protected void applyAvroBasePlugin() {
        applyPlugin("com.commercehub.gradle.plugin.avro-base")
    }

    protected void applyPlugin(String pluginId) {
        buildFile << "plugins { id \"${pluginId}\" }\n"
    }

    protected void applyPlugin(String pluginId, String version) {
        buildFile << "plugins { id \"${pluginId}\" version \"${version}\" }\n"
    }

    protected void addDefaultRepository() {
        buildFile << "repositories { jcenter() }\n"
    }

    protected void addImplementationDependency(String dependencySpec) {
        addDependency("implementation", dependencySpec)
    }

    protected void addRuntimeDependency(String dependencySpec) {
        addDependency("runtimeOnly", dependencySpec)
    }

    protected void addDependency(String configuration, String dependencySpec) {
        buildFile << "dependencies { ${configuration} \"${dependencySpec}\" }\n"
    }

    protected void addAvroDependency() {
        addImplementationDependency("org.apache.avro:avro:${avroVersion}")
    }

    protected void addAvroIpcDependency() {
        addImplementationDependency("org.apache.avro:avro-ipc:${avroVersion}")
    }

    protected void copyResource(String name, File targetFolder) {
        def file = new File(targetFolder, name)
        file.parentFile.mkdirs()
        file << getClass().getResourceAsStream(name)
    }

    protected void copyFile(String srcDir, String destDir, String path) {
        def destFile = new File(projectFile(destDir), path)
        def srcFile = new File(srcDir, path)
        destFile.parentFile.mkdirs()
        destFile << srcFile.bytes
    }

    protected File projectFile(String path) {
        return new File(testProjectDir.root, path)
    }

    protected GradleRunner createGradleRunner() {
        return GradleRunner.create().withProjectDir(testProjectDir.root).withGradleVersion(gradleVersion.version).withPluginClasspath()
    }

    protected BuildResult run(String... args = ["build"]) {
        return createGradleRunner().withArguments(determineGradleArguments(args)).build()
    }

    protected BuildResult runAndFail(String... args = ["build"]) {
        return createGradleRunner().withArguments(determineGradleArguments(args)).buildAndFail()
    }

    protected String buildOutputClassPath(String suffix) {
        return "build/classes/java/main/${suffix}"
    }

    private List<String> determineGradleArguments(String... args) {
        def arguments = ["--stacktrace"]
        arguments.addAll(Arrays.asList(args))
        if (GradleFeatures.configCache.isSupportedBy(gradleVersion) && !arguments.contains("--no-configuration-cache")) {
            arguments << "--configuration-cache"
        }
        return arguments
    }
}
