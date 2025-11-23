package org.apache.avro.gradle.plugin

import org.apache.avro.gradle.plugin.tasks.CompileSchemaTask
import org.gradle.testfixtures.ProjectBuilder
import org.gradle.testkit.runner.GradleRunner
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.Arrays
import java.util.HashSet
import kotlin.io.path.writeText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue


class SamplePluginTest {

    @Test
    fun `plugin is applied correctly to the project`() {
        val project = ProjectBuilder.builder().build()
        project.pluginManager.apply("org.apache.avro.gradle.plugin")
        assert(project.tasks.getByName("compile") is CompileSchemaTask)
    }

    @Test
    fun `plugin executes greet task successfully2`() {
        // Create a temporary project directory using NIO
        val projectDir: Path = Files.createTempDirectory("gradle-plugin-test-")

        // Write minimal settings and build files
        val settingsFile = projectDir.resolve("settings.gradle.kts")
        val buildFile = projectDir.resolve("build.gradle.kts")


        // 1. Find test/avro dir inside THIS project
        val localAvroDir = File("src/test/avro")
        require(localAvroDir.exists()) { "src/test/avro not found" }

        // ---- 2. Create test project structure ----
        val testResourcesDir = File(projectDir.toFile(), "src/test/avro")
        testResourcesDir.mkdirs()

        // ---- 3. Copy resource files to test project ----
        localAvroDir.copyRecursively(testResourcesDir, overwrite = true)



        settingsFile.writeText("")
        buildFile.writeText(
            """
            plugins {
                id("org.apache.avro.avro-gradle-plugin")
            }
            
            avro {
                srcDirectory = "src/test/avro"
                outputDirectory = "generated-sources/avro"
                includes = listOf("**/*.avsc")
            }
        """.trimIndent()
        )

        val outputDirectory = File(projectDir.toFile(), "build/generated-sources/avro/test")

        // Run Gradle using TestKit
        val result = GradleRunner.create()
            .withProjectDir(projectDir.toFile())  // still needs File for GradleRunner
            .withArguments("avroGenerateJavaClasses")
            .withPluginClasspath()
            .build()

        println("result.output: ${result.output}")

        val expectedFiles = setOf(
            "SchemaPrivacy.java",
            "SchemaUser.java",
            "PrivacyImport.java",
            "SchemaCustom.java",
            "PrivacyDirectImport.java"
        )

        // Verify output
        assertFilesExist(outputDirectory, expectedFiles)

        //assertTrue(result.output.contains("Hello from project"))
        //assertTrue(result.output.contains("BUILD SUCCESSFUL"))

        // Optional: clean up directory
        projectDir.toFile().deleteRecursively()
    }

    fun assertFilesExist(directory: File, expectedFiles: Set<String>) {
        assertNotNull(directory)
        assertTrue(directory.exists(), "Directory " + directory.toString() + " does not exists")
        assertNotNull(expectedFiles)
        assertTrue(expectedFiles.size > 0)

        val filesInDirectory: Set<String> = HashSet(Arrays.asList(*directory.list()))

        assertEquals(expectedFiles, filesInDirectory)
    }

}
