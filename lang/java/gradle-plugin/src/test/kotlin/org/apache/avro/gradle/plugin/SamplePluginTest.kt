package org.apache.avro.gradle.plugin

import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.io.path.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@ExperimentalPathApi
class SamplePluginTest {

    @TempDir
    lateinit var tempDir: Path

    @Test
    fun `plugin executes avroGenerateJavaClasses task successfully`() {
        // given
        val testSettingsFile = tempDir.resolve("settings.gradle.kts")
        val testBuildFile = tempDir.resolve("build.gradle.kts")
        val testAvroSrcDir = tempDir.resolve("src/test/avro").createDirectories()

        val avroSrcDir = Path.of("src/test/avro")
        val avroOutPutDir = Path.of("generated-sources/avro")

        val testOutPutDirectory = tempDir.resolve("build/$avroOutPutDir/test")

        avroSrcDir.copyToRecursively(
            testAvroSrcDir,
            overwrite = true,
            followLinks = false
        )

        testSettingsFile.writeText("")
        testBuildFile.writeText(
            """            
            plugins {
                id("org.apache.avro.avro-gradle-plugin")
            }
            
            avro {
                schemaType = "schema"
                sourceDirectory = "$avroSrcDir"
                outputDirectory = "$avroOutPutDir"
            }
        """.trimIndent()
        )

        // when
        val result = GradleRunner.create()
            .withProjectDir(tempDir.toFile())
            .withArguments("avroGenerateJavaClasses")
            .withPluginClasspath()
            .forwardOutput() // to see printLn in code
            .build()

        val expectedFiles = setOf(
            "SchemaPrivacy.java",
            "SchemaUser.java",
            "PrivacyImport.java",
            "SchemaCustom.java",
            "PrivacyDirectImport.java"
        )

        // then
        assertEquals(TaskOutcome.SUCCESS, result.task(":avroGenerateJavaClasses")?.outcome)
        assertFilesExist(testOutPutDirectory, expectedFiles)

        val schemaUserContent = testOutPutDirectory.resolve("SchemaUser.java").readText()
        assertTrue(schemaUserContent.contains("java.time.Instant"))
    }


    private fun assertFilesExist(directory: Path, expectedFiles: Set<String>) {
        assertTrue(directory.exists(), "Directory $directory does not exist")
        assertTrue(expectedFiles.isNotEmpty())

        val filesInDirectory: Set<String> = directory
            .listDirectoryEntries()
            .map { it.fileName.toString() }.toSet()

        assertEquals(expectedFiles, filesInDirectory)
    }

}
