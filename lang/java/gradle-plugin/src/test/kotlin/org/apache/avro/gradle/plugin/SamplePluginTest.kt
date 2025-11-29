package org.apache.avro.gradle.plugin

import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path
import java.util.Arrays
import java.util.HashSet
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.copyToRecursively
import kotlin.io.path.createDirectories
import kotlin.io.path.writeText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@ExperimentalPathApi
class SamplePluginTest {

    @TempDir
    lateinit var tempDir: Path

    //@Test
    //fun `plugin is applied correctly to the project`() {
    //    val project = ProjectBuilder.builder().build()
    //    project.pluginManager.apply("org.apache.avro.avro-gradle-plugin")
    //    assert(project.tasks.getByName("avroGenerateJavaClasses") is CompileSchemaTask)
    //}

    @Test
    fun `plugin executes avro generate task successfully`() {
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
                srcDirectory = "$avroSrcDir"
                outputDirectory = "$avroOutPutDir"
                includes = listOf("**/*.avsc")
            }
        """.trimIndent()
        )

        // when
        val result = GradleRunner.create()
            .withProjectDir(tempDir.toFile())
            .withArguments("avroGenerateJavaClasses")
            .withPluginClasspath()
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
        assertFilesExist(testOutPutDirectory.toFile(), expectedFiles)
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
