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
    fun `plugin executes avro generate task successfully`() {
        // given
        val projectDir: Path = Files.createTempDirectory("gradle-plugin-test-")

        val settingsFile = projectDir.resolve("settings.gradle.kts")
        val buildFile = projectDir.resolve("build.gradle.kts")

        val localAvroDir = File("src/test/avro")
        require(localAvroDir.exists()) { "src/test/avro not found" }

        val testResourcesDir = File(projectDir.toFile(), "src/test/avro")
        testResourcesDir.mkdirs()

        localAvroDir.copyRecursively(testResourcesDir, overwrite = true)

        settingsFile.writeText("")
        buildFile.writeText(
            """
            import org.apache.avro.gradle.plugin.SchemaType
            
            plugins {
                id("org.apache.avro.avro-gradle-plugin")
            }
            
            avro {
                schemaType = "schema"
                srcDirectory = "src/test/avro"
                outputDirectory = "generated-sources/avro"
                includes = listOf("**/*.avsc")
            }
        """.trimIndent()
        )

        val outputDirectory = File(projectDir.toFile(), "build/generated-sources/avro/test")

        // when
        val result = GradleRunner.create()
            .withProjectDir(projectDir.toFile())
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
        assertFilesExist(outputDirectory, expectedFiles)

        //assertTrue(result.output.contains("Hello from project"))
        //assertTrue(result.output.contains("BUILD SUCCESSFUL"))

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
