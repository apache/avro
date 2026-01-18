package eu.eventloopsoftware.avro.gradle.plugin

import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.io.path.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@ExperimentalPathApi
class CompileAvroSchemaTaskTest {

    @TempDir
    lateinit var tempDir: Path

    @Test
    fun `plugin executes avroGenerateJavaClasses task successfully`() {
        // given
        val tempSettingsFile = tempDir.resolve("settings.gradle.kts")
        val tempBuildFile = tempDir.resolve("build.gradle.kts")
        val tempAvroSrcDir = tempDir.resolve("src/test/avro").createDirectories()

        val testAvroFiles = Path.of("src/test/avro")
        val testAvroOutPutDir = Path.of("generated-sources/avro")

        val testOutPutDirectory = tempDir.resolve("build/$testAvroOutPutDir/test")

        testAvroFiles.copyToRecursively(
            tempAvroSrcDir,
            overwrite = true,
            followLinks = false
        )

        tempSettingsFile.writeText("")
        tempBuildFile.writeText(
            """            
            plugins {
                id("eu.eventloopsoftware.avro-gradle-plugin")
            }
            
            avro {
                schemaType = "schema"
                sourceDirectory = "$testAvroFiles"
                outputDirectory = "$testAvroOutPutDir"           
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


    @Test
    fun `plugin executes avroGenerateTestJavaClasses task successfully - for files in test directory`() {
        // given
        val tempSettingsFile = tempDir.resolve("settings.gradle.kts")
        val tempBuildFile = tempDir.resolve("build.gradle.kts")
        val tempAvroSrcDir = tempDir.resolve("src/test/avro").createDirectories()

        val testAvroFiles = Path.of("src/test/avro")
        val testAvroOutPutDir = Path.of("generated-test-sources-avro")

        val testOutPutDirectory = tempDir.resolve("build/$testAvroOutPutDir/test")

        testAvroFiles.copyToRecursively(
            tempAvroSrcDir,
            overwrite = true,
            followLinks = false
        )

        tempSettingsFile.writeText("")
        tempBuildFile.writeText(
            """            
            plugins {
                id("eu.eventloopsoftware.avro-gradle-plugin")
            }
            
            avro {
                schemaType = "schema"
                testSourceDirectory = "$testAvroFiles"
                testOutputDirectory = "$testAvroOutPutDir"
            }
        """.trimIndent()
        )

        // when
        val result = GradleRunner.create()
            .withProjectDir(tempDir.toFile())
            .withArguments("avroGenerateTestJavaClasses")
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
        assertEquals(TaskOutcome.SUCCESS, result.task(":avroGenerateTestJavaClasses")?.outcome)
        assertFilesExist(testOutPutDirectory, expectedFiles)

        val schemaUserContent = testOutPutDirectory.resolve("SchemaUser.java").readText()
        assertTrue(schemaUserContent.contains("java.time.Instant"))
    }

    @Test
    fun `plugin executes avroGenerateJavaClasses task successfully - with Velocity class names`() {
        // given
        val tempSettingsFile = tempDir.resolve("settings.gradle.kts")
        val tempBuildFile = tempDir.resolve("build.gradle.kts")
        val tempAvroSrcDir = tempDir.resolve("src/test/avro").createDirectories()
        val tempVelocityToolClassesDir = tempDir.resolve("src/test/resources/templates").createDirectories()

        val testAvroFilesDir = Path.of("src/test/avro")
        val testAvroOutPutDir = Path.of("generated-sources-avro")
        val testVelocityToolClassesDir = Path.of("src/test/resources/templates")

        val testOutPutDirectory = tempDir.resolve("build/$testAvroOutPutDir/test")

        testAvroFilesDir.copyToRecursively(
            tempAvroSrcDir,
            overwrite = true,
            followLinks = false
        )

        testVelocityToolClassesDir.copyToRecursively(
            tempVelocityToolClassesDir,
            overwrite = true,
            followLinks = false
        )

        tempSettingsFile.writeText("")
        tempBuildFile.writeText(
            """            
            plugins {
                id("eu.eventloopsoftware.avro-gradle-plugin")
            }
            
            avro {
                schemaType = "schema"
                sourceDirectory = "$testAvroFilesDir"
                outputDirectory = "$testAvroOutPutDir"
                templateDirectory = "${tempDir.resolve(testVelocityToolClassesDir).toString() + "/"}"
                velocityToolsClassesNames = listOf("java.lang.String")
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
        assertTrue(schemaUserContent.contains("It works!"))
    }

    @Test
    fun `plugin executes avroGenerateJavaClasses task successfully - custom recordSpecificClass`() {
        // given
        val tempSettingsFile = tempDir.resolve("settings.gradle.kts")
        val tempBuildFile = tempDir.resolve("build.gradle.kts")
        val tempAvroSrcDir = tempDir.resolve("src/test/avro").createDirectories()

        val testAvroFiles = Path.of("src/test/avro")
        val testAvroOutPutDir = Path.of("generated-sources/avro")

        val testOutPutDirectory = tempDir.resolve("build/$testAvroOutPutDir/test")

        testAvroFiles.copyToRecursively(
            tempAvroSrcDir,
            overwrite = true,
            followLinks = false
        )

        tempSettingsFile.writeText("")
        tempBuildFile.writeText(
            """            
            plugins {
                id("eu.eventloopsoftware.avro-gradle-plugin")
            }
            
            avro {
                schemaType = "schema"
                sourceDirectory = "$testAvroFiles"
                outputDirectory = "$testAvroOutPutDir"
                recordSpecificClass = "org.apache.avro.custom.CustomRecordBase"
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

        // then
        assertEquals(TaskOutcome.SUCCESS, result.task(":avroGenerateJavaClasses")?.outcome)

        val outPutFile = testOutPutDirectory.resolve("SchemaCustom.java")
        assertTrue(outPutFile.toFile().exists())

        val extendsLines = outPutFile.readLines()
            .filter { line -> line.contains("class SchemaCustom extends ") }
        assertEquals(1, extendsLines.size)

        val extendLine = extendsLines[0]
        assertTrue(extendLine.contains(" org.apache.avro.custom.CustomRecordBase "))
        assertFalse(extendLine.contains("org.apache.avro.specific.SpecificRecordBase"))
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
