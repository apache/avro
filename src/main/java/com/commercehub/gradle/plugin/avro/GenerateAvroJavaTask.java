package com.commercehub.gradle.plugin.avro;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.commons.io.FilenameUtils;
import org.gradle.api.GradleException;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class GenerateAvroJavaTask extends OutputDirTask {
    private static Set<String> SUPPORTED_EXTENSIONS = Collections.unmodifiableSet(new HashSet<>(
            Arrays.asList(Constants.PROTOCOL_EXTENSION, Constants.SCHEMA_EXTENSION)));

    private Schema.Parser parser = new Schema.Parser();

    @TaskAction
    protected void process() {
        getLogger().info("Found {} files", getInputs().getSourceFiles().getFiles().size());
        failOnUnsupportedFiles();
    }

    private void failOnUnsupportedFiles() {
        for (File sourceFile : getInputs().getSourceFiles()) {
            String extension = FilenameUtils.getExtension(sourceFile.getName());
            if (!SUPPORTED_EXTENSIONS.contains(extension)) {
                throw new GradleException(String.format("Unsupported file extension: %s for %s", extension, sourceFile));
            }
        }
    }

    private void processFiles() {
        int processedFileCount = 0;
        processedFileCount += processProtoFiles();
        processedFileCount += processSchemaFiles();
        setDidWork(processedFileCount > 0);
    }

    private int processProtoFiles() {
        int processedFileCount = 0;
        for (File sourceFile : getInputs().getSourceFiles()) {
            String extension = FilenameUtils.getExtension(sourceFile.getName());
            if (Constants.PROTOCOL_EXTENSION.equals(extension)) {
                processProtoFile(sourceFile);
                processedFileCount++;
            }
        }
        return processedFileCount;
    }

    private int processSchemaFiles() {
        int processedFileCount = 0;
        for (File sourceFile : getInputs().getSourceFiles()) {
            String extension = FilenameUtils.getExtension(sourceFile.getName());
            if (Constants.SCHEMA_EXTENSION.equals(extension)) {
                processSchemaFile(sourceFile);
                processedFileCount++;
            }
        }
        return processedFileCount;
    }

    private void processProtoFile(File sourceFile) {
        getLogger().info("Processing {}", sourceFile);
        try {
            Protocol protocol = Protocol.parse(sourceFile);
            SpecificCompiler compiler = new SpecificCompiler(protocol);
            compiler.compileToDestination(sourceFile, getOutputDir());
        } catch (IOException ex) {
            throw new GradleException(String.format("Failed to compile protocol definition file %s", sourceFile), ex);
        }
    }

    private void processSchemaFile(File sourceFile) {
        getLogger().info("Processing {}", sourceFile);
        try {
            Schema schema = parser.parse(sourceFile);
            SpecificCompiler compiler = new SpecificCompiler(schema);
            compiler.compileToDestination(sourceFile, getOutputDir());
        } catch (IOException ex) {
            throw new GradleException(String.format("Failed to compile schema definition file %s", sourceFile), ex);
        }
    }
}
