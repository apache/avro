package com.commercehub.gradle.plugin.avro;

import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.commons.io.FilenameUtils;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;

public class GenerateAvroJavaTask extends OutputDirTask {
    @TaskAction
    protected void process() {
        boolean didWork = false;
        FileCollection sourceFiles = getInputs().getSourceFiles();
        getLogger().info("Found {} files", sourceFiles.getFiles().size());
        for (File sourceFile : sourceFiles) {
            getLogger().info("Processing {}", sourceFile);
            String extension = FilenameUtils.getExtension(sourceFile.getName());
            if (Constants.PROTOCOL_EXTENSION.equals(extension)) {
                processProtoFile(sourceFile);
                didWork = true;
            } else if (Constants.SCHEMA_EXTENSION.equals(extension)) {
                processSchemaFile(sourceFile);
                didWork = true;
            } else {
                throw new GradleException(String.format("Supported file extension: %s", sourceFile));
            }
        }
        setDidWork(didWork);
    }

    private void processProtoFile(File sourceFile) {
        try {
            SpecificCompiler.compileProtocol(sourceFile, getOutputDir());
        } catch (IOException ex) {
            throw new GradleException(String.format("Failed to compile protocol definition file %s", sourceFile), ex);
        }
    }

    private void processSchemaFile(File sourceFile) {
        try {
            SpecificCompiler.compileSchema(sourceFile, getOutputDir());
        } catch (IOException ex) {
            throw new GradleException(String.format("Failed to compile schema definition file %s", sourceFile), ex);
        }
    }
}
