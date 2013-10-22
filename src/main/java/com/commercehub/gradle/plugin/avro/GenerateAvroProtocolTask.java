package com.commercehub.gradle.plugin.avro;

import org.apache.avro.compiler.idl.Idl;
import org.apache.avro.compiler.idl.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.specs.NotSpec;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;

import static com.commercehub.gradle.plugin.avro.Constants.*;

public class GenerateAvroProtocolTask extends OutputDirTask {
    @TaskAction
    protected void process() {
        getLogger().info("Found {} files", getInputs().getSourceFiles().getFiles().size());
        failOnUnsupportedFiles();
        processFiles();
    }

    private void failOnUnsupportedFiles() {
        FileCollection unsupportedFiles = filterSources(new NotSpec<>(new FileExtensionSpec(IDL_EXTENSION)));
        if (!unsupportedFiles.isEmpty()) {
            throw new GradleException(
                    String.format("Unsupported file extension for the following files: %s", unsupportedFiles));
        }
    }

    private void processFiles() {
        int processedFileCount = 0;
        for (File sourceFile : filterSources(new FileExtensionSpec(IDL_EXTENSION))) {
            processIDLFile(sourceFile);
            processedFileCount++;
        }
        setDidWork(processedFileCount > 0);
    }

    private void processIDLFile(File idlFile) {
        getLogger().info("Processing {}", idlFile);
        File protoFile = new File(getOutputDir(),
                FilenameUtils.getBaseName(idlFile.getName()) + "." + PROTOCOL_EXTENSION);
        try (Idl idl = new Idl(idlFile)) {
            String protoJson = idl.CompilationUnit().toString();
            FileUtils.writeStringToFile(protoFile, protoJson, Constants.UTF8_ENCONDING);
        } catch (IOException | ParseException ex) {
            throw new GradleException(String.format("Failed to compile IDL file %s", idlFile), ex);
        }
    }
}
