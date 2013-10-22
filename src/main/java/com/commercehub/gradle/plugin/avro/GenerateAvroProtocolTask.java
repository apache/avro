package com.commercehub.gradle.plugin.avro;

import org.apache.avro.compiler.idl.Idl;
import org.apache.avro.compiler.idl.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;

public class GenerateAvroProtocolTask extends OutputDirTask {
    @TaskAction
    protected void process() {
        boolean didWork = false;
        FileCollection sourceFiles = getInputs().getSourceFiles();
        getLogger().info("Found {} files", sourceFiles.getFiles().size());
        for (File sourceFile : sourceFiles) {
            getLogger().info("Processing {}", sourceFile);
            String extension = FilenameUtils.getExtension(sourceFile.getName());
            if (Constants.IDL_EXTENSION.equals(extension)) {
                processIDLFile(sourceFile);
                didWork = true;
            } else {
                throw new GradleException(String.format("Unsupported file extension: %s for %s", extension, sourceFile));
            }
        }
        setDidWork(didWork);
    }

    private void processIDLFile(File idlFile) {
        File protoFile = new File(getOutputDir(),
                FilenameUtils.getBaseName(idlFile.getName()) + Constants.PROTOCOL_EXTENSION);
        try (Idl idl = new Idl(idlFile)) {
            String protoJson = idl.CompilationUnit().toString();
            FileUtils.writeStringToFile(protoFile, protoJson, Constants.UTF8_ENCONDING);
        } catch (IOException | ParseException ex) {
            throw new GradleException(String.format("Failed to compile IDL file %s", idlFile), ex);
        }
    }
}
