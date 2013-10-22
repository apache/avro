package com.commercehub.gradle.plugin.avro;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.specs.NotSpec;
import org.gradle.api.specs.Spec;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static com.commercehub.gradle.plugin.avro.Constants.*;

public class GenerateAvroJavaTask extends OutputDirTask {
    private static Set<String> SUPPORTED_EXTENSIONS = Sets.newHashSet(PROTOCOL_EXTENSION, SCHEMA_EXTENSION);

    @TaskAction
    protected void process() {
        getLogger().info("Found {} files", getInputs().getSourceFiles().getFiles().size());
        failOnUnsupportedFiles();
        processFiles();
    }

    private void failOnUnsupportedFiles() {
        FileCollection unsupportedFiles = filterSources(new NotSpec<>(new FileExtensionSpec(SUPPORTED_EXTENSIONS)));
        if (!unsupportedFiles.isEmpty()) {
            throw new GradleException(
                    String.format("Unsupported file extension for the following files: %s", unsupportedFiles));
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
        for (File sourceFile : filterSources(new FileExtensionSpec(PROTOCOL_EXTENSION))) {
            processProtoFile(sourceFile);
            processedFileCount++;
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

    private int processSchemaFiles() {
        int processedTotal = 0;
        int processedThisPass = -1;
        Map<String, Schema> types = Maps.newHashMap();
        Queue<File> nextPass = Lists.newLinkedList(filterSources(new FileExtensionSpec(SCHEMA_EXTENSION)).getFiles());
        Queue<File> thisPass = Lists.newLinkedList();
        while (processedThisPass != 0) {
            if (processedThisPass > 0) {
                processedTotal += processedThisPass;
            }
            processedThisPass = 0;
            thisPass.addAll(nextPass);
            nextPass.clear();
            File sourceFile = thisPass.poll();
            while (sourceFile != null) {
                getLogger().debug("Processing {}", sourceFile);
                try {
                    Schema.Parser parser = new Schema.Parser();
                    parser.addTypes(types);
                    Schema schema = parser.parse(sourceFile);
                    SpecificCompiler compiler = new SpecificCompiler(schema);
                    compiler.compileToDestination(sourceFile, getOutputDir());
                    types = parser.getTypes();
                    getLogger().info("Processed {}", sourceFile);
                    processedThisPass++;
                } catch (SchemaParseException ex) {
                    if (ex.getMessage().matches("(?i).*(undefined name|not a defined name).*")) {
                        getLogger().debug("Found undefined name in {}; will try again later", sourceFile);
                        nextPass.add(sourceFile);
                    } else {
                        throw new GradleException(String.format("Failed to compile schema definition file %s", sourceFile), ex);
                    }
                } catch (NullPointerException ex) {
                    getLogger().debug("Encountered null reference while parsing {} (possibly due to unresolved dependency); will try again later", sourceFile);
                    nextPass.add(sourceFile);
                } catch (IOException ex) {
                    throw new GradleException(String.format("Failed to compile schema definition file %s", sourceFile), ex);
                }
                sourceFile = thisPass.poll();
            }
        }
        if (!nextPass.isEmpty()) {
            throw new GradleException(String.format("Failed to compile schema definition files due to undefined names: %s", nextPass));
        }
        return processedTotal;
    }

    private FileCollection filterSources(Spec<? super File> spec) {
        return getInputs().getSourceFiles().filter(spec);
    }
}
