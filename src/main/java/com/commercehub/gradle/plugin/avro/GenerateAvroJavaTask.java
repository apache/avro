package com.commercehub.gradle.plugin.avro;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.GenericData;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.specs.NotSpec;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static com.commercehub.gradle.plugin.avro.Constants.*;

public class GenerateAvroJavaTask extends OutputDirTask {
    private static Set<String> SUPPORTED_EXTENSIONS = SetBuilder.build(PROTOCOL_EXTENSION, SCHEMA_EXTENSION);

    private String encoding;

    private String stringType;

    @Input
    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    @Input
    public String getStringType() {
        return stringType;
    }

    public void setStringType(String stringType) {
        this.stringType = stringType;
    }

    private GenericData.StringType parseStringType() {
        String stringType = getStringType();
        for (GenericData.StringType type : GenericData.StringType.values()) {
            if (type.name().equalsIgnoreCase(stringType)) {
                return type;
            }
        }
        throw new IllegalArgumentException(String.format("Invalid stringType '%s'.  Valid values are: %s", stringType,
                Arrays.asList(GenericData.StringType.values())));

    }

    @TaskAction
    protected void process() {
        getLogger().info("Found {} files", getInputs().getSourceFiles().getFiles().size());
        failOnUnsupportedFiles();
        preClean();
        processFiles();
    }

    private void failOnUnsupportedFiles() {
        FileCollection unsupportedFiles = filterSources(new NotSpec<>(new FileExtensionSpec(SUPPORTED_EXTENSIONS)));
        if (!unsupportedFiles.isEmpty()) {
            throw new GradleException(
                    String.format("Unsupported file extension for the following files: %s", unsupportedFiles));
        }
    }

    /**
     * We need to remove all previously generated Java classes.  Otherwise, when we call
     * {@link SpecificCompiler#compileToDestination(java.io.File, java.io.File)}, it will skip generating classes for
     * any schema files where the generated class is newer than the schema file.  That seems like a useful performance
     * optimization, but it can cause problems in the case where the schema file for this class hasn't changed, but
     * the schema definition for one of the types it depends on has, resulting in some usages of a type now having
     * outdated schema.
     */
    private void preClean() {
        getProject().delete(getOutputDir());
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
            compiler.setStringType(parseStringType());
            if (encoding != null) {
                compiler.setOutputCharacterEncoding(encoding);
            }
            compiler.compileToDestination(sourceFile, getOutputDir());
        } catch (IOException ex) {
            throw new GradleException(String.format("Failed to compile protocol definition file %s", sourceFile), ex);
        }
    }

    private int processSchemaFiles() {
        int processedTotal = 0;
        int processedThisPass = -1;
        Map<String, Schema> types = new HashMap<>();
        Queue<File> nextPass = new LinkedList<>(filterSources(new FileExtensionSpec(SCHEMA_EXTENSION)).getFiles());
        Queue<File> thisPass = new LinkedList<>();
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
                    compiler.setStringType(parseStringType());
                    if (encoding != null) {
                        compiler.setOutputCharacterEncoding(encoding);
                    }
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
}
