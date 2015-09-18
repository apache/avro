package com.commercehub.gradle.plugin.avro;

import com.google.common.collect.ImmutableSet;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.compiler.specific.SpecificCompiler.FieldVisibility;
import org.apache.avro.generic.GenericData.StringType;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.specs.NotSpec;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.commercehub.gradle.plugin.avro.Constants.*;
import static java.lang.System.lineSeparator;

/**
 * Task to generate Java source files based on Avro protocol files and Avro schema files using {@link Protocol} and
 * {@link SpecificCompiler}.
 */
public class GenerateAvroJavaTask extends OutputDirTask {
    private static Set<String> SUPPORTED_EXTENSIONS = ImmutableSet.of(PROTOCOL_EXTENSION, SCHEMA_EXTENSION);

    private String encoding = Constants.DEFAULT_ENCODING;
    private String stringType = Constants.DEFAULT_STRING_TYPE;
    private String fieldVisibility = Constants.DEFAULT_FIELD_VISIBILITY;
    private String templateDirectory = DEFAULT_TEMPLATE_DIR;
    private transient StringType parsedStringType;
    private transient FieldVisibility parsedFieldVisibility;

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

    @Input
    public String getFieldVisibility() {
        return fieldVisibility;
    }

    public void setFieldVisibility(String fieldVisibility) {
        this.fieldVisibility = fieldVisibility;
    }

    @Input
    public String getTemplateDirectory() {
        return templateDirectory;
    }

    public void setTemplateDirectory(String templateDirectory) {
        this.templateDirectory = templateDirectory;
    }

    @TaskAction
    protected void process() {
        parsedStringType = Enums.parseCaseInsensitive("stringType", StringType.values(), getStringType());
        parsedFieldVisibility =
                Enums.parseCaseInsensitive("fieldVisibility", FieldVisibility.values(), getFieldVisibility());
        getLogger().debug("Using encoding {}", getEncoding());
        getLogger().debug("Using stringType {}", parsedStringType.name());
        getLogger().debug("Using fieldVisibility {}", parsedFieldVisibility.name());
        getLogger().debug("Using templateDirectory '{}'", getTemplateDirectory());
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
            compile(Protocol.parse(sourceFile), sourceFile);
        } catch (IOException ex) {
            throw new GradleException(String.format("Failed to compile protocol definition file %s", sourceFile), ex);
        }
    }

    private int processSchemaFiles() {
        int processedTotal = 0;
        int processedThisPass = -1;
        Map<String, Schema> types = new HashMap<>();
        Map<String, String> errors = new HashMap<>(); // file path to error message
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
                String path = getProject().relativePath(sourceFile);
                getLogger().debug("Processing {}", path);
                try {
                    Schema.Parser parser = new Schema.Parser();
                    parser.addTypes(types);
                    compile(parser.parse(sourceFile), sourceFile);
                    types = parser.getTypes();
                    getLogger().info("Processed {}", path);
                    processedThisPass++;
                    errors.remove(path);
                } catch (SchemaParseException ex) {
                    String errorMessage = ex.getMessage();
                    if (errorMessage.matches("(?i).*(undefined name|not a defined name).*")) {
                        getLogger().debug("Found undefined name in {} ({}); will try again later", path, errorMessage);
                        nextPass.add(sourceFile);
                        errors.put(path, ex.getMessage());
                    } else {
                        throw new GradleException(String.format("Failed to compile schema definition file %s", path), ex);
                    }
                } catch (NullPointerException ex) {
                    getLogger().debug("Encountered null reference while parsing {} (possibly due to unresolved dependency);"
                            + " will try again later", path);
                    nextPass.add(sourceFile);
                    errors.put(path, ex.getMessage());
                } catch (IOException ex) {
                    throw new GradleException(String.format("Failed to compile schema definition file %s", path), ex);
                }
                sourceFile = thisPass.poll();
            }
        }
        if (!nextPass.isEmpty()) {
            StringBuilder errorMessage = new StringBuilder("Could not compile schema definition files:");
            for (Map.Entry<String, String> error : errors.entrySet()) {
                errorMessage.append(lineSeparator()).append("* ").append(error.getKey()).append(": ").append(error.getValue());
            }
            throw new GradleException(errorMessage.toString());
        }
        return processedTotal;
    }

    private void compile(Protocol protocol, File sourceFile) throws IOException {
        compile(new SpecificCompiler(protocol), sourceFile);
    }

    private void compile(Schema schema, File sourceFile) throws IOException {
        compile(new SpecificCompiler(schema), sourceFile);
    }

    private void compile(SpecificCompiler compiler, File sourceFile) throws IOException {
        compiler.setOutputCharacterEncoding(getEncoding());
        compiler.setStringType(parsedStringType);
        compiler.setFieldVisibility(parsedFieldVisibility);
        compiler.setTemplateDir(getTemplateDirectory());
        compiler.compileToDestination(sourceFile, getOutputDir());
    }
}
