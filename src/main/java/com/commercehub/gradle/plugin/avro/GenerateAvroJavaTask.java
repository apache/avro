/**
 * Copyright © 2013-2015 Commerce Technologies, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.commercehub.gradle.plugin.avro;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.compiler.specific.SpecificCompiler.FieldVisibility;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.StringType;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.specs.NotSpec;
import org.gradle.api.tasks.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.commercehub.gradle.plugin.avro.Constants.*;
import static com.commercehub.gradle.plugin.avro.MapUtils.asymmetricDifference;

/**
 * Task to generate Java source files based on Avro protocol files and Avro schema files using {@link Protocol} and
 * {@link SpecificCompiler}.
 */
@CacheableTask
public class GenerateAvroJavaTask extends OutputDirTask {
    private static Pattern ERROR_UNKNOWN_TYPE = Pattern.compile("(?i).*(undefined name|not a defined name).*");
    private static Pattern ERROR_DUPLICATE_TYPE = Pattern.compile("Can't redefine: (.*)");
    private static Set<String> SUPPORTED_EXTENSIONS = new SetBuilder<String>().add(PROTOCOL_EXTENSION).add(SCHEMA_EXTENSION).build();

    private String outputCharacterEncoding;
    private String stringType = DEFAULT_STRING_TYPE;
    private String fieldVisibility = DEFAULT_FIELD_VISIBILITY;
    private String templateDirectory;
    private boolean createSetters = DEFAULT_CREATE_SETTERS;
    private boolean enableDecimalLogicalType = DEFAULT_ENABLE_DECIMAL_LOGICAL_TYPE;
    private boolean validateDefaults = DEFAULT_VALIDATE_DEFAULTS;

    private transient StringType parsedStringType;
    private transient FieldVisibility parsedFieldVisibility;

    @Optional
    @Input
    public String getOutputCharacterEncoding() {
        return outputCharacterEncoding;
    }

    public void setOutputCharacterEncoding(String outputCharacterEncoding) {
        this.outputCharacterEncoding = outputCharacterEncoding;
    }

    public void setOutputCharacterEncoding(Charset outputCharacterEncoding) {
        setOutputCharacterEncoding(outputCharacterEncoding.name());
    }

    @Input
    public String getStringType() {
        return stringType;
    }

    public void setStringType(GenericData.StringType stringType) {
        setStringType(stringType.name());
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

    public void setFieldVisibility(SpecificCompiler.FieldVisibility fieldVisibility) {
        setFieldVisibility(fieldVisibility.name());
    }

    @Optional
    @Input
    public String getTemplateDirectory() {
        return templateDirectory;
    }

    public void setTemplateDirectory(String templateDirectory) {
        this.templateDirectory = templateDirectory;
    }

    @Input
    public boolean isCreateSetters() {
        return createSetters;
    }

    public void setCreateSetters(String createSetters) {
        this.createSetters = Boolean.parseBoolean(createSetters);
    }

    @Input
    public boolean isEnableDecimalLogicalType() {
        return enableDecimalLogicalType;
    }

    public void setEnableDecimalLogicalType(String enableDecimalLogicalType) {
        this.enableDecimalLogicalType = Boolean.parseBoolean(enableDecimalLogicalType);
    }

    @Input
    public boolean isValidateDefaults() {
        return validateDefaults;
    }

    public void setValidateDefaults(boolean validateDefaults) {
        this.validateDefaults = validateDefaults;
    }

    @TaskAction
    protected void process() {
        parsedStringType = Enums.parseCaseInsensitive(OPTION_STRING_TYPE, StringType.values(), getStringType());
        parsedFieldVisibility =
            Enums.parseCaseInsensitive(OPTION_FIELD_VISIBILITY, FieldVisibility.values(), getFieldVisibility());
        getLogger().debug("Using outputCharacterEncoding {}", getOutputCharacterEncoding());
        getLogger().debug("Using stringType {}", parsedStringType.name());
        getLogger().debug("Using fieldVisibility {}", parsedFieldVisibility.name());
        getLogger().debug("Using templateDirectory '{}'", getTemplateDirectory());
        getLogger().debug("Using createSetters {}", isCreateSetters());
        getLogger().debug("Using enableDecimalLogicalType {}", isEnableDecimalLogicalType());
        getLogger().debug("Using validateDefaults {}", isValidateDefaults());
        getLogger().info("Found {} files", getInputs().getSourceFiles().getFiles().size());
        failOnUnsupportedFiles();
        processFiles();
    }

    private void failOnUnsupportedFiles() {
        FileCollection unsupportedFiles = filterSources(new NotSpec<File>(new FileExtensionSpec(SUPPORTED_EXTENSIONS)));
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
            compile(Protocol.parse(sourceFile), sourceFile);
        } catch (IOException ex) {
            throw new GradleException(String.format("Failed to compile protocol definition file %s", sourceFile), ex);
        }
    }

    private int processSchemaFiles() {
        Set<File> files = filterSources(new FileExtensionSpec(SCHEMA_EXTENSION)).getFiles();
        ProcessingState processingState = new ProcessingState(files, getProject());
        while (processingState.isWorkRemaining()) {
            processSchemaFile(processingState, processingState.nextFileState());
        }
        Set<FileState> failedFiles = processingState.getFailedFiles();
        if (!failedFiles.isEmpty()) {
            StringBuilder errorMessage = new StringBuilder("Could not compile schema definition files:");
            for (FileState fileState : failedFiles) {
                String path = fileState.getPath();
                String fileErrorMessage = fileState.getErrorMessage();
                errorMessage.append(System.lineSeparator()).append("* ").append(path).append(": ").append(fileErrorMessage);
            }
            throw new GradleException(errorMessage.toString());
        }
        return processingState.getProcessedTotal();
    }

    private void processSchemaFile(ProcessingState processingState, FileState fileState) {
        String path = fileState.getPath();
        getLogger().debug("Processing {}, excluding types {}", path, fileState.getDuplicateTypeNames());
        File sourceFile = fileState.getFile();
        Map<String, Schema> parserTypes = processingState.determineParserTypes(fileState);
        try {
            Schema.Parser parser = new Schema.Parser();
            parser.addTypes(parserTypes);
            parser.setValidateDefaults(isValidateDefaults());

            compile(parser.parse(sourceFile), sourceFile);
            Map<String, Schema> typesDefinedInFile = asymmetricDifference(parser.getTypes(), parserTypes);
            processingState.processTypeDefinitions(fileState, typesDefinedInFile);
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("Processed {}; contained types {}", path, typesDefinedInFile.keySet());
            } else {
                getLogger().info("Processed {}", path);
            }
        } catch (SchemaParseException ex) {
            String errorMessage = ex.getMessage();
            Matcher unknownTypeMatcher = ERROR_UNKNOWN_TYPE.matcher(errorMessage);
            Matcher duplicateTypeMatcher = ERROR_DUPLICATE_TYPE.matcher(errorMessage);
            if (unknownTypeMatcher.matches()) {
                fileState.setError(ex);
                processingState.queueForDelayedProcessing(fileState);
                getLogger().debug("Found undefined name in {} ({}); will try again", path, errorMessage);
            } else if (duplicateTypeMatcher.matches()) {
                String typeName = duplicateTypeMatcher.group(1);
                if (fileState.containsDuplicateTypeName(typeName)) {
                    throw new GradleException(
                        String.format("Failed to compile schema definition file %s; contains duplicate type definition %s", path, typeName),
                        ex);
                } else {
                    fileState.setError(ex);
                    fileState.addDuplicateTypeName(typeName);
                    processingState.queueForProcessing(fileState);
                    getLogger().debug("Identified duplicate type {} in {}; will re-process excluding it", typeName, path);
                }
            } else {
                throw new GradleException(String.format("Failed to compile schema definition file %s", path), ex);
            }
        } catch (NullPointerException ex) {
            fileState.setError(ex);
            processingState.queueForDelayedProcessing(fileState);
            getLogger().debug("Encountered null reference while parsing {} (possibly due to unresolved dependency); will try again", path);
        } catch (IOException ex) {
            throw new GradleException(String.format("Failed to compile schema definition file %s", path), ex);
        }
    }

    private void compile(Protocol protocol, File sourceFile) throws IOException {
        compile(new SpecificCompiler(protocol, SpecificCompiler.DateTimeLogicalTypeImplementation.DEFAULT), sourceFile);
    }

    private void compile(Schema schema, File sourceFile) throws IOException {
        compile(new SpecificCompiler(schema, SpecificCompiler.DateTimeLogicalTypeImplementation.DEFAULT), sourceFile);
    }

    private void compile(SpecificCompiler compiler, File sourceFile) throws IOException {
        String templateDirectory = getTemplateDirectory();
        compiler.setOutputCharacterEncoding(getOutputCharacterEncoding());
        compiler.setStringType(parsedStringType);
        compiler.setFieldVisibility(parsedFieldVisibility);
        if (templateDirectory != null) {
            compiler.setTemplateDir(templateDirectory);
        }
        compiler.setCreateSetters(isCreateSetters());
        compiler.setEnableDecimalLogicalType(isEnableDecimalLogicalType());

        compiler.compileToDestination(sourceFile, getOutputDir());
    }
}
