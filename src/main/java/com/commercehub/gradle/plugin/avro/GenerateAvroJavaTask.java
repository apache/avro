package com.commercehub.gradle.plugin.avro;

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
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.commercehub.gradle.plugin.avro.Constants.*;
import static com.commercehub.gradle.plugin.avro.MapUtils.asymmetricDifference;
import static java.lang.System.lineSeparator;

/**
 * Task to generate Java source files based on Avro protocol files and Avro schema files using {@link Protocol} and
 * {@link SpecificCompiler}.
 */
public class GenerateAvroJavaTask extends OutputDirTask {
    private static Pattern ERROR_UNKNOWN_TYPE = Pattern.compile("(?i).*(undefined name|not a defined name).*");
    private static Pattern ERROR_DUPLICATE_TYPE = Pattern.compile("Can't redefine: (.*)");
    private static Set<String> SUPPORTED_EXTENSIONS = SetBuilder.build(PROTOCOL_EXTENSION, SCHEMA_EXTENSION);

    private String encoding = DEFAULT_ENCODING;
    private String stringType = DEFAULT_STRING_TYPE;
    private String fieldVisibility = DEFAULT_FIELD_VISIBILITY;
    private String templateDirectory = DEFAULT_TEMPLATE_DIR;
    private boolean createSetters = DEFAULT_CREATE_SETTERS;
    private boolean retryDuplicateTypes = DEFAULT_RETRY_DUPLICATE_TYPES;

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

    @Input
    public boolean isCreateSetters() {
        return createSetters;
    }

    public void setCreateSetters(boolean createSetters) {
        this.createSetters = createSetters;
    }

    public void setCreateSetters(String createSetters) {
        this.createSetters = Boolean.parseBoolean(createSetters);
    }

    @Input
    public boolean isRetryDuplicateTypes() {
        return retryDuplicateTypes;
    }

    public void setRetryDuplicateTypes(boolean retryDuplicateTypes) {
        this.retryDuplicateTypes = retryDuplicateTypes;
    }

    public void setRetryDuplicateTypes(String retryDuplicateTypes) {
        this.retryDuplicateTypes = Boolean.parseBoolean(retryDuplicateTypes);
    }

    @TaskAction
    protected void process() {
        parsedStringType = Enums.parseCaseInsensitive(OPTION_STRING_TYPE, StringType.values(), getStringType());
        parsedFieldVisibility =
            Enums.parseCaseInsensitive(OPTION_FIELD_VISIBILITY, FieldVisibility.values(), getFieldVisibility());
        getLogger().debug("Using encoding {}", getEncoding());
        getLogger().debug("Using stringType {}", parsedStringType.name());
        getLogger().debug("Using fieldVisibility {}", parsedFieldVisibility.name());
        getLogger().debug("Using templateDirectory '{}'", getTemplateDirectory());
        getLogger().debug("Using createSetters {}", isCreateSetters());
        getLogger().debug("Using retryDuplicateTypes {}", isRetryDuplicateTypes());
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
     * {@link SpecificCompiler#compileToDestination(java.io.File, java.io.File)}, it will skip generating classes for any schema files where
     * the generated class is newer than the schema file.  That seems like a useful performance optimization, but it can cause problems in
     * the case where the schema file for this class hasn't changed, but the schema definition for one of the types it depends on has,
     * resulting in some usages of a type now having outdated schema.
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
        Set<File> files = filterSources(new FileExtensionSpec(SCHEMA_EXTENSION)).getFiles();
        ProcessingState processingState = new ProcessingState(files, getProject());
        while (processingState.isWorkRemaining()) {
            FileState fileState = processingState.nextFileState();
            String path = fileState.getPath();
            getLogger().debug("Processing {}, excluding types {}", path, fileState.getDuplicateTypeNames());
            File sourceFile = fileState.getFile();
            Map<String, Schema> parserTypes = processingState.determineParserTypes(fileState);
            try {
                Schema.Parser parser = new Schema.Parser();
                parser.addTypes(parserTypes);
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
                    processingState.queueForDelayedRetry(fileState);
                    getLogger().debug("Found undefined name in {} ({}); will try again", path, errorMessage);
                } else if (duplicateTypeMatcher.matches()) {
                    String typeName = duplicateTypeMatcher.group(1);
                    if (isRetryDuplicateTypes()) {
                        fileState.setError(ex);
                        fileState.addDuplicateTypeName(typeName);
                        processingState.queueForImmediateRetry(fileState);
                        getLogger().debug("Identified duplicate type {} in {}; will re-process excluding it", typeName, path);
                    } else {
                        throw new GradleException(String.format(
                            "Failed to compile schema definition file %s due to duplicate definition of type %s;"
                                + " This can be resolved by either declaring %s in its own schema file, or enabling the %s option",
                            path, typeName, typeName, OPTION_RETRY_DUPLICATE_TYPES), ex);
                    }
                } else {
                    throw new GradleException(String.format("Failed to compile schema definition file %s", path), ex);
                }
            } catch (NullPointerException ex) {
                fileState.setError(ex);
                processingState.queueForDelayedRetry(fileState);
                getLogger().debug("Encountered null reference while parsing {} (possibly due to unresolved dependency);"
                    + " will try again", path);
            } catch (IOException ex) {
                throw new GradleException(String.format("Failed to compile schema definition file %s", path), ex);
            }
        }
        Set<FileState> failedFiles = processingState.getFailedFiles();
        if (!failedFiles.isEmpty()) {
            StringBuilder errorMessage = new StringBuilder("Could not compile schema definition files:");
            for (FileState fileState : failedFiles) {
                String path = fileState.getPath();
                String fileErrorMessage = fileState.getErrorMessage();
                errorMessage.append(lineSeparator()).append("* ").append(path).append(": ").append(fileErrorMessage);
            }
            throw new GradleException(errorMessage.toString());
        }
        return processingState.getProcessedTotal();
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
        compiler.setCreateSetters(isCreateSetters());
        compiler.compileToDestination(sourceFile, getOutputDir());
    }
}
