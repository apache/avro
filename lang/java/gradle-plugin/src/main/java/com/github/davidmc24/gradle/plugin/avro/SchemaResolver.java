package com.github.davidmc24.gradle.plugin.avro;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.gradle.api.GradleException;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.logging.Logger;

class SchemaResolver {
    private static Pattern ERROR_DUPLICATE_TYPE = Pattern.compile("Can't redefine: (.*)");

    private final ProjectLayout projectLayout;
    private final Logger logger;

    SchemaResolver(ProjectLayout projectLayout, Logger logger) {
        this.projectLayout = projectLayout;
        this.logger = logger;
    }

    ProcessingState resolve(Iterable<File> files) {
        ProcessingState processingState = new ProcessingState(files, projectLayout);
        while (processingState.isWorkRemaining()) {
            processSchemaFile(processingState, processingState.nextFileState());
        }
        Set<FileState> failedFiles = processingState.getFailedFiles();
        if (!failedFiles.isEmpty()) {
            StringBuilder errorMessage = new StringBuilder("Could not resolve schema definition files:");
            for (FileState fileState : failedFiles) {
                String path = fileState.getPath();
                String fileErrorMessage = fileState.getErrorMessage();
                errorMessage.append(System.lineSeparator()).append("* ").append(path).append(": ").append(fileErrorMessage);
            }
            throw new GradleException(errorMessage.toString());
        }
        return processingState;
    }

    private void processSchemaFile(ProcessingState processingState, FileState fileState) {
        String path = fileState.getPath();
        logger.debug("Processing {}, excluding types {}", path, fileState.getDuplicateTypeNames());
        File sourceFile = fileState.getFile();
        Map<String, Schema> parserTypes = processingState.determineParserTypes(fileState);
        try {
            Schema.Parser parser = new Schema.Parser();
            parser.addTypes(parserTypes.values());
            parser.parse(sourceFile);
            Map<String, Schema> typesDefinedInFile = MapUtils.asymmetricDifference(parser.getTypes(), parserTypes);
            processingState.processTypeDefinitions(fileState, typesDefinedInFile);
            if (logger.isDebugEnabled()) {
                logger.debug("Processed {}; contained types {}", path, typesDefinedInFile.keySet());
            } else {
                logger.info("Processed {}", path);
            }
        } catch (AvroRuntimeException ex) {
            String errorMessage = ex.getMessage();
            Matcher duplicateTypeMatcher = ERROR_DUPLICATE_TYPE.matcher(errorMessage);
            if (duplicateTypeMatcher.matches()) {
                String typeName = duplicateTypeMatcher.group(1);
                if (fileState.containsDuplicateTypeName(typeName)) {
                    throw new GradleException(
                        String.format("Failed to resolve schema definition file %s; contains duplicate type definition %s", path, typeName),
                        ex);
                } else {
                    fileState.setError(ex);
                    fileState.addDuplicateTypeName(typeName);
                    processingState.queueForProcessing(fileState);
                    logger.debug("Identified duplicate type {} in {}; will re-process excluding it", typeName, path);
                }
            } else {
                fileState.setError(ex);
                processingState.queueForDelayedProcessing(fileState);
                logger.debug("Found error in {} ({}); will try again", path, errorMessage);
            }
        } catch (IOException ex) {
            throw new GradleException(String.format("Failed to resolve schema definition file %s", path), ex);
        }
    }
}
