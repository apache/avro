package com.commercehub.gradle.plugin.avro;

import org.apache.avro.Schema;
import org.gradle.api.Project;

import java.io.File;
import java.util.*;

class ProcessingState {
    private final Set<FileState> fileStates = new TreeSet<>();
    private final Map<String, TypeState> typeStates = new HashMap<>();
    private final Queue<FileState> nextPass = new LinkedList<>();
    private final Queue<FileState> thisPass = new LinkedList<>();
    private int processedTotal = 0;
    private int processedThisPass = 0;

    ProcessingState(Set<File> files, Project project) {
        for (File file : files) {
            fileStates.add(new FileState(file, project.relativePath(file)));
        }
        thisPass.addAll(fileStates);
    }

    Map<String, Schema> determineParserTypes(FileState fileState) {
        Set<String> duplicateTypeNames = fileState.getDuplicateTypeNames();
        Map<String, Schema> types = new HashMap<>();
        for (TypeState typeState : typeStates.values()) {
            String typeName = typeState.getName();
            if (!duplicateTypeNames.contains(typeName)) {
                types.put(typeState.getName(), typeState.getSchema());
            }
        }
        return types;
    }

    void processTypeDefinitions(FileState fileState, Map<String, Schema> newTypes) {
        String path = fileState.getPath();
        for (Map.Entry<String, Schema> entry : newTypes.entrySet()) {
            String typeName = entry.getKey();
            Schema schema = entry.getValue();
            getTypeState(typeName).processTypeDefinition(path, schema);
        }
        fileState.clearError();
        processedThisPass++;
        processedTotal++;
    }

    Set<FileState> getFailedFiles() {
        Set<FileState> failedFiles = new LinkedHashSet<>();
        for (FileState fileState : fileStates) {
            if (fileState.isFailed()) {
                failedFiles.add(fileState);
            }
        }
        return failedFiles;
    }

    TypeState getTypeState(String typeName) {
        TypeState typeState = typeStates.get(typeName);
        if (typeState == null) {
            typeState = new TypeState(typeName);
            typeStates.put(typeName, typeState);
        }
        return typeState;
    }

    void queueForImmediateRetry(FileState fileState) {
        thisPass.add(fileState);
    }

    void queueForDelayedRetry(FileState fileState) {
        nextPass.add(fileState);
    }

    FileState nextFileState() {
        FileState fileState = thisPass.poll();
        if (fileState != null) {
            return fileState;
        }
        nextPass();
        return thisPass.poll();
    }

    boolean isWorkRemaining() {
        return !thisPass.isEmpty() || (processedThisPass > 0 && !nextPass.isEmpty());
    }

    int getProcessedTotal() {
        return processedTotal;
    }

    private void nextPass() {
        if (processedThisPass > 0) {
            thisPass.addAll(nextPass);
            nextPass.clear();
            processedThisPass = 0;
        }
    }
}
