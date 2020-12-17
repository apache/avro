/**
 * Copyright © 2015 Commerce Technologies, LLC.
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

import java.io.File;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.gradle.api.file.ProjectLayout;

class ProcessingState {
    private final Map<String, TypeState> typeStates = new HashMap<>();
    private final Set<FileState> delayedFiles = new LinkedHashSet<>();
    private final Queue<FileState> filesToProcess = new LinkedList<>();
    private int processedTotal;

    ProcessingState(Iterable<File> files, ProjectLayout projectLayout) {
        for (File file : files) {
            filesToProcess.add(new FileState(file, FileUtils.projectRelativePath(projectLayout, file)));
        }
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
        processedTotal++;
        queueDelayedFilesForProcessing();
    }

    Set<FileState> getFailedFiles() {
        return delayedFiles;
    }

    private TypeState getTypeState(String typeName) {
        TypeState typeState = typeStates.get(typeName);
        if (typeState == null) {
            typeState = new TypeState(typeName);
            typeStates.put(typeName, typeState);
        }
        return typeState;
    }

    void queueForProcessing(FileState fileState) {
        filesToProcess.add(fileState);
    }

    void queueForDelayedProcessing(FileState fileState) {
        delayedFiles.add(fileState);
    }

    private void queueDelayedFilesForProcessing() {
        filesToProcess.addAll(delayedFiles);
        delayedFiles.clear();
    }

    FileState nextFileState() {
        return filesToProcess.poll();
    }

    boolean isWorkRemaining() {
        return !filesToProcess.isEmpty();
    }

    int getProcessedTotal() {
        return processedTotal;
    }

    Iterable<? extends Schema> getSchemasForLocation(String path) {
        return typeStates.values().stream().filter(it -> it.hasLocation(path)).map(TypeState::getSchema).collect(Collectors.toList());
    }

    Iterable<? extends Schema> getSchemas() {
        return typeStates.values().stream().map(TypeState::getSchema).collect(Collectors.toSet());
    }
}
