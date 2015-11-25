package com.commercehub.gradle.plugin.avro;

import java.io.File;
import java.util.Set;
import java.util.TreeSet;

class FileState implements Comparable<FileState> {
    private final File file;
    private final String path;
    private String errorMessage;
    private Set<String> duplicateTypeNames = new TreeSet<String>();

    FileState(File file, String path) {
        this.file = file;
        this.path = path;
    }

    File getFile() {
        return file;
    }

    Set<String> getDuplicateTypeNames() {
        return duplicateTypeNames;
    }

    void clearError() {
        errorMessage = null;
    }

    void setError(Throwable ex) {
        this.errorMessage = ex.getMessage();
    }

    void addDuplicateTypeName(String typeName) {
        duplicateTypeNames.add(typeName);
    }

    public String getPath() {
        return path;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public int compareTo(FileState o) {
        return path.compareTo(o.getPath());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileState fileState = (FileState) o;
        return path.equals(fileState.path);
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }
}
